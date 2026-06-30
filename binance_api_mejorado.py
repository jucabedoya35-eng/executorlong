# binance_fixed.py - Versión corregida para errores de position side
from binance.client import Client
from binance.enums import *
from matplotlib import ticker
import pandas as pd
import time
import logging
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING, InvalidOperation
import time
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Dict, List, Union

class BinanceAPI:
        def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
            """
            Inicializa la conexión con Binance Futures API
            """
            if not api_key or not api_secret:
                raise ValueError("API Key y API Secret son requeridos")
            
            print(f"Inicializando API con Key: {api_key[:8]}...")
            self.client = Client(api_key, api_secret, testnet=testnet)
            self.testnet = testnet
            
            # Configurar logging
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
            
            # Cache para información de símbolos
            self._symbol_info_cache = {}
            
            # Detectar automáticamente el modo de posición del usuario
            self._position_mode = None
            self._detect_position_mode()
            
        def _detect_position_mode(self):
            """Detecta automáticamente si el usuario tiene Hedge Mode activado"""
            try:
                # Obtener configuración actual de posición
                response = self.client.futures_get_position_mode()
                self._position_mode = "hedge" if response["dualSidePosition"] else "one_way"
                self.logger.info(f"Modo de posición detectado: {self._position_mode}")
            except Exception as e:
                self.logger.warning(f"No se pudo detectar el modo de posición: {e}")
                self._position_mode = "one_way"  # Default más común
        
        def set_position_mode(self, dual_side_position: bool) -> Optional[Dict]:
            """
            Cambia el modo de posición (Hedge Mode on/off)
            
            Args:
                dual_side_position: True para Hedge Mode, False para One-way Mode
            """
            try:
                result = self.client.futures_change_position_mode(
                    dualSidePosition=dual_side_position,
                    timestamp=int(time.time() * 1000)
                )
                mode = "Hedge Mode" if dual_side_position else "One-way Mode"
                self.logger.info(f"Modo de posición cambiado a: {mode}")
                self._position_mode = "hedge" if dual_side_position else "one_way"
                return result
            except Exception as e:
                error_msg = str(e)
                if "No need to change position side" in error_msg:
                    mode = "Hedge Mode" if dual_side_position else "One-way Mode"
                    self.logger.info(f"El modo de posición ya está en: {mode}")
                    self._position_mode = "hedge" if dual_side_position else "one_way"
                    return {"msg": f"Already in {mode}"}
                else:
                    self.logger.error(f"Error cambiando modo de posición: {e}")
                    return None
        
        def get_position_mode(self) -> str:
            """Retorna el modo de posición actual"""
            return self._position_mode
        
        def _get_correct_position_side(self, side: str, force_position_side: Optional[str] = None) -> str:
            """
            Determina el positionSide correcto según el modo de posición.
            
            Args:
                side: 'BUY' o 'SELL'
                force_position_side: 'BOTH', 'LONG' o 'SHORT' para forzar un side específico
            """
            # En One-way Mode siempre usar BOTH, ignorando cualquier force_position_side
            if self._position_mode != "hedge":
                return "BOTH"

            # En Hedge Mode, respetar force_position_side si se pasa
            if force_position_side:
                return force_position_side

            # En Hedge Mode sin force, usar LONG/SHORT según side
            return "LONG" if side == "BUY" else "SHORT"
    
        
        def _get_symbol_info(self, symbol: str) -> Dict:
            """Obtiene información del símbolo con cache"""
            if symbol not in self._symbol_info_cache:
                try:
                    info = self.client.futures_exchange_info()
                    for s in info['symbols']:
                        if s['symbol'] == symbol:
                            self._symbol_info_cache[symbol] = s
                            break
                except Exception as e:
                    self.logger.error(f"Error getting symbol info: {e}")
                    return {}
            return self._symbol_info_cache.get(symbol, {})
        
        def _round_quantity(self, symbol: str, quantity: float) -> str:
            """Redondea la cantidad según las reglas del símbolo"""
            symbol_info = self._get_symbol_info(symbol)
            if not symbol_info:
                return str(int(quantity)) if quantity.is_integer() else str(quantity)
            
            for f in symbol_info.get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    precision = len(str(step_size).split('.')[-1].rstrip('0'))
                    rounded = float(Decimal(str(quantity)).quantize(
                        Decimal(str(step_size)), rounding=ROUND_DOWN
                    ))
                    
                    # ✅ elimina el .0 si el número es entero, pero conserva otros decimales
                    if rounded.is_integer():
                        return str(int(rounded))
                    else:
                        return f"{rounded:.{precision}f}".rstrip('0').rstrip('.')
            
            # Si no encuentra filtro, también limpia el .0
            return str(int(quantity)) if quantity.is_integer() else str(quantity)
        
        def _get_position_quantity(self, symbol: str, position_side: Optional[str]) -> float:
            """
            Devuelve la cantidad absoluta de la posición abierta en 'symbol' y 'position_side'.
            """
            positions = self.client.futures_position_information(symbol=symbol)
            for p in positions:
                if p['positionSide'] == position_side:
                    return abs(float(p['positionAmt']))
            return 0.0

        def set_take_profit(self, symbol: str, take_profit_price: float, position_side: Optional[str] = None) -> Optional[Dict]:
            """
            Coloca una orden de take profit utilizando una orden condicional de mercado.

            Se calcula la orientación de la orden a partir de ``position_side``:
            - Para una posición LONG, la toma de ganancias se logra vendiendo. El ``stopPrice`` debe estar por
              encima del precio de mercado actual, de modo que la orden sólo se active cuando el precio
              alcance o supere el nivel de take profit.
            - Para una posición SHORT, la toma de ganancias se logra comprando. El ``stopPrice`` debe estar
              por debajo del precio de mercado actual, de modo que la orden sólo se active cuando el precio
              caiga hasta el nivel de take profit.

            Si ``position_side`` no se especifica o es ``BOTH``, se asume que la posición es LONG para los
            cálculos de stopPrice (esto es coherente con el modo one-way de Binance, donde ``positionSide``
            siempre es ``BOTH``). La orden se envía siempre con ``positionSide='BOTH'`` para cumplir el modo
            one-way, pero la lógica interna determina la dirección correcta para ``side``.
            """
            try:
                # Obtener el precio de mercado actual y el tickSize
                ticker = self.get_ticker_price(symbol)
                mark_price = float(ticker['price'])

                symbol_info = self._get_symbol_info(symbol)
                tick_size = next(
                    float(f['tickSize'])
                    for f in symbol_info.get('filters', [])
                    if f.get('filterType') == 'PRICE_FILTER'
                )

                # Pequeño margen para evitar activación inmediata
                buffer = max(mark_price * 0.00001, tick_size * 10)

                # Determinar orientación (LONG por defecto)
                direction = (position_side or "LONG").upper()

                # Ajustar el precio de take profit según dirección y precio de mercado
                if direction == "LONG":
                    # Para LONG se vende. stopPrice debe estar por encima del precio actual
                    target_price = max(take_profit_price, mark_price + buffer)
                    side = "SELL"
                elif direction == "SHORT":
                    # Para SHORT se compra. stopPrice debe estar por debajo del precio actual
                    target_price = min(take_profit_price, mark_price - buffer)
                    side = "BUY"
                else:
                    # En caso de valor inesperado, asumir LONG
                    target_price = max(take_profit_price, mark_price + buffer)
                    side = "SELL"

                # Redondear el stopPrice al tick size
                stop_price = self._round_price_limit(symbol, target_price)

                # Determinar cantidad actual de la posición (en modo one-way siempre 'BOTH')
                qty = self._get_position_quantity(symbol, "BOTH")
                if qty <= 0:
                    self.logger.warning(f"No hay posición abierta para colocar TP en {symbol}")
                    return None

                order_params = {
                    'symbol':      symbol,
                    'side':        side,
                    'type':        'TAKE_PROFIT_MARKET',
                    'stopPrice':   stop_price,
                    'quantity':    qty,
                    'reduceOnly':  'true',
                    'workingType': 'MARK_PRICE',
                    'priceProtect':'true',
                    'positionSide': 'BOTH',
                    'timestamp':   int(time.time() * 1000)
                }

                order = self.client.futures_create_order(**order_params)
                order_id = order.get('orderId') if order else None
                self.logger.info(
                    f"TP MARKET colocado en {stop_price} para {symbol} "
                    f"(side: {side}, positionSide: BOTH, orderId: {order_id})"
                )
                return order

            except Exception as e:
                self.logger.error(f"Error placing take profit: {e}")
                return None

        def set_stop_loss(self, symbol: str, stop_price: float, position_side: Optional[str] = None) -> Optional[Dict]:
            """
            Coloca una orden de stop loss utilizando una orden condicional de mercado.

            Para LONG se vende cuando el precio cae hasta el stop loss; por lo tanto, ``stopPrice`` debe
            estar por debajo del precio de mercado actual. Para SHORT se compra cuando el precio sube
            hasta el stop loss; por lo tanto, ``stopPrice`` debe estar por encima del precio de mercado.

            Si ``position_side`` no se especifica o es ``BOTH``, se asume que la posición es LONG para los
            cálculos, pero la orden siempre se envía con ``positionSide='BOTH'`` en modo one-way.
            """
            try:
                # Obtener el precio de mercado actual y el tickSize
                ticker = self.get_ticker_price(symbol)
                mark_price = float(ticker['price'])

                symbol_info = self._get_symbol_info(symbol)
                tick_size = next(
                    float(f['tickSize'])
                    for f in symbol_info.get('filters', [])
                    if f.get('filterType') == 'PRICE_FILTER'
                )

                # Pequeño margen para evitar activación inmediata
                buffer = max(mark_price * 0.00001, tick_size * 10)

                # Determinar orientación (LONG por defecto)
                direction = (position_side or "LONG").upper()

                # Ajustar el stop_loss según dirección y precio de mercado
                if direction == "LONG":
                    # Para LONG se vende cuando el precio cae: stopPrice por debajo del mercado
                    target_price = min(stop_price, mark_price - buffer)
                    side = "SELL"
                elif direction == "SHORT":
                    # Para SHORT se compra cuando el precio sube: stopPrice por encima del mercado
                    target_price = max(stop_price, mark_price + buffer)
                    side = "BUY"
                else:
                    target_price = min(stop_price, mark_price - buffer)
                    side = "SELL"

                # Redondear el stopPrice al tick size
                stop_price_rounded = self._round_price_limit(symbol, target_price)

                # Obtener cantidad de la posición en modo one-way
                qty = self._get_position_quantity(symbol, "BOTH")
                if qty <= 0:
                    self.logger.warning(f"No hay posición abierta para colocar SL en {symbol}")
                    return None

                order_params = {
                    'symbol':      symbol,
                    'side':        side,
                    'type':        'STOP_MARKET',
                    'stopPrice':   stop_price_rounded,
                    'quantity':    qty,
                    'reduceOnly':  'true',
                    'workingType': 'MARK_PRICE',
                    'priceProtect':'true',
                    'positionSide': 'BOTH',
                    'timestamp':   int(time.time() * 1000)
                }

                order = self.client.futures_create_order(**order_params)
                order_id = order.get('orderId') if order else None
                self.logger.info(
                    f"SL MARKET colocado en {stop_price_rounded} para {symbol} "
                    f"(side: {side}, positionSide: BOTH, orderId: {order_id})"
                )
                return order

            except Exception as e:
                self.logger.error(f"Error placing stop loss: {e}")
                return None

        def _round_price(self, symbol: str, price: float) -> str:
            """
            Redondea el precio usando la misma precisión decimal que el precio de mercado actual
            """
            try:
                # Obtener precio de mercado actual
                ticker = self.get_ticker_price(symbol)
                market_price_str = str(float(ticker['price']))
                
                # Determinar número de decimales del precio de mercado
                if '.' in market_price_str:
                    decimal_places = len(market_price_str.split('.')[1].rstrip('0'))
                    # Si no hay decimales significativos, usar al menos 2
                    if decimal_places == 0:
                        decimal_places = 2
                else:
                    decimal_places = 2
                
                # Aplicar la misma precisión al precio dado
                formatted_price = f"{price:.{decimal_places}f}"
                
                self.logger.debug(f"Price formatting for {symbol}: {price} -> {formatted_price} (market precision: {decimal_places})")
                return formatted_price
                
            except Exception as e:
                self.logger.error(f"Error formatting price for {symbol}: {e}")
                # Fallback: usar 6 decimales por defecto
                return f"{price:.6f}".rstrip('0').rstrip('.')
                    
        def _round_price_limit(self, symbol: str, price: float) -> str:
            """
            Redondea `price` siguiendo la precisión que indica PRICE_FILTER.tickSize.
            """
            try:
                # 1) Obtén el tickSize
                symbol_info = self._get_symbol_info(symbol)
                tick_size = next(
                    Decimal(f['tickSize'])
                    for f in symbol_info.get('filters', [])
                    if f.get('filterType') == 'PRICE_FILTER'
                )
                
                # 2) Calcula el exponente (–log10(tick_size)) para saber decimales
                #    Ej: tick_size=0.001 -> precision=3
                precision = int(-tick_size.as_tuple().exponent)
                
                # 3) Cuantiza el precio al múltiplo de tick_size (hacia abajo)
                rounded = (Decimal(price)
                        .quantize(tick_size, rounding=ROUND_DOWN))
                
                # 4) Formatea a string con la precisión exacta
                formatted = format(rounded, f'.{precision}f')
                
                self.logger.debug(
                    f"_round_price {symbol}: {price} → {formatted} "
                    f"(tickSize={tick_size}, precision={precision})"
                )
                return formatted

            except Exception as e:
                self.logger.error(f"Error formatting price for {symbol}: {e}")
                # Fallback seguro a 8 decimales
                return f"{price:.8f}"
        
    # ======================== CONFIGURACIÓN DE CUENTA ========================
        
        def set_leverage(self, symbol: str, leverage: int) -> Optional[Dict]:
            """Establece el apalancamiento para un símbolo"""
            try:
                result = self.client.futures_change_leverage(
                    symbol=symbol,
                    leverage=leverage,
                    timestamp=int(time.time() * 999)
                )
                self.logger.info(f"Leverage set to {leverage}x for {symbol}")
                return result
            except Exception as e:
                self.logger.error(f"Error setting leverage: {e}")
                return None
        
        def set_margin_type(self, symbol: str, margin_type: str) -> Optional[Dict]:
            """Establece el tipo de margen (ISOLATED o CROSSED)"""
            try:
                result = self.client.futures_change_margin_type(
                    symbol=symbol,
                    marginType=margin_type,
                    timestamp=int(time.time() * 999)
                )
                self.logger.info(f"Margin type set to {margin_type} for {symbol}")
                return result
            except Exception as e:
                error_msg = str(e)
                if "No need to change margin type" in error_msg:
                    self.logger.info(f"El tipo de margen ya está en {margin_type} para {symbol}")
                    return {"msg": f"Already in {margin_type} mode"}
                else:
                    self.logger.error(f"Error setting margin type: {e}")
                    return None

        # ======================== INFORMACIÓN DE MERCADO ========================
        
        def get_ticker_price(self, symbol: str) -> Optional[Dict]:
            """Obtiene el precio actual del símbolo"""
            try:
                ticker = self.client.futures_symbol_ticker(symbol=symbol)
                return ticker
            except Exception as e:
                self.logger.error(f"Error getting ticker price: {e}")
                return None
        
        def get_position_info(self, symbol: Optional[str] = None) -> Optional[Union[Dict, List]]:
            """Obtiene información de posiciones"""
            try:
                if symbol:
                    positions = self.client.futures_position_information(symbol=symbol)
                    # Retornar todas las posiciones del símbolo (puede haber LONG y SHORT en hedge mode)
                    relevant_positions = []
                    for position in positions:
                        if position['symbol'] == symbol and float(position['positionAmt']) != 0:
                            relevant_positions.append(position)
                    return relevant_positions if relevant_positions else None
                else:
                    return self.client.futures_position_information()
            except Exception as e:
                self.logger.error(f"Error getting position info: {e}")
                return None
        
        def get_account_info(self) -> Optional[Dict]:
            """Obtiene información general de la cuenta"""
            try:
                return self.client.futures_account()
            except Exception as e:
                self.logger.error(f"Error getting account info: {e}")
                return None
        
        def get_open_orders(self, symbol: Optional[str] = None) -> Optional[List]:
            """Obtiene todas las órdenes abiertas"""
            try:
                if symbol:
                    orders = self.client.futures_get_open_orders()
                else:
                    orders = self.client.futures_get_open_orders()
                return orders
            except Exception as e:
                self.logger.error(f"Error getting open orders: {e}")
                return None

        # ======================== ÓRDENES ========================
        
        def create_market_order(self, symbol: str, side: str, quantity: float,
                                position_side: Optional[str] = None, reduce_only: bool = False) -> Optional[Dict]:
            """
            Crea una orden de mercado con position_side automático.
            
            Args:
                symbol: Símbolo del par
                side: 'BUY' o 'SELL'
                quantity: Cantidad (se asume ya valida)
                position_side: 'BOTH', 'LONG', 'SHORT' (automático si no se especifica)
                reduce_only: True para cerrar posición únicamente
            """
            try:
                # Obtener precio de mercado (por si necesitas usarlo en otra lógica)
                ticker = self.get_ticker_price(symbol)
                mark_price = float(ticker['price'])

                # Redondeo de cantidad y cálculo de positionSide
                quantity_str = self._round_quantity(symbol, quantity)
                correct_position_side = self._get_correct_position_side(side, position_side)

                # Parámetros de la orden
                order_params = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'MARKET',
                    'quantity': quantity_str,
                    'positionSide': correct_position_side,
                    'timestamp': int(time.time() * 1000)
                }
                if reduce_only:
                    order_params['reduceOnly'] = 'true'

                # Creación de la orden
                order = self.client.futures_create_order(**order_params)
                self.logger.info(
                    f"Market order created: {side} {quantity_str} {symbol} "
                    f"(positionSide: {correct_position_side})"
                )
                return order

            except Exception as e:
                self.logger.error(f"Error creating market order: {e}")
                return None

        
        def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                            position_side: Optional[str] = None, time_in_force: str = 'GTC',
                            reduce_only: bool = False) -> Optional[Dict]:
            """Crea una orden límite con position_side automático"""
            try:
                quantity_str = self._round_quantity(symbol, quantity)
                price_str = self._round_price(symbol, price)
                correct_position_side = self._get_correct_position_side(side, position_side)
                
                order_params = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'LIMIT',
                    'quantity': quantity_str,
                    'price': price_str,
                    'timeInForce': time_in_force,
                    'positionSide': correct_position_side,
                    'timestamp': int(time.time() * 999)
                }
                
                if reduce_only:
                    order_params['reduceOnly'] = 'true'
                
                order = self.client.futures_create_order(**order_params)
                self.logger.info(f"Limit order created: {side} {quantity_str} {symbol} @ {price_str}")
                return order
            except Exception as e:
                self.logger.error(f"Error creating limit order: {e}")
                return None

        # ======================== FUNCIONES DE CONVENIENCIA ========================
        
        def open_long_position(self, symbol: str, quantity: float, leverage: Optional[int] = None) -> Optional[Dict]:
            """Abre una posición larga"""
            if leverage:
                self.set_leverage(symbol, leverage)
            
            return self.create_market_order(symbol, 'BUY', quantity)
        
        def open_short_position(self, symbol: str, quantity: float, leverage: Optional[int] = None) -> Optional[Dict]:
            """Abre una posición corta"""
            if leverage:
                self.set_leverage(symbol, leverage)
            
            return self.create_market_order(symbol, 'SELL', quantity)
        
        def close_all_positions(self, symbol: str) -> Dict:
            """
            Cierra COMPLETAMENTE todas las posiciones de un símbolo.
            Antes de cerrar la posición de mercado, cancela en este orden:
              1. Órdenes TP/SL (TAKE_PROFIT_MARKET, STOP_MARKET, TAKE_PROFIT, STOP)
              2. Órdenes LIMIT (de apertura y de cierre)
              3. Algo Orders (órdenes condicionales del endpoint /fapi/v1/algoOrder)
              4. Cierre de la posición a mercado
            """
            summary = {
                'symbol': symbol,
                'tp_sl_cancelled':   None,
                'limit_cancelled':   None,
                'algo_cancelled':    None,
                'positions_closed':  [],
            }
            
                        # ── 1) Cancelar TP / SL ───────────────────────────────────────────────
            self.logger.info(f"[close_all_positions] Cancelando órdenes TP/SL para {symbol}...")
            summary['tp_sl_cancelled'] = self.cancel_all_tp_sl_orders(symbol)

            # ── 2) Cancelar órdenes LIMIT ─────────────────────────────────────────
            self.logger.info(f"[close_all_positions] Cancelando órdenes LIMIT para {symbol}...")
            summary['limit_cancelled'] = None #self.cancel_all_limit_orders(symbol)

            # ── 3) Cancelar Algo Orders ───────────────────────────────────────────
            self.logger.info(f"[close_all_positions] Cancelando Algo Orders para {symbol}...")
            summary['algo_cancelled'] = self.cancel_all_algo_orders(symbol)

            # ── 4) Cerrar posición(es) a mercado ─────────────────────────────────
            positions = self.get_position_info(symbol)
            if not positions:
                self.logger.info(f"[close_all_positions] No hay posiciones abiertas para {symbol}")
                return summary

            if isinstance(positions, dict):
                positions = [positions]

            for position in positions:
                position_amt = float(position['positionAmt'])
                if position_amt != 0:
                    quantity     = abs(position_amt)
                    side         = 'SELL' if position_amt > 0 else 'BUY'
                    position_side = position['positionSide']

                    result = self.create_market_order(
                        symbol, side, quantity, position_side, reduce_only=True
                    )
                    summary['positions_closed'].append(result)

                    if result:
                        self.logger.info(
                            f"[close_all_positions] ✅ Posición cerrada: "
                            f"{position_side} {quantity} {symbol}"
                        )
                    else:
                        self.logger.error(
                            f"[close_all_positions] ❌ Error cerrando posición "
                            f"{position_side} {quantity} {symbol}"
                        )

            return summary
        
        def get_position_summary(self, symbol: str) -> Dict:
            """Obtiene un resumen de las posiciones"""
            positions = self.get_position_info(symbol)
            summary = {
                'symbol': symbol,
                'mode': self._position_mode,
                'positions': [],
                'total_pnl': 0.0
            }
            
            if positions:
                if isinstance(positions, dict):
                    positions = [positions]
                
                for pos in positions:
                    if float(pos['positionAmt']) != 0:
                        pos_info = {
                            'side': pos['positionSide'],
                            'size': float(pos['positionAmt']),
                            'entry_price': float(pos['entryPrice']),
                            'mark_price': float(pos['markPrice']),
                            'pnl': float(pos['unRealizedProfit']),
                            'percentage': float(pos['percentage'])
                        }
                        summary['positions'].append(pos_info)
                        summary['total_pnl'] += pos_info['pnl']
            
            return summary
        
        
            # Funciones LIMIT para agregar a la clase BinanceAPI
        # Agregar estas funciones a tu clase BinanceAPI en binance_api_mejorado.py

        def limit_open_long(self, symbol: str, quantity: float, limit_price: float, 
                        leverage: Optional[int] = None, 
                        time_in_force: str = 'GTC') -> Optional[Dict]:
            """
            Abre posición LONG con orden LIMIT
            
            Args:
                symbol: Par de trading (ej: "BTCUSDT")
                quantity: Cantidad a comprar
                limit_price: Precio límite máximo para la compra
                leverage: Apalancamiento (opcional)
                time_in_force: Tipo de validez ('GTC', 'IOC', 'FOK')
            
            Returns:
                Dict con información de la orden o None si falla
            """
            try:
                # Configurar leverage si se especifica
                if leverage:
                    leverage_result = self.set_leverage(symbol, leverage)
                    if not leverage_result:
                        self.logger.warning(f"No se pudo configurar leverage {leverage}x para {symbol}")
                
                # Obtener precio actual para validación
                ticker = self.get_ticker_price(symbol)
                if not ticker:
                    self.logger.error(f"No se pudo obtener precio actual de {symbol}")
                    return None
                
                current_price = float(ticker['price'])
                
                # Validar que el precio límite sea menor al actual (para LONG)
                if limit_price >= current_price:
                    self.logger.warning(
                        f"Precio límite ${limit_price:.4f} debe ser menor al actual ${current_price:.4f} para LONG"
                    )
                    # Auto-ajustar a 0.1% por debajo del precio actual
                    limit_price = current_price * 0.99999
                    self.logger.info(f"Auto-ajustando precio límite a ${limit_price:.4f}")
                
                # Redondear cantidad y precio
                quantity_str = self._round_quantity(symbol, quantity)
                price_str = self._round_price(symbol, limit_price)
                
                # Obtener positionSide correcto
                correct_position_side = self._get_correct_position_side('BUY')
                
                # Crear orden LIMIT de compra
                order_params = {
                    'symbol': symbol,
                    'side': 'BUY',
                    'type': 'LIMIT',
                    'quantity': quantity_str,
                    'price': price_str,
                    'timeInForce': time_in_force,
                    'positionSide': correct_position_side,
                    'timestamp': int(time.time() * 1000)
                }
                
                order = self.client.futures_create_order(**order_params)
                
                if order:
                    order_id = order['orderId']
                    self.logger.info(f"LONG LIMIT creada: {symbol} | {quantity_str} @ ${price_str}")
                    return order
                
            except Exception as e:
                self.logger.error(f"Error abriendo LONG LIMIT para {symbol}: {e}")
                return None

        def limit_open_short(self, symbol: str, quantity: float, limit_price: float, 
                            leverage: Optional[int] = None, 
                            time_in_force: str = 'GTC') -> Optional[Dict]:
            """
            Abre posición SHORT con orden LIMIT
            
            Args:
                symbol: Par de trading (ej: "BTCUSDT")
                quantity: Cantidad a vender
                limit_price: Precio límite mínimo para la venta
                leverage: Apalancamiento (opcional)
                time_in_force: Tipo de validez ('GTC', 'IOC', 'FOK')
            
            Returns:
                Dict con información de la orden o None si falla
            """
            try:
                # Configurar leverage si se especifica
                if leverage:
                    leverage_result = self.set_leverage(symbol, leverage)
                    if not leverage_result:
                        self.logger.warning(f"No se pudo configurar leverage {leverage}x para {symbol}")
                
                # Obtener precio actual para validación
                ticker = self.get_ticker_price(symbol)
                if not ticker:
                    self.logger.error(f"No se pudo obtener precio actual de {symbol}")
                    return None
                
                current_price = float(ticker['price'])
                
                # Validar que el precio límite sea mayor al actual (para SHORT)
                if limit_price <= current_price:
                    self.logger.warning(
                        f"Precio límite ${limit_price:.4f} debe ser mayor al actual ${current_price:.4f} para SHORT"
                    )
                    # Auto-ajustar a 0.1% por encima del precio actual
                    limit_price = current_price * 1.00001
                    self.logger.info(f"Auto-ajustando precio límite a ${limit_price:.4f}")
                
                # Redondear cantidad y precio
                quantity_str = self._round_quantity(symbol, quantity)
                price_str = self._round_price(symbol, limit_price)
                
                # Obtener positionSide correcto
                correct_position_side = self._get_correct_position_side('SELL')
                
                # Crear orden LIMIT de venta
                order_params = {
                    'symbol': symbol,
                    'side': 'SELL',
                    'type': 'LIMIT',
                    'quantity': quantity_str,
                    'price': price_str,
                    'timeInForce': time_in_force,
                    'positionSide': correct_position_side,
                    'timestamp': int(time.time() * 1000)
                }
                
                order = self.client.futures_create_order(**order_params)
                
                if order:
                    order_id = order['orderId']
                    self.logger.info(f"SHORT LIMIT creada: {symbol} | {quantity_str} @ ${price_str}")
                    return order
                
            except Exception as e:
                self.logger.error(f"Error abriendo SHORT LIMIT para {symbol}: {e}")
                return None

        def limit_exit_long(self, symbol: str, quantity: Optional[float] = None, 
                        limit_price: Optional[float] = None, 
                        time_in_force: str = 'GTC') -> Optional[Dict]:
            """
            Cierra posición LONG con orden LIMIT (vende)
            
            Args:
                symbol: Par de trading
                quantity: Cantidad a cerrar (None para cerrar toda la posición)
                limit_price: Precio límite mínimo de venta (None para precio actual + 0.1%)
                time_in_force: Tipo de validez
            
            Returns:
                Dict con información de la orden o None si falla
            """
            try:
                # Obtener información de la posición si no se especifica cantidad
                if quantity is None:
                    position_info = self.get_position_info(symbol)
                    if not position_info:
                        self.logger.error(f"No se encontró posición LONG para {symbol}")
                        return None
                    
                    # Manejar si position_info es una lista o dict
                    if isinstance(position_info, list):
                        long_position = None
                        for pos in position_info:
                            if float(pos['positionAmt']) > 0:  # Posición LONG
                                long_position = pos
                                break
                        if not long_position:
                            self.logger.error(f"No se encontró posición LONG activa para {symbol}")
                            return None
                        position_amt = float(long_position['positionAmt'])
                    else:
                        position_amt = float(position_info['positionAmt'])
                        if position_amt <= 0:
                            self.logger.error(f"No hay posición LONG para cerrar en {symbol}")
                            return None
                    
                    quantity = abs(position_amt)
                
                # Obtener precio actual si no se especifica límite
                if limit_price is None:
                    ticker = self.get_ticker_price(symbol)
                    if not ticker:
                        self.logger.error(f"No se pudo obtener precio actual de {symbol}")
                        return None
                    current_price = float(ticker['price'])
                    # Para salir de LONG, queremos vender a precio igual o mayor
                    limit_price = current_price * 1.001  # 0.1% por encima
                
                # Validar precio límite
                ticker = self.get_ticker_price(symbol)
                if ticker:
                    current_price = float(ticker['price'])
                    if limit_price < current_price * 0.95:  # Más de 5% por debajo del actual
                        self.logger.warning(
                            f"Precio límite ${limit_price:.4f} muy bajo comparado con actual ${current_price:.4f}"
                        )
                
                # Redondear cantidad y precio
                quantity_str = self._round_quantity(symbol, quantity)
                price_str = self._round_price(symbol, limit_price)
                
                # Obtener positionSide correcto para cerrar LONG
                correct_position_side = self._get_correct_position_side('SELL', 'LONG')
                
                # Crear orden LIMIT de venta para cerrar LONG
                order_params = {
                    'symbol': symbol,
                    'side': 'SELL',
                    'type': 'LIMIT',
                    'quantity': quantity_str,
                    'price': price_str,
                    'timeInForce': time_in_force,
                    'reduceOnly': 'true',
                    'positionSide': correct_position_side,
                    'timestamp': int(time.time() * 1000)
                }
                
                order = self.client.futures_create_order(**order_params)
                
                if order:
                    order_id = order['orderId']
                    self.logger.info(f"EXIT LONG LIMIT creada: {symbol} | {quantity_str} @ ${price_str}")
                    return order
                
            except Exception as e:
                self.logger.error(f"Error cerrando LONG LIMIT para {symbol}: {e}")
                return None

        def limit_exit_short(self, symbol: str, quantity: Optional[float] = None, 
                            limit_price: Optional[float] = None, 
                            time_in_force: str = 'GTC') -> Optional[Dict]:
            """
            Cierra posición SHORT con orden LIMIT (compra)
            
            Args:
                symbol: Par de trading
                quantity: Cantidad a cerrar (None para cerrar toda la posición)
                limit_price: Precio límite máximo de compra (None para precio actual - 0.1%)
                time_in_force: Tipo de validez
            
            Returns:
                Dict con información de la orden o None si falla
            """
            try:
                # Obtener información de la posición si no se especifica cantidad
                if quantity is None:
                    position_info = self.get_position_info(symbol)
                    if not position_info:
                        self.logger.error(f"No se encontró posición SHORT para {symbol}")
                        return None
                    
                    # Manejar si position_info es una lista o dict
                    if isinstance(position_info, list):
                        short_position = None
                        for pos in position_info:
                            if float(pos['positionAmt']) < 0:  # Posición SHORT
                                short_position = pos
                                break
                        if not short_position:
                            self.logger.error(f"No se encontró posición SHORT activa para {symbol}")
                            return None
                        position_amt = float(short_position['positionAmt'])
                    else:
                        position_amt = float(position_info['positionAmt'])
                        if position_amt >= 0:
                            self.logger.error(f"No hay posición SHORT para cerrar en {symbol}")
                            return None
                    
                    quantity = abs(position_amt)
                
                # Obtener precio actual si no se especifica límite
                if limit_price is None:
                    ticker = self.get_ticker_price(symbol)
                    if not ticker:
                        self.logger.error(f"No se pudo obtener precio actual de {symbol}")
                        return None
                    current_price = float(ticker['price'])
                    # Para salir de SHORT, queremos comprar a precio igual o menor
                    limit_price = current_price * 0.999  # 0.1% por debajo
                
                # Validar precio límite
                ticker = self.get_ticker_price(symbol)
                if ticker:
                    current_price = float(ticker['price'])
                    if limit_price > current_price * 1.05:  # Más de 5% por encima del actual
                        self.logger.warning(
                            f"Precio límite ${limit_price:.4f} muy alto comparado con actual ${current_price:.4f}"
                        )
                
                # Redondear cantidad y precio
                quantity_str = self._round_quantity(symbol, quantity)
                price_str = self._round_price(symbol, limit_price)
                
                # Obtener positionSide correcto para cerrar SHORT
                correct_position_side = self._get_correct_position_side('BUY', 'SHORT')
                
                # Crear orden LIMIT de compra para cerrar SHORT
                order_params = {
                    'symbol': symbol,
                    'side': 'BUY',
                    'type': 'LIMIT',
                    'quantity': quantity_str,
                    'price': price_str,
                    'timeInForce': time_in_force,
                    'reduceOnly': 'true',
                    'positionSide': correct_position_side,
                    'timestamp': int(time.time() * 1000)
                }
                
                order = self.client.futures_create_order(**order_params)
                
                if order:
                    order_id = order['orderId']
                    self.logger.info(f"EXIT SHORT LIMIT creada: {symbol} | {quantity_str} @ ${price_str}")
                    return order
                
            except Exception as e:
                self.logger.error(f"Error cerrando SHORT LIMIT para {symbol}: {e}")
                return None

        def cancel_limit_long(self, symbol: str, order_id: Optional[str] = None) -> Optional[Dict]:
            """
            Cancela orden LIMIT de apertura LONG específica o todas las órdenes LONG del símbolo
            
            Args:
                symbol: Par de trading
                order_id: ID específico de la orden (None para cancelar todas las LONG)
            
            Returns:
                Dict con información de cancelación o None si falla
            """
            try:
                # Si se especifica order_id, cancelar solo esa orden
                if order_id:
                    cancel_result = self.client.futures_cancel_order(
                        symbol=symbol,
                        orderId=order_id,
                        timestamp=int(time.time() * 1000)
                    )
                    self.logger.info(f"Orden LONG cancelada: {symbol} | ID: {order_id}")
                    return cancel_result
                
                # Si no se especifica order_id, cancelar todas las órdenes LONG del símbolo
                open_orders = self.get_open_orders(symbol)
                if not open_orders:
                    self.logger.info(f"No hay órdenes abiertas para {symbol}")
                    return None
                
                cancelled_orders = []
                long_orders_found = 0
                
                for order in open_orders:
                    # Identificar órdenes LONG (BUY para abrir, SELL para cerrar con reduceOnly)
                    is_long_open = (order['side'] == 'BUY' and 
                                order.get('reduceOnly', False) == False and
                                order['type'] in ['LIMIT'])
                    
                    is_long_close = (order['side'] == 'SELL' and 
                                    order.get('reduceOnly', False) == True and
                                    order['type'] in ['LIMIT'])
                    
                    if is_long_open or is_long_close:
                        long_orders_found += 1
                        try:
                            cancel_result = self.client.futures_cancel_order(
                                symbol=symbol,
                                orderId=order['orderId'],
                                timestamp=int(time.time() * 1000)
                            )
                            cancelled_orders.append({
                                'orderId': order['orderId'],
                                'side': order['side'],
                                'type': order['type'],
                                'price': order['price'],
                                'quantity': order['origQty'],
                                'result': 'CANCELLED'
                            })
                            self.logger.info(f"Orden LONG cancelada: {order['side']} {symbol} @ ${order['price']}")
                        except Exception as e:
                            cancelled_orders.append({
                                'orderId': order['orderId'],
                                'result': 'ERROR',
                                'error': str(e)
                            })
                
                if long_orders_found == 0:
                    self.logger.info(f"No se encontraron órdenes LONG para {symbol}")
                    return None
                
                summary = {
                    'symbol': symbol,
                    'total_long_orders_found': long_orders_found,
                    'cancelled_orders': cancelled_orders,
                    'successful_cancellations': len([o for o in cancelled_orders if o['result'] == 'CANCELLED'])
                }
                
                self.logger.info(f"Cancelación LONG completada: {symbol} | "
                                f"{summary['successful_cancellations']}/{long_orders_found} órdenes canceladas")
                
                return summary
                
            except Exception as e:
                self.logger.error(f"Error cancelando órdenes LONG para {symbol}: {e}")
                return None

        def cancel_limit_short(self, symbol: str, order_id: Optional[str] = None) -> Optional[Dict]:
            """
            Cancela orden LIMIT de apertura SHORT específica o todas las órdenes SHORT del símbolo
            
            Args:
                symbol: Par de trading
                order_id: ID específico de la orden (None para cancelar todas las SHORT)
            
            Returns:
                Dict con información de cancelación o None si falla
            """
            try:
                # Si se especifica order_id, cancelar solo esa orden
                if order_id:
                    cancel_result = self.client.futures_cancel_order(
                        symbol=symbol,
                        orderId=order_id,
                        timestamp=int(time.time() * 1000)
                    )
                    self.logger.info(f"Orden SHORT cancelada: {symbol} | ID: {order_id}")
                    return cancel_result
                
                # Si no se especifica order_id, cancelar todas las órdenes SHORT del símbolo
                open_orders = self.get_open_orders(symbol)
                if not open_orders:
                    self.logger.info(f"No hay órdenes abiertas para {symbol}")
                    return None
                
                cancelled_orders = []
                short_orders_found = 0
                
                for order in open_orders:
                    # Identificar órdenes SHORT (SELL para abrir, BUY para cerrar con reduceOnly)
                    is_short_open = (order['side'] == 'SELL' and 
                                    order.get('reduceOnly', False) == False and
                                    order['type'] in ['LIMIT'])
                    
                    is_short_close = (order['side'] == 'BUY' and 
                                    order.get('reduceOnly', False) == True and
                                    order['type'] in ['LIMIT'])
                    
                    if is_short_open or is_short_close:
                        short_orders_found += 1
                        try:
                            cancel_result = self.client.futures_cancel_order(
                                symbol=symbol,
                                orderId=order['orderId'],
                                timestamp=int(time.time() * 1000)
                            )
                            cancelled_orders.append({
                                'orderId': order['orderId'],
                                'side': order['side'],
                                'type': order['type'],
                                'price': order['price'],
                                'quantity': order['origQty'],
                                'result': 'CANCELLED'
                            })
                            self.logger.info(f"Orden SHORT cancelada: {order['side']} {symbol} @ ${order['price']}")
                        except Exception as e:
                            cancelled_orders.append({
                                'orderId': order['orderId'],
                                'result': 'ERROR',
                                'error': str(e)
                            })
                
                if short_orders_found == 0:
                    self.logger.info(f"No se encontraron órdenes SHORT para {symbol}")
                    return None
                
                summary = {
                    'symbol': symbol,
                    'total_short_orders_found': short_orders_found,
                    'cancelled_orders': cancelled_orders,
                    'successful_cancellations': len([o for o in cancelled_orders if o['result'] == 'CANCELLED'])
                }
                
                self.logger.info(f"Cancelación SHORT completada: {symbol} | "
                                f"{summary['successful_cancellations']}/{short_orders_found} órdenes canceladas")
                
                return summary
                
            except Exception as e:
                self.logger.error(f"Error cancelando órdenes SHORT para {symbol}: {e}")
                return None

        def cancel_all_limit_orders(self, symbol: str) -> Optional[Dict]:
            """
            Cancela TODAS las órdenes LIMIT del símbolo (LONG y SHORT)
            
            Args:
                symbol: Par de trading
            
            Returns:
                Dict con resumen de cancelaciones
            """
            try:
                self.logger.info(f"Iniciando cancelación de todas las órdenes LIMIT para {symbol}")
                
                # Cancelar órdenes LONG
                long_result = self.cancel_limit_long(symbol)
                
                # Cancelar órdenes SHORT  
                short_result = self.cancel_limit_short(symbol)
                
                # Compilar resumen
                summary = {
                    'symbol': symbol,
                    'long_cancellations': long_result,
                    'short_cancellations': short_result,
                    'total_cancelled': 0
                }
                
                if long_result:
                    summary['total_cancelled'] += long_result.get('successful_cancellations', 0)
                
                if short_result:
                    summary['total_cancelled'] += short_result.get('successful_cancellations', 0)
                
                self.logger.info(f"Cancelación completa: {symbol} | "
                                f"{summary['total_cancelled']} órdenes LIMIT canceladas")
                
                return summary
                
            except Exception as e:
                self.logger.error(f"Error cancelando todas las órdenes LIMIT para {symbol}: {e}")
                return None
        
        def cancel_all_tp_sl_orders(self, symbol: str) -> Optional[Dict]:
            """
            Cancela todas las órdenes TAKE_PROFIT_MARKET y STOP_MARKET activas para un símbolo.
            
            Args:
                symbol: Par de trading (ej. BTCUSDT)
                
            Returns:
                Resumen de cancelaciones
            """
            try:
                open_orders = self.get_open_orders(symbol)
                if not open_orders:
                    self.logger.info(f"No hay órdenes abiertas para {symbol}")
                    return None
                
                cancelled_orders = []
                tp_sl_orders_found = 0
                
                for order in open_orders:
                    if order['type'] in ['TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'STOP', 'STOP_MARKET']:
                        tp_sl_orders_found += 1
                        try:
                            cancel_result = self.client.futures_cancel_order(
                                symbol=symbol,
                                orderId=order['orderId'],
                                timestamp=int(time.time() * 1000)
                            )
                            cancelled_orders.append({
                                'orderId': order['orderId'],
                                'type': order['type'],
                                'result': 'CANCELLED'
                            })
                            self.logger.info(f"Orden TP/SL cancelada: {order['type']} {symbol}")
                        except Exception as e:
                            cancelled_orders.append({
                                'orderId': order['orderId'],
                                'result': 'ERROR',
                                'error': str(e)
                            })
                
                summary = {
                    'symbol': symbol,
                    'total_tp_sl_orders_found': tp_sl_orders_found,
                    'cancelled_orders': cancelled_orders,
                    'successful_cancellations': len([o for o in cancelled_orders if o['result'] == 'CANCELLED'])
                }
                
                self.logger.info(f"Cancelación TP/SL completada: {symbol} | "
                                f"{summary['successful_cancellations']}/{tp_sl_orders_found} órdenes canceladas")
                
                return summary
            
            except Exception as e:
                self.logger.error(f"Error cancelando órdenes TP/SL para {symbol}: {e}")
                return None


                # ============ BATCH ORDERS / BRACKET EN 1 REQUEST ============
      
        def _normalize_batch_order(self, symbol: str, order: dict) -> dict:
            """
            Normaliza una sub-orden para batch:
            - Rellena/valida positionSide con _get_correct_position_side
            - Redondea quantity, price, stopPrice
            - Asegura newClientOrderId <= 36 chars
            """
            import time, json, math
            o = order.copy()

            # symbol puede venir en cada orden; si no, usamos el que nos pasan arriba
            o.setdefault('symbol', symbol)

            # positionSide automático si no lo especifican
            side = o.get('side')
            if side:
                forced = o.get('positionSide')  # 'BOTH' | 'LONG' | 'SHORT' (opcional)
                o['positionSide'] = self._get_correct_position_side(side, forced)

            # timeInForce por defecto para órdenes con price
            if o.get('type') in ('LIMIT', 'STOP', 'TAKE_PROFIT') and 'timeInForce' not in o:
                o['timeInForce'] = 'GTC'

            # Redondeos
            if 'quantity' in o:
                o['quantity'] = self._round_quantity(o['symbol'], float(o['quantity']))
            if 'price' in o:
                o['price'] = self._round_price(o['symbol'], float(o['price']))
            if 'stopPrice' in o:
                # Para triggers usamos el filtro de tickSize exacto
                o['stopPrice'] = self._round_price_limit(o['symbol'], float(o['stopPrice']))

         

            # reduceOnly debe ser boolean/bool-compatible
            if 'reduceOnly' in o:
                o['reduceOnly'] = 'true' if o['reduceOnly'] else 'false'

            return o

        def place_batch_orders(self, orders: list, symbol: str = None, max_retries: int = 1):
            """
            Envía varias órdenes en 1 request al endpoint batch de Binance Futures.
            - 'orders' es una lista de dicts con los mismos campos que futures_create_order
            - Opcionalmente pasa 'symbol' para normalizar redondeos al mismo par
            Devuelve la lista de resultados por sub-orden (en el mismo orden).
            """
            import json, time

            if not isinstance(orders, list) or len(orders) == 0:
                raise ValueError("orders debe ser una lista con al menos 1 elemento")
            if len(orders) > 5:
                raise ValueError("Binance Futures batchOrders acepta máximo 5 sub-órdenes")

            # Normalización previa para minimizar rechazos de PRICE_FILTER/stepSize
            norm = [self._normalize_batch_order(o.get('symbol', symbol) or orders[0].get('symbol'), o)
                    for o in orders]

            payload = {'batchOrders': json.dumps(norm), 'timestamp': int(time.time() * 1000)}

            # Llamamos al método de la librería si existe; si no, usamos la ruta interna
            for attempt in range(1, max_retries + 1):
                try:
                    if hasattr(self.client, 'futures_place_batch_order'):
                        resp = self.client.futures_place_batch_order(batchOrders=norm)
                    else:
                        # Fallback al request interno si la versión de python-binance no expone el helper
                        resp = self.client._request_futures_api('post', 'batchOrders', True, data=payload)
                    # Actualiza cache TP/SL si procede
                    try:
                        for i, r in enumerate(resp or []):
                            # 1. VERIFICAR SI HAY ERROR PRIMERO
                            if 'code' in r:
                                error_code = r.get('code')
                                error_msg = r.get('msg')
                                print(f"❌ Error en orden índice {i}: Código {error_code} - {error_msg}")
                                
                                self.logger.error(f"Orden Batch {i} falló: {error_code} {error_msg}. Reintentando vía Algo Order...")

                                # Extraemos los datos de la orden original que falló
                                original_order = norm[i]
                                o_type = original_order.get('type')
                                o_symbol = original_order.get('symbol')
                                o_quantity = original_order.get('quantity')
                                o_price = original_order.get('price')
                                # En órdenes condicionales, el precio de activación suele venir en 'stopPrice'
                                o_trigger = float(original_order.get('stopPrice', 0))
                                o_side = original_order.get('side')
                                o_pos_side = original_order.get('positionSide')

                                # 2. REINTENTO USANDO CREATE_ALGO_ORDER
                                if o_type in ('TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'STOP', 'STOP_MARKET'):
                                    self.create_algo_order(
                                        symbol=o_symbol,
                                        side=o_side,
                                        order_type=o_type,
                                        quantity=float(o_quantity),
                                        price=float(o_price),
                                        trigger_price=o_trigger,
                                        position_side=o_pos_side,
                                        close_position=True, # Normalmente TP/SL en batch son para cerrar posición
                                        reduce_only=original_order.get('reduceOnly', 'false').lower() == 'true'
                                    )
                                
                                continue # Salta a la siguiente orden del batch


                            # 2. SI NO HAY ERROR, PROCESAR NORMALMENTE
                            typ = str(r.get('type', '')).upper()
                            sym = r.get('symbol')
                            
                            if typ in ('TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'STOP', 'STOP_MARKET') and sym:
                                self.tp_sl_orders.setdefault(sym, {'tp': None, 'sl': None})
                                
                                if 'TAKE_PROFIT' in typ:
                                    self.tp_sl_orders[sym]['tp'] = r.get('orderId')
                                    print(f"✅ TP registrado para {sym}: {r.get('orderId')}")
                                    
                                if 'STOP' in typ:
                                    self.tp_sl_orders[sym]['sl'] = r.get('orderId')
                                    print(f"✅ SL registrado para {sym}: {r.get('orderId')}")
                    
                    
                    except Exception:
                        pass
                    return resp
                except Exception as e:
                    self.logger.warning(f"Batch intento {attempt} falló: {e}")
                    if attempt >= max_retries:
                        raise

        def bracket_batch(self, symbol: str, side: str, quantity: float,
                        entry_type: str = 'MARKET',
                        entry_price: float = None,
                        take_profit: float = None,
                        stop_loss: float = None,
                        time_in_force: str = 'GTC',
                        position_side: str = None):
            """
            Envía EN 1 REQUEST:
            - Orden de entrada (MARKET o LIMIT)
            - TP (TAKE_PROFIT LIMIT) reduceOnly
            - SL (STOP LIMIT) reduceOnly
            """
            side = side.upper()
            assert side in ('BUY', 'SELL')
            qty = float(quantity)

            orders = []

            # 1) Entrada
            entry = {
                'symbol': symbol,
                'side': side,
                'type': entry_type.upper(),
                'quantity': qty,
                'positionSide': self._get_correct_position_side(side, position_side),
            }
            if entry['type'] == 'LIMIT':
                if entry_price is None:
                    raise ValueError("entry_price es requerido para LIMIT")
                entry['price'] = entry_price
                entry['timeInForce'] = time_in_force
            orders.append(entry)

            # 2) TP (opcional)
            if take_profit is not None:
                # Para LONG vendemos; para SHORT compramos
                tp_side = 'SELL' if side == 'BUY' else 'BUY'
                tp_sp = float(take_profit)
                tp_price = tp_sp  # usamos limit = stopPrice para precisión
                orders.append({
                    'symbol': symbol,
                    'side': tp_side,
                    'type': 'TAKE_PROFIT',
                    'stopPrice': tp_sp,
                    'price': tp_price,
                    'quantity': qty,
                    'timeInForce': 'GTC',
                    'reduceOnly': True,
                    'positionSide': self._get_correct_position_side(tp_side, position_side if position_side in ('LONG','SHORT') else 'BOTH')
                })

            # 3) SL (opcional)
            if stop_loss is not None:
                sl_side = 'SELL' if side == 'BUY' else 'BUY'
                sl_sp = float(stop_loss)
                # límite ligeramente peor para asegurar fill al disparar
                limit_worse = sl_sp * (0.9995 if sl_side == 'SELL' else 1.0005)
                orders.append({
                    'symbol': symbol,
                    'side': sl_side,
                    'type': 'STOP',
                    'stopPrice': sl_sp,
                    'price': limit_worse,
                    'quantity': qty,
                    'timeInForce': 'GTC',
                    'reduceOnly': True,
                    'positionSide': self._get_correct_position_side(sl_side, position_side if position_side in ('LONG','SHORT') else 'BOTH')
                })

            return self.place_batch_orders(orders, symbol=symbol)

        # ============ PYRAMIDING BATCH ============

        def pyramiding_batch(
            self,
            symbol: str,
            side: str,
            entries: List[Dict],
            position_side: str = None,
            time_in_force: str = 'GTC',
            validate_prices: bool = True,
            leverage: Optional[int] = None,
        ) -> Optional[List]:
            """
            Envía hasta 5 órdenes de entrada (pyramiding) en UN SOLO REQUEST al endpoint
            batchOrders de Binance Futures.

            Cada entrada en ``entries`` es un dict con:
                - 'quantity'  (float, obligatorio): cantidad de la orden.
                - 'price'     (float, obligatorio para LIMIT): precio límite de entrada.
                - 'type'      (str,   opcional): 'LIMIT' (default) | 'MARKET'.

            Jerarquía de fallbacks (de más a menos prioritario):
                1. place_batch_orders  → intento principal en 1 request.
                2. create_limit_order / create_market_order → si el batch falla total o
                   si una sub-orden viene rechazada dentro del batch.
                3. create_algo_order (endpoint /fapi/v1/algoOrder) → último recurso,
                   igual que hace bracket_batch internamente vía place_batch_orders.

            Args:
                symbol:          Par de trading, p.ej. "BTCUSDT".
                side:            'BUY'  → pirámide LONG (compras escalonadas)
                                 'SELL' → pirámide SHORT (ventas escalonadas)
                entries:         Lista de 1–5 dicts que describen cada entrada.
                position_side:   'LONG' | 'SHORT' | 'BOTH' (auto si no se pasa).
                time_in_force:   Validez de las órdenes LIMIT ('GTC', 'IOC', 'FOK').
                validate_prices: Si True, verifica que los precios sean coherentes con
                                 el precio de mercado actual y la dirección de la operación.
                leverage:        Si se indica, configura el apalancamiento antes de enviar.

            Returns:
                Lista de resultados por sub-orden (igual que place_batch_orders).

            Raises:
                ValueError: Si entries está vacío, supera 5 órdenes o faltan campos
                            obligatorios.

            Ejemplo de uso:
                # Pirámide LONG de 4 niveles: comprar más cuanto más baje BTC
                api.pyramiding_batch(
                    symbol="BTCUSDT",
                    side="BUY",
                    entries=[
                        {'price': 95_000, 'quantity': 0.002},
                        {'price': 93_000, 'quantity': 0.003},
                        {'price': 91_000, 'quantity': 0.005},
                        {'price': 89_000, 'quantity': 0.008},
                    ],
                    leverage=20,
                )

                # Pirámide SHORT de 3 niveles: vender más cuanto más suba ETH
                api.pyramiding_batch(
                    symbol="ETHUSDT",
                    side="SELL",
                    entries=[
                        {'price': 3_600, 'quantity': 0.5},
                        {'price': 3_700, 'quantity': 0.7},
                        {'price': 3_800, 'quantity': 1.0},
                    ],
                )
            """

            # ─────────────────────────────────────────────────────────────────────
            # HELPER INTERNO: último recurso vía create_algo_order
            # Para órdenes de entrada condicionadas por precio usamos:
            #   - BUY  LIMIT (precio < mercado) → la orden espera que el precio BAJE  → STOP_MARKET / STOP
            #   - SELL LIMIT (precio > mercado) → la orden espera que el precio SUBA  → STOP_MARKET / STOP
            # Ambos casos modelan una entrada condicional cuyo trigger es el precio objetivo.
            # ─────────────────────────────────────────────────────────────────────
            def _algo_fallback(entry: Dict, idx: int) -> Optional[Dict]:
                """
                Intenta colocar la entrada vía create_algo_order como último recurso.
                - MARKET → STOP_MARKET con triggerPrice = precio de mercado actual.
                - LIMIT  → STOP        con triggerPrice = price y price limit = price
                           (orden condicional con límite al mismo nivel de entrada).
                """
                try:
                    if entry['type'] == 'MARKET':
                        # Para MARKET usamos el precio actual como trigger referencial
                        current = float(self.get_ticker_price(symbol)['price'])
                        algo_r = self.create_algo_order(
                            symbol        = symbol,
                            side          = side,
                            order_type    = 'STOP_MARKET',
                            quantity      = entry['quantity'],
                            trigger_price = current,
                            position_side = pos_side,
                            reduce_only   = False,
                        )
                    else:
                        # Para LIMIT usamos STOP (condicional con límite) → trigger + precio igual
                        algo_r = self.create_algo_order(
                            symbol        = symbol,
                            side          = side,
                            order_type    = 'STOP',
                            quantity      = entry['quantity'],
                            price         = entry['price'],
                            trigger_price = entry['price'],
                            position_side = pos_side,
                            reduce_only   = False,
                        )

                    if algo_r and algo_r.get('algoId'):
                        self.logger.info(
                            f"   ✅ [algo_fallback] Entrada {idx} enviada vía Algo Order: "
                            f"algoId={algo_r.get('algoId')} | type={algo_r.get('type')}"
                        )
                    else:
                        self.logger.error(
                            f"   ❌ [algo_fallback] Entrada {idx} también falló en Algo Order."
                        )
                    return algo_r

                except Exception as algo_exc:
                    self.logger.error(
                        f"   ❌ [algo_fallback] Excepción en Algo Order para entrada {idx}: {algo_exc}"
                    )
                    return None

            # ── 0) Validaciones iniciales ─────────────────────────────────────────
            side = side.upper()
            if side not in ('BUY', 'SELL'):
                raise ValueError(f"side debe ser 'BUY' o 'SELL', recibido: {side!r}")

            if not entries or not isinstance(entries, list):
                raise ValueError("entries debe ser una lista con al menos 1 elemento.")

            if len(entries) > 5:
                raise ValueError(
                    f"Binance batchOrders acepta máximo 5 sub-órdenes. "
                    f"Recibiste {len(entries)}. Divide en varias llamadas."
                )

            # Normalizar tipo de orden por entrada
            normalized_entries = []
            for i, e in enumerate(entries):
                entry = e.copy()
                entry.setdefault('type', 'LIMIT')
                entry['type'] = entry['type'].upper()

                if entry['type'] not in ('LIMIT', 'MARKET'):
                    raise ValueError(
                        f"entry[{i}]['type'] debe ser 'LIMIT' o 'MARKET', "
                        f"recibido: {entry['type']!r}"
                    )

                if 'quantity' not in entry or entry['quantity'] is None:
                    raise ValueError(f"entry[{i}] requiere 'quantity'.")

                if entry['type'] == 'LIMIT' and ('price' not in entry or entry['price'] is None):
                    raise ValueError(
                        f"entry[{i}] requiere 'price' porque type='LIMIT'."
                    )

                entry['quantity'] = float(entry['quantity'])
                if entry['type'] == 'LIMIT':
                    entry['price'] = float(entry['price'])

                normalized_entries.append(entry)

            # ── 1) Configurar apalancamiento si se pide ───────────────────────────
            if leverage:
                lev_result = self.set_leverage(symbol, leverage)
                if lev_result is None:
                    self.logger.warning(
                        f"⚠️  No se pudo configurar leverage {leverage}x para {symbol}. "
                        f"Se continúa con el leverage actual."
                    )

            # ── 2) Obtener precio de mercado para validaciones ────────────────────
            ticker = self.get_ticker_price(symbol)
            if ticker is None:
                raise RuntimeError(f"No se pudo obtener el precio actual de {symbol}.")
            market_price = float(ticker['price'])
            self.logger.info(f"[pyramiding_batch] Precio de mercado {symbol}: {market_price}")

            # ── 3) Validar precios de las entradas LIMIT ──────────────────────────
            if validate_prices:
                symbol_info   = self._get_symbol_info(symbol)
                min_notional  = next(
                    (float(f.get('notional', f.get('minNotional', 0)))
                     for f in symbol_info.get('filters', [])
                     if f['filterType'] in ('MIN_NOTIONAL', 'NOTIONAL')),
                    0.0
                )
                lot_size_filter = next(
                    (f for f in symbol_info.get('filters', []) if f['filterType'] == 'LOT_SIZE'),
                    {}
                )
                min_qty  = float(lot_size_filter.get('minQty', 0))
                step_qty = float(lot_size_filter.get('stepSize', 0))

                for i, entry in enumerate(normalized_entries):
                    qty = entry['quantity']

                    # Validar cantidad mínima
                    if min_qty and qty < min_qty:
                        raise ValueError(
                            f"entry[{i}]: quantity={qty} es menor al mínimo permitido "
                            f"({min_qty}) para {symbol}."
                        )

                    # Validar notional mínimo (precio * cantidad)
                    if entry['type'] == 'LIMIT':
                        px = entry['price']
                        notional = px * qty
                        if min_notional and notional < min_notional:
                            raise ValueError(
                                f"entry[{i}]: notional estimado={notional:.4f} USDT es menor "
                                f"al mínimo requerido ({min_notional}) para {symbol}."
                            )

                        # Para BUY (LONG): las entradas deben estar por debajo del mercado
                        # Para SELL (SHORT): las entradas deben estar por encima del mercado
                        if side == 'BUY' and px >= market_price:
                            self.logger.warning(
                                f"⚠️  entry[{i}] price={px} ≥ market={market_price}. "
                                f"Binance puede rechazarla como orden de mercado o POST_ONLY. "
                                f"Considera usar type='MARKET' para esta entrada."
                            )
                        elif side == 'SELL' and px <= market_price:
                            self.logger.warning(
                                f"⚠️  entry[{i}] price={px} ≤ market={market_price}. "
                                f"Binance puede rechazarla. "
                                f"Considera usar type='MARKET' para esta entrada."
                            )

            # ── 4) Construir la lista de sub-órdenes ──────────────────────────────
            pos_side = self._get_correct_position_side(side, position_side)
            orders   = []

            for i, entry in enumerate(normalized_entries):
                order: Dict = {
                    'symbol':       symbol,
                    'side':         side,
                    'type':         entry['type'],
                    'quantity':     entry['quantity'],
                    'positionSide': pos_side,
                }

                if entry['type'] == 'LIMIT':
                    order['price']       = entry['price']
                    order['timeInForce'] = entry.get('time_in_force', time_in_force)

                orders.append(order)
                self.logger.debug(
                    f"[pyramiding_batch] Entrada {i+1}/{len(normalized_entries)}: "
                    f"{side} {entry['quantity']} {symbol}"
                    + (f" @ {entry.get('price')}" if entry['type'] == 'LIMIT' else " MARKET")
                )

            # ── 5) Enviar como batch y capturar resultado ─────────────────────────
            self.logger.info(
                f"🔼 Enviando pyramiding batch: {len(orders)} entradas "
                f"{side} {symbol} | positionSide={pos_side}"
            )

            try:
                batch_results = self.place_batch_orders(orders, symbol=symbol)
            except Exception as batch_exc:
                self.logger.error(
                    f"❌ place_batch_orders lanzó excepción: {batch_exc}. "
                    f"Intentando fallback orden-por-orden..."
                )
                batch_results = None

            # ── 6) Fallback individual → si sigue fallando, Algo Order ────────────
            #
            #  Niveles de escalada por entrada:
            #    A) Intento normal   → create_limit_order / create_market_order
            #    B) Último recurso   → create_algo_order  (igual que bracket_batch)
            #
            fallback_results = []

            if batch_results is None:
                # ── Caso A: Batch completo fallido ────────────────────────────────
                self.logger.warning(
                    "⚠️  Batch fallido por completo. "
                    "Reintentando entradas individualmente (nivel 1 → normal, nivel 2 → algo)..."
                )
                for i, entry in enumerate(normalized_entries):
                    label = (
                        f"{side} {entry['quantity']}"
                        + (f" @ {entry.get('price')}" if entry['type'] == 'LIMIT' else " MARKET")
                    )
                    self.logger.info(
                        f"   [fallback] Entrada {i+1}/{len(normalized_entries)}: {label}"
                    )

                    # — Nivel 1: método normal ————————————————————————————————————
                    r = None
                    try:
                        if entry['type'] == 'MARKET':
                            r = self.create_market_order(
                                symbol, side, entry['quantity'], position_side=pos_side
                            )
                        else:
                            r = self.create_limit_order(
                                symbol, side, entry['quantity'], entry['price'],
                                position_side=pos_side,
                                time_in_force=entry.get('time_in_force', time_in_force)
                            )
                    except Exception as lvl1_exc:
                        self.logger.warning(
                            f"   ⚠️  Nivel 1 falló para entrada {i} ({lvl1_exc}). "
                            f"Escalando a Algo Order..."
                        )

                    if r and r.get('orderId'):
                        self.logger.info(
                            f"   ✅ Nivel 1 OK entrada {i}: orderId={r['orderId']}"
                        )
                        fallback_results.append(r)
                        continue

                    # — Nivel 2: create_algo_order ————————————————————————————————
                    self.logger.warning(
                        f"   ⚠️  Nivel 1 sin éxito para entrada {i}. "
                        f"Intentando vía Algo Order..."
                    )
                    algo_r = _algo_fallback(entry, i)
                    fallback_results.append(
                        algo_r if algo_r else {'entry_index': i, 'result': 'ALL_FAILED'}
                    )

                return fallback_results if fallback_results else None

            # ── Caso B: Batch parcialmente exitoso ────────────────────────────────
            if isinstance(batch_results, list):
                for i, r in enumerate(batch_results):
                    if isinstance(r, dict) and 'code' in r:
                        # Sub-orden rechazada por Binance dentro del batch
                        error_code = r.get('code')
                        error_msg  = r.get('msg', '')
                        self.logger.warning(
                            f"⚠️  Entrada batch {i} rechazada "
                            f"(code={error_code}: {error_msg}). "
                            f"Reintentando..."
                        )
                        entry = normalized_entries[i]

                        # — Nivel 1: método normal ————————————————————————————————
                        retry_r = None
                        try:
                            if entry['type'] == 'MARKET':
                                retry_r = self.create_market_order(
                                    symbol, side, entry['quantity'], position_side=pos_side
                                )
                            else:
                                retry_r = self.create_limit_order(
                                    symbol, side, entry['quantity'], entry['price'],
                                    position_side=pos_side,
                                    time_in_force=entry.get('time_in_force', time_in_force)
                                )
                        except Exception as lvl1_exc:
                            self.logger.warning(
                                f"   ⚠️  Nivel 1 falló para entrada {i} ({lvl1_exc}). "
                                f"Escalando a Algo Order..."
                            )

                        if retry_r and retry_r.get('orderId'):
                            self.logger.info(
                                f"   ✅ Nivel 1 OK entrada {i}: orderId={retry_r['orderId']}"
                            )
                            batch_results[i] = retry_r
                            continue

                        # — Nivel 2: create_algo_order ────────────────────────────
                        self.logger.warning(
                            f"   ⚠️  Nivel 1 sin éxito para entrada {i}. "
                            f"Intentando vía Algo Order..."
                        )
                        algo_r = _algo_fallback(entry, i)
                        batch_results[i] = (
                            algo_r if algo_r else {'entry_index': i, 'result': 'ALL_FAILED'}
                        )

                    elif isinstance(r, dict) and 'orderId' in r:
                        self.logger.info(
                            f"✅ Entrada {i+1} aceptada en batch: orderId={r['orderId']} "
                            f"| status={r.get('status')} | price={r.get('price')}"
                        )

            return batch_results

        # ============ /PYRAMIDING BATCH ============
        
        def cancel_all_orders_all_symbols(self) -> Optional[Dict]:
            """
            Cancela TODAS las órdenes abiertas de TODOS los símbolos en Binance Futures.
            
            Returns:
                Dict con resumen de cancelaciones.
            """
            try:
                self.logger.info("Iniciando cancelación de todas las órdenes abiertas (todos los símbolos)...")

                # Obtener todas las órdenes abiertas (de todos los símbolos)
                open_orders = self.client.futures_get_open_orders()
                if not open_orders:
                    self.logger.info("No hay órdenes abiertas en ningún símbolo.")
                    return {"total_orders_found": 0, "total_cancelled": 0}

                cancelled_orders = []
                total_orders = len(open_orders)

                for order in open_orders:
                    symbol = order['symbol']
                    order_id = order['orderId']
                    try:
                        cancel_result = self.client.futures_cancel_order(
                            symbol=symbol,
                            orderId=order_id,
                            timestamp=int(time.time() * 1000)
                        )
                        cancelled_orders.append({
                            "symbol": symbol,
                            "orderId": order_id,
                            "type": order["type"],
                            "side": order["side"],
                            "result": "CANCELLED"
                        })
                        self.logger.info(f"Orden cancelada: {symbol} | ID {order_id}")
                    except Exception as e:
                        cancelled_orders.append({
                            "symbol": symbol,
                            "orderId": order_id,
                            "result": "ERROR",
                            "error": str(e)
                        })
                        self.logger.warning(f"Error cancelando orden {order_id} en {symbol}: {e}")

                successful = len([o for o in cancelled_orders if o["result"] == "CANCELLED"])

                summary = {
                    "total_orders_found": total_orders,
                    "total_cancelled": successful,
                    "details": cancelled_orders
                }

                self.logger.info(f"Cancelación global completada: {successful}/{total_orders} órdenes canceladas.")
                return summary

            except Exception as e:
                self.logger.error(f"Error cancelando todas las órdenes globales: {e}")
                return None

        # def create_algo_order(self, symbol: str, side: str, order_type: str, 
        #               quantity: float = None, price: float = None, 
        #               trigger_price: float = None, position_side: str = None,
        #               close_position: bool = False, time_in_force: str = 'GTC',
        #               reduce_only: bool = False) -> Optional[Dict]:
        #     """
        #     Crea una orden condicional usando el nuevo endpoint /fapi/v1/algoOrder
            
        #     Args:
        #         symbol: Par de trading (ej. BTCUSDT)
        #         side: BUY o SELL
        #         order_type: STOP_MARKET, TAKE_PROFIT_MARKET, STOP, TAKE_PROFIT, TRAILING_STOP_MARKET
        #         quantity: Cantidad (no enviar si close_position=True)
        #         price: Precio límite (solo para STOP y TAKE_PROFIT)
        #         trigger_price: Precio de activación (stopPrice/triggerPrice)
        #         position_side: BOTH, LONG o SHORT
        #         close_position: True para cerrar toda la posición
        #         time_in_force: GTC, IOC, FOK, GTX
        #         reduce_only: True para reduce-only
        #     """
        #     try:
        #         # Determinar positionSide correcto
        #         pos_side = self._get_correct_position_side(side, position_side)
                
        #         # Parámetros base
        #         params = {
        #             'algoType': 'CONDITIONAL',
        #             'symbol': symbol,
        #             'side': side,
        #             'positionSide': pos_side,
        #             'type': order_type,
        #             'timeInForce': time_in_force,
        #             'timestamp': int(time.time() * 1000)
        #         }
                
        #         # Cantidad (solo si no es closePosition)
        #         if not close_position and quantity is not None:
        #             params['quantity'] = self._round_quantity(symbol, quantity)
                
        #         # Precio límite (solo para STOP y TAKE_PROFIT)
        #         if price is not None and order_type in ['STOP', 'TAKE_PROFIT']:
        #             params['price'] = self._round_price(symbol, price)
                
        #         # Precio de activación
        #         if trigger_price is not None:
        #             params['triggerPrice'] = self._round_price(symbol, trigger_price)
                
        #         # Close position
        #         if close_position:
        #             params['closePosition'] = 'true'
                
        #         # Reduce only
        #         if reduce_only and not close_position:
        #             params['reduceOnly'] = 'true'

        #         # Si es una orden LIMIT, eliminar closePosition si existe
        #         if order_type in ['STOP', 'TAKE_PROFIT']:
        #             params['quantity'] = self._round_quantity(symbol, quantity)
        #             params.pop('closePosition', None)
        #             if 'quantity' not in params:
        #                 self.logger.error("Error: STOP/TAKE_PROFIT (Limit) requieren quantity")
                
        #         # Llamar al endpoint de Algo Orders
        #         response = self.client._request_futures_api('post', 'algoOrder', signed=True, data=params)
                
        #         self.logger.info(f"✅ Orden Algo creada: {order_type} {symbol} @ {trigger_price}")
        #         return response
                
        #     except Exception as e:
        #         self.logger.error(f"❌ Error creando orden Algo: {e}")
        #         return None


        # ---------- HELPERS (añádelos a tu clase) ----------
        def _fetch_exchange_info(self, force: bool = False) -> dict:
            """Obtiene y cachea exchangeInfo (symbols, filtros)."""
            now = int(time.time())
            if not hasattr(self, "_exchange_info_cache") or force:
                try:
                    info = self.client._request_futures_api('get', 'exchangeInfo', signed=False, data={})
                    # guardar timestamp simple y la info
                    self._exchange_info_cache = {"ts": now, "info": info}
                except Exception as e:
                    self.logger.warning(f"_fetch_exchange_info fallo: {e}")
                    # devolver cache vieja si existe
                    return getattr(self, "_exchange_info_cache", {}).get("info", {})
            return self._exchange_info_cache.get("info", {})

        def _get_symbol_tick_size(self, symbol: str) -> Decimal:
            """
            Devuelve tickSize como Decimal (p. ej. Decimal('0.0001')).
            Si no encuentra nada, devuelve un tick razonable basado en precio.
            """
            info = self._fetch_exchange_info()
            try:
                for s in info.get('symbols', []) or []:
                    if s.get('symbol') == symbol:
                        for f in s.get('filters', []) or []:
                            # PRICE_FILTER contiene 'tickSize'
                            if f.get('filterType') == 'PRICE_FILTER':
                                ts = f.get('tickSize')
                                if ts:
                                    return Decimal(str(ts))
                        # si no hay PRICE_FILTER, intentar otros campos (fallback)
                        break
            except Exception as e:
                self.logger.debug(f"_get_symbol_tick_size parse error: {e}")

            # fallback: intenta estimar tick según precio actual
            try:
                t = self.client._request_futures_api('get', 'ticker/price', signed=False, data={'symbol': symbol})
                price = Decimal(str(t.get('price') or t.get('lastPrice') or 1))
            except Exception:
                price = Decimal('1')
            # estimación conservadora: 1e-6 * price (ajusta si lo deseas)
            est = (abs(price) * Decimal('0.000001')).quantize(Decimal('1e-8'))  # pequeño
            if est == 0:
                est = Decimal('1e-8')
            return est.normalize()

        def _decimal_quant(self, tick: Decimal) -> Decimal:
            """
            Convierte tickDecimal a exponente de quantize. 
            Ej: tick=Decimal('0.001') -> returns Decimal('0.001')
            """
            # tick ya es algo como Decimal('0.00100000') -> normalizar
            try:
                return tick.normalize()
            except InvalidOperation:
                return tick

        def _round_prices(self, symbol: str, price: float | str | Decimal, mode: str = 'nearest') -> str:
            """
            Redondea respetando tickSize del symbol.
            mode: 'nearest' (ROUND_HALF_UP), 'up' (ROUND_CEILING), 'down' (ROUND_FLOOR)
            Devuelve string (para incluir en params directamente).
            """
            tick = self._get_symbol_tick_size(symbol)
            q = self._decimal_quant(tick)

            # escoger la estrategia de rounding
            if mode == 'up':
                rounding = ROUND_CEILING
            elif mode == 'down':
                rounding = ROUND_FLOOR
            else:
                rounding = ROUND_HALF_UP

            # convertir price a Decimal
            price_d = Decimal(str(price))
            try:
                rounded = price_d.quantize(q, rounding=rounding)
            except InvalidOperation:
                # si quantize falla (ej tick es raro), fallback a formato con 8 decimales
                rounded = price_d.quantize(Decimal('1e-8'), rounding=rounding)

            # devolver como string sin notación exponencial
            s = format(rounded, 'f')
            return s

        # ---------- FIN HELPERS ----------


        # ---------- VERSIÓN CORREGIDA DE create_algo_order ----------
        def create_algo_order(self, symbol: str, side: str, order_type: str,
                            quantity: float = None, price: float = None,
                            trigger_price: float = None, position_side: str = None,
                            close_position: bool = False, time_in_force: str = 'GTC',
                            reduce_only: bool = False) -> Optional[dict]:
            """
            create algo order usando helpers de tick/rounding.
            """
            try:
                pos_side = self._get_correct_position_side(side, position_side)
                side_u = side.upper()
                order_type_u = (order_type or '').upper()

                params = {
                    'algoType': 'CONDITIONAL',
                    'symbol': symbol,
                    'side': side_u,
                    'positionSide': pos_side,
                    'type': order_type_u,
                    'timeInForce': time_in_force,
                    'timestamp': int(time.time() * 1000)
                }

                # quantity (no enviar si close_position=True)
                if not close_position and quantity is not None:
                    params['quantity'] = self._round_quantity(symbol, quantity)

                # price (solo para STOP y TAKE_PROFIT (limit-like))
                if price is not None and order_type_u in ['STOP', 'TAKE_PROFIT']:
                    params['price'] = self._round_prices(symbol, price, mode='nearest')

                # closePosition / reduceOnly
                # ✅ DESPUÉS - closePosition solo para *_MARKET; limit-like usan reduceOnly + quantity
                MARKET_TYPES = {'STOP_MARKET', 'TAKE_PROFIT_MARKET'}
                LIMIT_TYPES  = {'STOP', 'TAKE_PROFIT'}

                if close_position:
                    if order_type_u in MARKET_TYPES:
                        params['closePosition'] = 'true'
                        # closePosition=true implica cerrar posición completa → no enviar quantity
                        params.pop('quantity', None)
                    elif order_type_u in LIMIT_TYPES:
                        # Binance no acepta closePosition en órdenes limit-like.
                        # Se debe pasar quantity + reduceOnly en su lugar.
                        if quantity is None:
                            self.logger.error(
                                f"❌ {order_type_u} con close_position=True requiere 'quantity' "
                                f"(Binance no soporta closePosition en órdenes limit)."
                            )
                            return None
                        params['quantity']   = self._round_quantity(symbol, quantity)
                        params['reduceOnly'] = 'true'
                    else:
                        self.logger.error(f"❌ Tipo de orden desconocido para close_position: {order_type_u}")
                        return None
                elif reduce_only:
                    params['reduceOnly'] = 'true'

                # determine if necesitamos triggerPrice
                needs_trigger = order_type_u in ['STOP_MARKET', 'STOP', 'TAKE_PROFIT_MARKET', 'TAKE_PROFIT']

                # obtener precio actual y tick (para calcular trigger por defecto si falta)
                current_price = None
                tick = None
                if needs_trigger:
                    # obtener market reference price (markPrice preferible)
                    try:
                        tick_resp = self.client._request_futures_api('get', 'premiumIndex', signed=False, data={'symbol': symbol})
                        current_price = Decimal(str(tick_resp.get('markPrice') or tick_resp.get('lastPrice') or tick_resp.get('price')))
                    except Exception:
                        try:
                            t = self.client._request_futures_api('get', 'ticker/price', signed=False, data={'symbol': symbol})
                            current_price = Decimal(str(t.get('price') or 0))
                        except Exception:
                            current_price = Decimal('0')

                    # obtener tickSize
                    tick = self._get_symbol_tick_size(symbol)

                # si no se pasó trigger_price pero se requiere, calcular uno seguro
                if needs_trigger and trigger_price is None:
                    # sugerir trigger un tick por encima/abajo del precio según side
                    if side_u == 'BUY':
                        suggested = (current_price + tick)
                        trigger_calc = self._round_prices(symbol, suggested, mode='up')
                    else:  # SELL
                        suggested = (current_price - tick)
                        trigger_calc = self._round_prices(symbol, suggested, mode='down')

                    params['triggerPrice'] = trigger_calc
                elif trigger_price is not None:
                    # usar trigger dado, pero redondear a la precisión del símbolo
                    # y para BUY usar up, SELL usar down (evita triggers que inmediatamente disparen)
                    mode = 'up' if side_u == 'BUY' else 'down'
                    params['triggerPrice'] = self._round_prices(symbol, trigger_price, mode=mode)

                # STOP/TAKE_PROFIT (limit-like) requieren quantity
                # ✅ DESPUÉS - ya se habrá seteado quantity arriba si close_position=True + limit-like
                if order_type_u in LIMIT_TYPES and 'quantity' not in params:
                    self.logger.error(
                        f"❌ {order_type_u} requiere 'quantity' (o usa {order_type_u.replace('_', '_MARKET') if '_' in order_type_u else order_type_u + '_MARKET'} con closePosition)."
                    )
                    return None

                # seguridad: añadir orderType también
                params['orderType'] = params['type']

                # DEBUG: registrar params antes de enviar (comenta si no quieres logs)
                self.logger.debug(f"[create_algo_order] params antes post: {params}")

                # enviar orden; si -2021 ajustar trigger 1 tick y reintentar una vez
                # antes de enviar la request, dentro del loop de reintento
                max_retries = 5
                try_count = 0
                while True:
                    
                    
                        try:                           
                            
                            # ✅ LIMPIAR campos inyectados por el cliente en iteraciones anteriores
                            params.pop('signature', None)
                            params.pop('recvWindow', None)  # se restablece abajo con setdefault

                            # Actualizar timestamp ANTES de cada intento
                            params['timestamp'] = int(time.time() * 1000)
                            params.setdefault('recvWindow', 99999)
                            response = self.client._request_futures_api('post', 'algoOrder', signed=True, data=params)
                            self.logger.info(f"✅ Orden Algo creada: {order_type_u} {symbol} @ trigger={params.get('triggerPrice')}")
                            return response

                        except Exception as e:
                            err_msg = str(e)
                            try_count += 1

                            # Manejo -2021 (reajustar trigger y reintentar)
                            if try_count <= max_retries and ('Order would immediately trigger' in err_msg or '-2021' in err_msg or '-1022' in err_msg):
                                self.logger.warning(f"-2021 detectado. Ajustando trigger 1 tick y reintentando. err: {err_msg}")
                                
                                if order_type["type"] == "STOP_MARKET":

                                    # recalcular tick si no lo tenemos
                                    if tick is None:
                                        tick = self._get_symbol_tick_size(symbol)

                                    # tomar trigger actual y moverlo 1 tick en la dirección segura
                                    try:
                                        cur_trigger = Decimal(str(params.get('triggerPrice')))
                                        ticker = api.get_ticker_price(symbol)
                                        current_price = float(ticker['price'])
                                        cur_trigger =  Decimal(str(current_price))
                                    except Exception:
                                        cur_trigger = current_price

                                    if side_u == 'BUY':
                                        new_trigger = cur_trigger +tick
                                        params['triggerPrice'] = self._round_prices(symbol, new_trigger, mode='up')
                                        params['timestamp'] = int(time.time() * 1000)
                                        params.setdefault('recvWindow', 99999)  # opcional, evita rejections por latencia
                                    else:
                                        new_trigger = cur_trigger -tick
                                        params['triggerPrice'] = self._round_prices(symbol, new_trigger, mode='down')
                                        params['timestamp'] = int(time.time() * 1000)
                                        params.setdefault('recvWindow', 99999)  # opcional, evita rejections por latencia

                                    self.logger.debug(f"Reintentando con trigger {params['triggerPrice']}")
                                    # IMPORTANT: antes de reintentar el loop actualiza timestamp (lo hará al inicio del loop)
                                    continue

                            # Manejo -1022 (firma inválida) y otros errores no recuperables
                            if 'Signature for this request is not valid' in err_msg or '-1022' in err_msg:
                                self.logger.error("❌ Error -1022: Signature invalid. Verifica API key/secret y sincronización horaria (NTP).")
                                self.logger.debug(f"Params usados (para debug de firma): {params}")
                                return None

                            # si no es recuperable o reintentos agotados, loguear y devolver None
                            self.logger.error(f"❌ Error creando orden Algo: {err_msg}")
                            return None

                    
            
            except Exception as e:
                self.logger.error(f"❌ Error interno creando orden Algo: {e}")
                return None
        # ---------- FIN create_algo_order ----------


        def cancel_all_algo_orders(self, symbol: str) -> Optional[Dict]:
                    """
                    Cancela TODAS las Algo Orders activas de un símbolo.

                    Usa el endpoint DELETE /fapi/v1/algoOpenOrders de Binance Futures.
                    """
                    try:
                        self.logger.info(f"[cancel_all_algo_orders] Cancelando Algo Orders para {symbol}...")

                        # ── Intentar cancelación masiva (endpoint correcto: algoOpenOrders) ────────
                        try:
                            params = {
                                'symbol':    symbol,
                                'timestamp': int(time.time() * 1000),
                            }
                            
                            # 1. Intentamos usar la función nativa de la librería si está disponible
                            if hasattr(self.client, 'futures_cancel_all_algo_open_orders'):
                                resp = self.client.futures_cancel_all_algo_open_orders(**params)
                            else:
                                # 2. Fallback manual con el endpoint correcto
                                resp = self.client._request_futures_api(
                                    'delete', 'algoOpenOrders', signed=True, data=params
                                )
                                
                            self.logger.info(
                                f"[cancel_all_algo_orders] ✅ Cancelación masiva completada para {symbol}: {resp}"
                            )
                            return {
                                'symbol':  symbol,
                                'method':  'bulk',
                                'result':  resp,
                            }
                        except Exception as bulk_err:
                            self.logger.warning(
                                f"[cancel_all_algo_orders] Cancelación masiva no disponible o falló "
                                f"({bulk_err}). Intentando cancelación individual..."
                            )

                        # ── Fallback: listar y cancelar una a una ────────────────────────
                        params_list = {
                            'symbol':    symbol,
                            'timestamp': int(time.time() * 1000),
                        }
                        
                        # Para consultar las abiertas, el endpoint correcto sí es openAlgoOrders
                        open_algo = self.client._request_futures_api(
                            'get', 'openAlgoOrders', signed=True, data=params_list
                        )

                        # La respuesta puede ser una lista directa o venir en una clave 'orders'
                        if isinstance(open_algo, dict):
                            orders_list = open_algo.get('orders', [])
                        elif isinstance(open_algo, list):
                            orders_list = open_algo
                        else:
                            orders_list = []

                        # Filtrar por símbolo por si la API devuelve todos los símbolos
                        orders_list = [
                            o for o in orders_list
                            if str(o.get('symbol', '')).upper() == symbol.upper()
                        ]

                        if not orders_list:
                            self.logger.info(
                                f"[cancel_all_algo_orders] No hay Algo Orders abiertas para {symbol}"
                            )
                            return {
                                'symbol':                symbol,
                                'total_algo_found':      0,
                                'total_cancelled':       0,
                                'details':               [],
                            }

                        cancelled = []
                        for order in orders_list:
                            algo_id = order.get('algoId') or order.get('orderId')
                            try:
                                cancel_params = {
                                    'algoId':    algo_id,
                                    'timestamp': int(time.time() * 1000),
                                }
                                # El borrado individual es con algoOrder
                                cancel_resp = self.client._request_futures_api(
                                    'delete', 'algoOrder', signed=True, data=cancel_params
                                )
                                cancelled.append({
                                    'algoId': algo_id,
                                    'type':   order.get('type'),
                                    'result': 'CANCELLED',
                                })
                                self.logger.info(
                                    f"[cancel_all_algo_orders] ✅ Algo Order cancelada: "
                                    f"algoId={algo_id} | type={order.get('type')}"
                                )
                            except Exception as e:
                                cancelled.append({
                                    'algoId': algo_id,
                                    'result': 'ERROR',
                                    'error':  str(e),
                                })
                                self.logger.warning(
                                    f"[cancel_all_algo_orders] ❌ Error cancelando algoId={algo_id}: {e}"
                                )

                        successful = len([o for o in cancelled if o['result'] == 'CANCELLED'])
                        summary = {
                            'symbol':           symbol,
                            'total_algo_found': len(orders_list),
                            'total_cancelled':  successful,
                            'details':          cancelled,
                        }
                        self.logger.info(
                            f"[cancel_all_algo_orders] Completado: {successful}/{len(orders_list)} "
                            f"Algo Orders canceladas para {symbol}"
                        )
                        return summary

                    except Exception as e:
                        self.logger.error(f"[cancel_all_algo_orders] Error fatal: {e}")
                        return None

# ============ /BATCH ORDERS ==================================




        # ======================== EJEMPLO DE USO COMPLETO ========================

        """

        api = BinanceAPI(API_KEY, API_SECRET, testnet=False)

# 1) Entrada MARKET + TP + SL en 1 request (BUY/LONG)
api.bracket_batch(
    symbol="BTCUSDT",
    side="BUY",
    quantity=0.005,
    entry_type="MARKET",
    take_profit=72000.0,
    stop_loss=68000.0
)

# 2) Entrada LIMIT + TP + SL en 1 request (SELL/SHORT)
api.bracket_batch(
    symbol="ETHUSDT",
    side="SELL",
    quantity=0.2,
    entry_type="LIMIT",
    entry_price=3550.0,
    take_profit=3300.0,
    stop_loss=3650.0
)






        # Para usar estas funciones, agrégalas a tu clase BinanceAPI y úsalas así:

        api = BinanceAPI(API_KEY, API_SECRET, testnet=False)

        # 1. ABRIR POSICIONES LIMIT
        long_order = api.limit_open_long("BTCUSDT", 0.01, 45000.0, leverage=50)
        short_order = api.limit_open_short("ETHUSDT", 0.1, 3500.0, leverage=50)

        # Guardar los IDs de las órdenes
        long_order_id = long_order['orderId'] if long_order else None
        short_order_id = short_order['orderId'] if short_order else None

        # 2. CERRAR POSICIONES LIMIT (cuando tengas posiciones abiertas)
        # Opción A: Especificar todo manualmente
        exit_long = api.limit_exit_long("BTCUSDT", 0.01, 46000.0)

        # Opción B: Cerrar toda la posición automáticamente
        exit_short = api.limit_exit_short("ETHUSDT")  # Detecta cantidad y precio automáticamente

        # Opción C: Solo especificar precio, detectar cantidad
        exit_long2 = api.limit_exit_long("BTCUSDT", limit_price=47000.0)

        # 3. CANCELAR ÓRDENES ESPECÍFICAS
        # Cancelar una orden específica por ID
        api.cancel_limit_long("BTCUSDT", long_order_id)
        api.cancel_limit_short("ETHUSDT", short_order_id)

        # 4. CANCELAR TODAS LAS ÓRDENES DE UN TIPO
        # Cancelar todas las órdenes LONG del símbolo
        api.cancel_limit_long("BTCUSDT")

        # Cancelar todas las órdenes SHORT del símbolo  
        api.cancel_limit_short("ETHUSDT")

        # 5. CANCELAR TODO
        # Cancelar todas las órdenes LIMIT (LONG y SHORT) del símbolo
        api.cancel_all_limit_orders("BTCUSDT")

        # EJEMPLO DE FLUJO COMPLETO:
        # Abrir → Esperar → Cancelar si no se ejecuta
        long_order = api.limit_open_long("BTCUSDT", 0.01, 45000.0, leverage=50)
        if long_order:
            order_id = long_order['orderId']
            
            # Esperar 5 minutos
            time.sleep(300)
            
            # Verificar si se ejecutó, si no, cancelar
            order_status = api.check_order_status("BTCUSDT", order_id)
            if order_status and order_status['status'] not in ['FILLED', 'PARTIALLY_FILLED']:
                print("Orden no ejecutada, cancelando...")
                api.cancel_limit_long("BTCUSDT", order_id)
        """


# ======================== EJEMPLO DE USO ========================

# if __name__ == "__main__":
#     # Configuración
#     API_KEY = "bfTHsxAHNKQsSpW9u9SmIUBM1EnDQX8eAvXUyxlsTQDKEpndcnxkaY9PIpceD9o2"
#     API_SECRET = "Su9OdJzTR8x0nXlC1xUNU9AXtGxu4jtVUod8bZmbGtY3iKdHKUW78YkJMbw4UqAQ"
    
#     # Inicializar API (usar testnet=True para pruebas)
#     api = BinanceAPI(API_KEY, API_SECRET, testnet=False)
    
#     symbol = "BTCUSDT"
    
#     try:
#         # Configurar apalancamiento
#         api.set_leverage(symbol, 125)
        
#         # Configurar margen cruzado
#         api.set_margin_type(symbol, "CROSSED")
        
#         # Obtener precio actual
#         ticker = api.get_ticker_price(symbol)
#         current_price = float(ticker['price'])
#         print(f"Precio actual de {symbol}: ${current_price}")
        
#         # Abrir posición larga
#         quantity = 0.01
#         order = api.open_long_position(symbol, quantity)
#         if order:
#             print(f"Posición larga abierta: {order}")
            
#             # Establecer stop loss 2% por debajo
#             stop_price = current_price * 0.98
#             sl_order = api.set_stop_loss(symbol, stop_price, "LONG")
#             print(f"Stop loss establecido: {sl_order}")
            
#             # Establecer take profit 3% por encima
#             tp_price = current_price * 1.03
#             tp_order = api.set_take_profit(symbol, tp_price, "LONG")
#             print(f"Take profit establecido: {tp_order}")
        
#         # Verificar posiciones
#         positions = api.get_position_info(symbol)
#         print(f"Información de posición: {positions}")
        
#         # Verificar órdenes abiertas
#         open_orders = api.get_open_orders(symbol)
#         print(f"Órdenes abiertas: {open_orders}")
        
#     except Exception as e:
#         print(f"Error en el ejemplo: {e}")


if __name__ == "__main__":
    # 🚨 Usa claves nuevas de Testnet para probar esto
    API_KEY = "j65vqKTAEvJtOZMCQbSiH5GZXfzyg1W70dWvhnb5DHxMOlLaW1JlrohJtYf8hJMH"
    API_SECRET = "qBqVSu0b0stLoN5hWEo5TAeK0IyfI4bNP1kQh7X3JoXVlzBOVutMSr0CWtvTua0O"
    
    # Inicializar API (¡USAR TESTNET=True SIEMPRE PARA PRUEBAS!)
    api = BinanceAPI(API_KEY, API_SECRET, testnet=False)
    
    symbol = "BTCUSDT"
    
    try:
        print(f"--- INICIANDO PRUEBA DE ALGO ORDERS PARA {symbol} ---")
        
        # 1. Obtener precio actual (solo como referencia)
        ticker = api.get_ticker_price(symbol)
        current_price = float(ticker['price'])
        print(f"Precio actual de {symbol}: ${current_price}")
        
        # 2. CREAR una Algo Order NORMAL (STOPMARKET: activa compra si precio baja)
        quantity = 0.01
        trigger_price = current_price 
        print(f"\nCreando Algo Order NORMAL (STOPMARKET) para comprar {quantity} {symbol} si precio <= ${trigger_price:.2f}...")
        
        # CORREGIDO: Parámetros exactos de create_algo_order (sin TWAP, con position_side)
        algo_order = api.create_algo_order(
            symbol=symbol,
            side='BUY',
            order_type='STOP_MARKET',  # Tipo normal soportado
            quantity=quantity,
            trigger_price=trigger_price,  # stopPrice
            position_side='LONG',  # ¡GUIÓN BAJO!
            time_in_force='GTC'
        )
        print(f"✅ Algo Order creada exitosamente:\n  ID: {algo_order.get('algoId')} | Tipo: {algo_order.get('type')}")
        
        # Esperamos un par de segundos
        time.sleep(2)
        
        # 3. VERIFICAR abiertas (usa método directo si no hay wrapper)
        print("\nVerificando Algo Orders activas...")
        open_algo_orders = api.client.futures_get_open_algo_orders(symbol=symbol)
        orders_list = open_algo_orders if isinstance(open_algo_orders, list) else open_algo_orders.get('orders', [])
        print(f"Órdenes activas encontradas: {len(orders_list)}")
        for o in orders_list:
            print(f"  - ID: {o.get('algoId')} | Qty: {o.get('origQty')} | Trigger: {o.get('stopPrice')}")
        
        # 4. CANCELAR TODAS
        if len(orders_list) > 0:
            print(f"\nCancelando todas con cancel_all_algo_orders...")
            cancel_result = api.close_all_positions(symbol)
            print(f"Resultado: {cancel_result}")
            time.sleep(2)
            
            # 5. VERIFICAR FINAL
            check_orders = api.client.futures_get_open_algo_orders(symbol=symbol)
            final_list = check_orders if isinstance(check_orders, list) else check_orders.get('orders', [])
            if len(final_list) == 0:
                print("🎉 ¡ÉXITO TOTAL! No quedan órdenes.")
            else:
                print(f"⚠️ Quedan {len(final_list)} órdenes.")
        else:
            print("No hay órdenes para cancelar.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
