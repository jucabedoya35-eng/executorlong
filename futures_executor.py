"""
futures_executor_ws.py — Executor de Futuros Binance 100% WebSocket para trading

- Órdenes de apertura/cierre por Binance USDⓈ-M Futures WebSocket API.
- Consultas de balance y posiciones por WebSocket API.
- Precios de mercado desde SymbolWebSocketPriceCache (ws.py / WS.py).
"""

import asyncio
import aiohttp
from aiohttp import web
import logging
import math
import re
from datetime import datetime, timezone
import os
import json
import uuid
import hmac
import hashlib
from dataclasses import dataclass
from typing import Optional

# ══════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
USE_TESTNET        = os.environ.get("USE_TESTNET", "false").lower() == "true"
SIGNAL_SECRET      = os.environ.get("SIGNAL_SECRET", "cambiar-por-secreto-seguro")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

LEVERAGE        = int(os.environ.get("LEVERAGE", "4"))
HEDGE_MODE      = os.environ.get("HEDGE_MODE", "false").lower() == "true"


def set_hedge_mode_runtime(value: bool) -> None:
    """Cambia el flag HEDGE_MODE del proceso en caliente (sin reiniciar).

    Todo el código que usa HEDGE_MODE lo lee como variable global en el
    momento de cada llamada (no queda "congelado" en ningún closure), así
    que reasignarlo aquí es suficiente para que open_trade/close_trade,
    el cálculo de positionSide, etc. usen el nuevo modo de inmediato.
    """
    global HEDGE_MODE
    HEDGE_MODE = bool(value)
PORT            = int(os.environ.get("PORT", "10000"))
POSITION_POLL_S = int(os.environ.get("POSITION_POLL_S", "30"))

MIN_NOTIONAL_USDT = float(os.environ.get("MIN_NOTIONAL_USDT", "5.1"))
NOTIONAL_SAFETY_BUFFER_PCT = float(os.environ.get("NOTIONAL_SAFETY_BUFFER_PCT", "2.0"))
# Edad máxima (segundos) que se acepta para un precio cacheado del WS antes
# de considerarlo "no confiable" y forzar espera de un tick fresco / REST.
# Evita el bug de reabrir un símbolo y heredar un precio viejo guardado.
MAX_PRICE_AGE_S = float(os.environ.get("MAX_PRICE_AGE_S", "5.0"))
MIN_VALID_PRICE = 0.00001

WS_API_URL = os.environ.get(
    "BINANCE_WS_FAPI_URL",
    "wss://testnet.binancefuture.com/ws-fapi/v1" if USE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1",
)

REST_FAPI_URL = os.environ.get(
    "BINANCE_REST_FAPI_URL",
    "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Executor")


# ══════════════════════════════════════════════════════════
#  AJUSTE DE CANTIDAD / NOTIONAL MÍNIMO
# ══════════════════════════════════════════════════════════
def _step_decimals(step: float) -> int:
    """Cantidad de decimales implicada por un stepSize (p.ej. 0.001 -> 3)."""
    if step <= 0:
        return 8
    s = f"{step:.10f}".rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def floor_to_step(value: float, step: float) -> float:
    """Redondea `value` hacia abajo al múltiplo de `step` más cercano,
    evitando los errores típicos de coma flotante (0.1 + 0.2, etc.)."""
    if step <= 0:
        return value
    decimals = _step_decimals(step)
    units = math.floor(round(value / step, 8))
    return round(units * step, decimals)


def ceil_to_step(value: float, step: float) -> float:
    """Redondea `value` hacia arriba al múltiplo de `step` más cercano."""
    if step <= 0:
        return value
    decimals = _step_decimals(step)
    units = math.ceil(round(value / step, 8))
    return round(units * step, decimals)

def clamp_price(value: float, minimum: float = MIN_VALID_PRICE) -> float:
    """Asegura que un precio nunca quede por debajo del mínimo válido."""
    try:
        price = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(price):
        return 0.0
    return max(minimum, price)


def format_qty(value: float, step: float) -> str:
    """Formatea la cantidad con la cantidad de decimales del stepSize,
    sin notación científica ni decimales innecesarios. Para cantidades
    enteras usa round() en vez de int() truncado, para no perder una
    unidad por ruido de coma flotante (p.ej. 26.999999999 -> 26)."""
    decimals = _step_decimals(step)
    return f"{value:.{decimals}f}" if decimals > 0 else str(int(round(value)))


def resolve_safe_quantity(
    desired_notional: float,
    price: float,
    filters: dict,
    extra_buffer_pct: float = 0.0,
) -> tuple[float, float]:
    """
    Calcula la cantidad final a enviar a Binance a partir de un notional
    (tamaño de orden en USDT) objetivo y el precio de referencia, en vez
    de confiar ciegamente en la `quantity` que llega en la señal.

    - Convierte notional -> quantity con el precio más fresco disponible.
    - Redondea al stepSize (LOT_SIZE) del símbolo para evitar -1111/-1013.
    - Si el notional resultante queda por debajo del mínimo exigido
      (MIN_NOTIONAL_USDT, con colchón opcional), sube la cantidad al
      siguiente múltiplo de stepSize que sí lo cumpla.

    Devuelve (quantity, notional_final).
    """
    if price <= 0:
        raise ValueError("price debe ser > 0 para calcular la cantidad")

    # Default seguro: si no sabemos el stepSize real, asumimos cantidad
    # entera (stepSize=1) en vez de 0.001. Un entero SIEMPRE es múltiplo
    # válido de cualquier stepSize más fino (0.1, 0.01, 0.001...), así que
    # es el fallback universalmente seguro — al revés (asumir decimales en
    # un símbolo que en realidad exige enteros) es lo que dispara -1111.
    step = float(filters.get("stepSize", 1.0)) or 1.0
    min_qty = float(filters.get("minQty", step))
    min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
    min_notional *= (1 + extra_buffer_pct / 100.0)

    raw_qty = desired_notional / price
    qty = ceil_to_step(raw_qty, step)
    if qty < min_qty:
        qty = min_qty

    notional = qty * price
    if notional < min_notional:
        needed_qty = min_notional / price
        qty = ceil_to_step(needed_qty, step)
        if qty < min_qty:
            qty = min_qty
        notional = qty * price

    return qty, notional


# ══════════════════════════════════════════════════════════
#  MODELO DE TRADE
# ══════════════════════════════════════════════════════════
@dataclass
class Trade:
    id: int
    symbol: str
    direction: str  # LONG | SHORT
    entry_price: float
    quantity: float
    open_time: str
    leverage: int
    paper_trade_id: int = 0
    entry_order_id: str = ""
    current_price: float = 0.0
    status: str = "OPEN"  # OPEN | TP | SL | CLOSED | MANUAL | CLOSE_ALL
    close_price: float = 0.0
    close_time: str = ""
    pnl_usdt: float = 0.0
    roi_pct: float = 0.0
    order_assumed: bool = False

    @property
    def notional_usdt(self) -> float:
        return self.entry_price * self.quantity

    def update_unrealized(self, price: float):
        self.current_price = price
        if self.direction == "LONG":
            self.pnl_usdt = (price - self.entry_price) * self.quantity
        else:
            self.pnl_usdt = (self.entry_price - price) * self.quantity
        self.roi_pct = (self.pnl_usdt / self.notional_usdt * 100) if self.notional_usdt else 0.0


# ══════════════════════════════════════════════════════════
#  BINANCE WS API
# ══════════════════════════════════════════════════════════
class BinanceAPI:
    """Cliente mínimo para Binance Futures WebSocket API."""

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, ws_url: str = WS_API_URL):
        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")

        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.testnet = testnet
        self.ws_url = ws_url

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}
        self._closed = False

        # Cache de filtros por símbolo (stepSize/minQty/minNotional) leídos
        # de /fapi/v1/exchangeInfo. Se usa para calcular la cantidad real a
        # enviar a partir del tamaño de orden deseado en USDT. Se carga
        # de UNA SOLA VEZ para todos los símbolos (no uno por símbolo) y
        # se refresca cada EXCHANGE_INFO_TTL_S, para minimizar peso REST.
        self._symbol_filters_cache: dict[str, dict] = {}
        self._symbol_filters_lock = asyncio.Lock()
        self._exchange_info_loaded_at: float = 0.0

        # Cache de leverage aplicado por símbolo, para no repetir la
        # llamada REST de set_leverage si el valor no cambió (velocidad).
        self._leverage_cache: dict[str, int] = {}
        self._leverage_lock = asyncio.Lock()

        # Freno de bloqueo de IP (Binance -1003 / HTTP 418): si Binance ya
        # nos banea por exceso de requests, dejamos de pegarle a REST hasta
        # que pase el tiempo indicado en el propio mensaje de error, en vez
        # de seguir reintentando y empeorar/alargar el bloqueo.
        self._rest_ban_until_ms: float = 0.0

    @staticmethod
    def _payload_string(params: dict) -> str:
        return "&".join(
            f"{k}={params[k]}"
            for k in sorted(params.keys())
            if k != "signature"
        )

    def _sign(self, params: dict) -> str:
        payload = self._payload_string(params)
        return hmac.new(self.api_secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _is_rest_banned(self) -> bool:
        return self._rest_ban_until_ms > datetime.now(timezone.utc).timestamp() * 1000

    def _rest_ban_remaining_s(self) -> float:
        return max(0.0, self._rest_ban_until_ms / 1000 - datetime.now(timezone.utc).timestamp())

    def _note_possible_ip_ban(self, response_text: str):
        """
        Si la respuesta de Binance indica -1003 (demasiadas requests, IP
        baneada), guarda el timestamp hasta el que dura el bloqueo para
        que las próximas llamadas REST se omitan en vez de seguir
        golpeando la API y alargar/empeorar el bloqueo.
        """
        if "-1003" not in response_text:
            return
        match = re.search(r"banned until (\d+)", response_text)
        if not match:
            return
        until_ms = float(match.group(1))
        if until_ms > self._rest_ban_until_ms:
            self._rest_ban_until_ms = until_ms
            until_dt = datetime.fromtimestamp(until_ms / 1000, tz=timezone.utc)
            log.error(
                f"⛔ IP bloqueada por Binance (rate limit -1003) hasta {until_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} "
                f"(~{self._rest_ban_remaining_s():.0f}s) — se omitirán llamadas REST hasta entonces"
            )

    def _check_rest_ban_or_raise(self):
        if self._is_rest_banned():
            raise RuntimeError(f"REST omitida: IP bloqueada por Binance (-1003), quedan ~{self._rest_ban_remaining_s():.0f}s")

    def _ws_alive(self) -> bool:
        return bool(
            self._ws and not self._ws.closed
            and self._reader_task and not self._reader_task.done()
        )

    async def _ensure_http_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session

    async def connect(self):
        if self._ws_alive():
            return

        async with self._connect_lock:
            if self._ws_alive():
                return

            # Limpiar conexión muerta (el reader pudo haber terminado sin
            # cerrar el socket explícitamente — quedaba "zombie")
            if self._ws is not None:
                try:
                    if not self._ws.closed:
                        await self._ws.close()
                except Exception:
                    pass
                self._ws = None
            if self._reader_task is not None and not self._reader_task.done():
                self._reader_task.cancel()

            await self._ensure_http_session()

            log.info(f"Conectando Binance WS API → {self.ws_url}")
            self._ws = await self._session.ws_connect(
                self.ws_url,
                autoping=True,
                heartbeat=30,
                max_msg_size=0,
            )
            self._reader_task = asyncio.create_task(self._reader())

    async def close(self):
        self._closed = True
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()

    async def _reader(self):
        assert self._ws is not None
        while not self._closed:
            try:
                msg = await self._ws.receive()
            except Exception as e:
                log.error(f"WS reader error: {e}")
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    log.warning(f"WS no JSON: {msg.data!r}")
                    continue

                req_id = str(data.get("id")) if data.get("id") is not None else None
                fut = self._pending.pop(req_id, None) if req_id is not None else None
                if fut is not None and not fut.done():
                    fut.set_result(data)
                else:
                    log.debug(f"WS event no mapeado: {data}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

        err = ConnectionError("WebSocket API desconectado")
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(err)
        self._pending.clear()

    async def _request(self, method: str, params: Optional[dict] = None, signed: bool = False, timeout: float = 20.0, _retry: bool = True) -> dict:
        await self.connect()

        params = dict(params or {})
        if signed:
            params.setdefault("apiKey", self.api_key)
            params.setdefault("timestamp", int(datetime.now(timezone.utc).timestamp() * 1000))
            params.setdefault("recvWindow", 5000)
            params["signature"] = self._sign(params)

        req_id = str(uuid.uuid4())
        payload = {"id": req_id, "method": method}
        if params:
            payload["params"] = params

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending[req_id] = fut

        try:
            assert self._ws is not None
            await self._ws.send_json(payload)
        except Exception as e:
            self._pending.pop(req_id, None)
            if _retry:
                log.warning(f"_request: fallo enviando ({e!r}); reconectando y reintentando una vez")
                return await self._request(method, params, signed=False, timeout=timeout, _retry=False)
            raise

        try:
            response = await asyncio.wait_for(fut, timeout=timeout)
        except Exception as e:
            self._pending.pop(req_id, None)
            if _retry and not self._ws_alive():
                log.warning(f"_request: sin respuesta ({e!r}); conexión muerta, reconectando y reintentando una vez")
                return await self._request(method, params, signed=False, timeout=timeout, _retry=False)
            raise

        if response.get("status") != 200:
            err = response.get("error") or {}
            raise RuntimeError(f"Binance WS error {response.get('status')}: {err}")
        return response.get("result", response)

    async def account_balance(self) -> list[dict]:
        result = await self._request("account.balance", signed=True)
        return result if isinstance(result, list) else []

    async def position_information(self, symbol: Optional[str] = None) -> list[dict]:
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = await self._request("account.position", params=params, signed=True)
        return result if isinstance(result, list) else []

    async def set_leverage(self, symbol: str, leverage: int, force: bool = False) -> dict:
        """
        Cambia el leverage inicial de un símbolo. Esto es exclusivamente
        REST en Binance (POST /fapi/v1/leverage) — la WS API no expone
        ningún método equivalente.
        """
        leverage = int(leverage)
        if not force and self._leverage_cache.get(symbol) == leverage:
            return {"symbol": symbol, "leverage": leverage, "cached": True}

        async with self._leverage_lock:
            if not force and self._leverage_cache.get(symbol) == leverage:
                return {"symbol": symbol, "leverage": leverage, "cached": True}

            self._check_rest_ban_or_raise()

            session = await self._ensure_http_session()
            params = {
                "symbol": symbol,
                "leverage": leverage,
                "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "recvWindow": 5000,
            }
            query = self._payload_string(params)
            signature = self._sign(params)
            url = f"{REST_FAPI_URL}/fapi/v1/leverage?{query}&signature={signature}"
            headers = {"X-MBX-APIKEY": self.api_key}

            async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    self._note_possible_ip_ban(text)
                    raise RuntimeError(f"REST set_leverage error {resp.status}: {text}")
                try:
                    data = json.loads(text)
                except Exception:
                    data = {"raw": text}
                log.info(f"Leverage REST OK: {symbol} → {data.get('leverage', leverage)}x")
                self._leverage_cache[symbol] = leverage
                return data

    # ── Escalera de leverage de respaldo ───────────────────────────────
    # Algunos símbolos rechazan el leverage configurado por defecto
    # (cada símbolo tiene su propio límite máximo según el notional, y
    # no podemos conocerlos todos de antemano, menos aún con limitaciones
    # de IP que impiden golpear /leverageBracket por cada símbolo). En
    # vez de abortar la apertura, se prueba esta escalera 10x→4x hasta
    # que Binance acepte uno.
    LEVERAGE_FALLBACK_LADDER = [10, 9, 8, 7, 6, 5, 4]

    async def set_leverage_with_fallback(self, symbol: str, preferred: int) -> int:
        """
        Intenta aplicar `preferred`; si Binance lo rechaza (p.ej. -4028
        "Leverage X is not valid", típico en símbolos con límite propio
        más bajo), recorre LEVERAGE_FALLBACK_LADDER (excluyendo el ya
        intentado) hasta encontrar uno aceptado. Devuelve el leverage
        que finalmente quedó aplicado en el símbolo.
        """
        ladder = [preferred] + [lv for lv in self.LEVERAGE_FALLBACK_LADDER if lv != preferred]
        last_err: Optional[Exception] = None
        for lv in ladder:
            try:
                await self.set_leverage(symbol, lv)
                if lv != preferred:
                    log.warning(f"set_leverage_with_fallback: {symbol} rechazó {preferred}x — aplicado {lv}x en su lugar")
                return lv
            except Exception as e:
                last_err = e
                log.warning(f"set_leverage_with_fallback: {symbol} rechazó {lv}x ({e}); probando siguiente de la escalera")
        log.error(f"set_leverage_with_fallback: {symbol} rechazó TODA la escalera de leverage ({ladder}): {last_err}")
        return preferred

    # ── REST firmado genérico (para endpoints sin equivalente en la WS API) ──
    async def _rest_signed(self, http_method: str, path: str, params: dict, timeout: float = 10.0) -> dict:
        self._check_rest_ban_or_raise()
        session = await self._ensure_http_session()
        params = dict(params or {})
        params.setdefault("timestamp", int(datetime.now(timezone.utc).timestamp() * 1000))
        params.setdefault("recvWindow", 5000)
        query = self._payload_string(params)
        signature = self._sign(params)
        url = f"{REST_FAPI_URL}{path}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        async with session.request(http_method, url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            if resp.status != 200:
                self._note_possible_ip_ban(text)
                raise RuntimeError(f"REST {http_method} {path} error {resp.status}: {text}")
            try:
                return json.loads(text)
            except Exception:
                return {"raw": text}

    async def get_rest_price(self, symbol: str) -> float:
        """Último recurso para obtener precio cuando el WS no tiene el dato
        a tiempo: GET /fapi/v1/ticker/price (público, no firmado)."""
        self._check_rest_ban_or_raise()
        session = await self._ensure_http_session()
        url = f"{REST_FAPI_URL}/fapi/v1/ticker/price?symbol={symbol}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            text = await resp.text()
            if resp.status != 200:
                self._note_possible_ip_ban(text)
                raise RuntimeError(f"REST ticker/price error {resp.status}: {text}")
            data = json.loads(text)
            return float(data.get("price", 0))

    async def get_open_orders(self, symbol: Optional[str] = None) -> list[dict]:
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = await self._rest_signed("GET", "/fapi/v1/openOrders", params)
        return result if isinstance(result, list) else []

    async def cancel_order(self, symbol: str, order_id) -> dict:
        return await self._rest_signed("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})

    async def cancel_all_open_orders(self, symbol: str) -> dict:
        return await self._rest_signed("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    async def create_tp_sl_order(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        order_type: str,  # "STOP_MARKET" (SL) o "TAKE_PROFIT_MARKET" (TP)
        position_side: Optional[str] = None,
        close_position: bool = True,
        quantity: Optional[float] = None,
        time_in_force: str = "GTC",
    ) -> dict:
        """
        TP/SL de Binance Futures. -4120 confirma que la WS API
        (order.place) NO acepta STOP_MARKET/TAKE_PROFIT_MARKET con
        closePosition — exige el endpoint dedicado de Algo Order
        (POST /fapi/v1/algoOrder), así que se envían por ahí.

        FIX -4509 "Time in Force (TIF) GTE can only be used with open
        positions": closePosition=true usa internamente el TIF especial
        GTE_GTC, y Binance SOLO lo acepta si en el instante exacto de la
        request ya existe una posición (o una orden) registrada en su
        lado para ese símbolo. Si el TP/SL se manda justo después de la
        entrada (o sin posición todavía), aparece esa carrera y la
        request se rechaza aunque la posición exista un instante después.

        Para no depender de ese timing, en cuanto se conoce la cantidad
        de la posición NUNCA se usa closePosition=true: se manda
        STOP_MARKET/TAKE_PROFIT_MARKET con `quantity` + `reduceOnly`
        (o solo `quantity` + `positionSide` en Hedge Mode, donde
        reduceOnly no está permitido). Esa combinación no depende de que
        ya exista posición — Binance simplemente deja la orden
        condicional en NEW hasta que haya algo que reducir cuando se
        dispare el trigger. closePosition=true queda solo como fallback
        para cuando no se tiene la cantidad exacta a mano.
        """
        trigger_price = clamp_price(trigger_price)
        if trigger_price <= 0:
            raise ValueError("trigger_price inválido")

        use_close_position = close_position and quantity is None

        reduce_only = None
        if not use_close_position and not HEDGE_MODE:
            # En Hedge Mode, reduceOnly no se puede enviar (Binance lo
            # rechaza): side + positionSide ya implica reducción. En
            # One-way Mode sí hace falta para no abrir posición nueva.
            reduce_only = "true"

        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            triggerPrice=str(trigger_price),
            positionSide=position_side or "BOTH",
            closePosition="true" if use_close_position else None,
            quantity=None if use_close_position else (str(quantity) if quantity is not None else None),
            reduceOnly=reduce_only,
            timeInForce=time_in_force,
            workingType="MARK_PRICE",
        )

    async def create_limit_order(
        self,
        symbol: str,
        side: str,
        quantity,
        price: float,
        position_side: Optional[str] = None,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
    ) -> dict:
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": time_in_force,
            "newOrderRespType": "RESULT",
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        result = await self._request("order.place", params=params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def create_algo_order(self, symbol: str, side: str, order_type: str, **extra) -> dict:
        """
        Algo Order condicional (POST /fapi/v1/algoOrder) — endpoint
        obligatorio para STOP_MARKET/TAKE_PROFIT_MARKET con
        closePosition (la WS API los rechaza con -4120). `extra` admite
        cualquier param adicional (triggerPrice, positionSide,
        closePosition, quantity, reduceOnly, timeInForce, price,
        workingType...); las claves con valor None se omiten.
        """
        params = {"symbol": symbol, "side": side, "algoType": "CONDITIONAL", "type": order_type}
        for k, v in extra.items():
            if v is not None:
                params[k] = v
        return await self._rest_signed("POST", "/fapi/v1/algoOrder", params)

    async def get_open_algo_orders(self, symbol: Optional[str] = None) -> list[dict]:
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = await self._rest_signed("GET", "/fapi/v1/algoOpenOrders", params)
        if isinstance(result, dict):
            return result.get("orders") or result.get("algoOrders") or []
        return result if isinstance(result, list) else []

    async def cancel_algo_order(self, algo_id) -> dict:
        return await self._rest_signed("DELETE", "/fapi/v1/algoOrder", {"algoId": algo_id})

    async def cancel_all_algo_orders(self, symbol: str) -> dict:
        return await self._rest_signed("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol})

    async def cancel_symbol_orders(self, symbol: str) -> dict:
        """
        Cancela todas las órdenes vivas del símbolo: normales y algo orders.
        Se usa al cerrar posiciones para evitar que queden órdenes huérfanas.
        """
        result: dict = {"symbol": symbol}
        normal_err = None
        algo_err = None

        try:
            result["normal"] = await self.cancel_all_open_orders(symbol)
        except Exception as e:
            normal_err = e
            result["normal_error"] = str(e)

        try:
            result["algo"] = await self.cancel_all_algo_orders(symbol)
        except Exception as e:
            algo_err = e
            result["algo_error"] = str(e)

        if normal_err and algo_err:
            raise RuntimeError(
                f"No se pudieron cancelar órdenes normales ni algo orders para {symbol}: {normal_err}; {algo_err}"
            )

        return result

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict:
        """margin_type: 'ISOLATED' o 'CROSSED'."""
        return await self._rest_signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})

    async def get_position_mode(self) -> bool:
        """Consulta el modo actual de la CUENTA en Binance (no el de este
        proceso). True = Hedge Mode (dualSidePosition), False = One-way."""
        result = await self._rest_signed("GET", "/fapi/v1/positionSide/dual", {})
        return bool(result.get("dualSidePosition"))

    async def set_position_mode(self, hedge: bool) -> dict:
        """Cambia el modo de posición de la CUENTA en Binance.

        IMPORTANTE: Binance rechaza este cambio (-4059 / -4068) si hay
        posiciones abiertas u órdenes activas en la cuenta — hay que
        cerrar todo primero. Esto es una restricción de Binance, no de
        este bot.
        """
        return await self._rest_signed(
            "POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "true" if hedge else "false"}
        )

    async def modify_position_margin(self, symbol: str, amount: float, position_side: str = "BOTH", add: bool = True) -> dict:
        """type=1 añade margen, type=2 lo retira (solo válido en ISOLATED)."""
        params = {
            "symbol": symbol,
            "amount": str(abs(amount)),
            "type": 1 if add else 2,
            "positionSide": position_side,
        }
        return await self._rest_signed("POST", "/fapi/v1/positionMargin", params)

    EXCHANGE_INFO_TTL_S = 6 * 3600  # los filtros de lote casi nunca cambian; refrescar cada 6h basta

    async def _load_all_symbol_filters(self, force_refresh: bool = False) -> None:
        """
        Carga TODOS los símbolos de /fapi/v1/exchangeInfo en UNA sola
        llamada REST y cachea sus filtros (en vez de una llamada por
        símbolo por señal, que es lo que terminó disparando el bloqueo de
        IP -1003 de Binance). El cache se reutiliza durante
        EXCHANGE_INFO_TTL_S, ya que estos filtros prácticamente no
        cambian día a día.
        """
        now = datetime.now(timezone.utc).timestamp()
        if not force_refresh and self._symbol_filters_cache and (now - self._exchange_info_loaded_at) < self.EXCHANGE_INFO_TTL_S:
            return

        async with self._symbol_filters_lock:
            now = datetime.now(timezone.utc).timestamp()
            if not force_refresh and self._symbol_filters_cache and (now - self._exchange_info_loaded_at) < self.EXCHANGE_INFO_TTL_S:
                return

            self._check_rest_ban_or_raise()

            session = await self._ensure_http_session()
            url = f"{REST_FAPI_URL}/fapi/v1/exchangeInfo"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    self._note_possible_ip_ban(text)
                    raise RuntimeError(f"REST exchangeInfo error {resp.status}: {text}")
                data = json.loads(text)

            new_cache: dict[str, dict] = {}
            for s in data.get("symbols", []):
                sym = s.get("symbol")
                if not sym:
                    continue

                qty_precision = int(s.get("quantityPrecision", 0))
                entry = {
                    "stepSize": round(10 ** (-qty_precision), 10),
                    "minQty": round(10 ** (-qty_precision), 10),
                    "qty_precision": qty_precision,
                    "min_notional": MIN_NOTIONAL_USDT,
                }

                lot_size_filter = None
                market_lot_size_filter = None
                for f in s.get("filters", []):
                    ftype = f.get("filterType")
                    if ftype == "LOT_SIZE":
                        lot_size_filter = f
                    elif ftype == "MARKET_LOT_SIZE":
                        market_lot_size_filter = f
                    elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
                        notional = f.get("notional") or f.get("minNotional")
                        if notional is not None:
                            entry["min_notional"] = float(notional)

                # Las órdenes que envía este executor son siempre MARKET,
                # así que MARKET_LOT_SIZE es el filtro que Binance
                # realmente valida; LOT_SIZE queda como respaldo.
                chosen = market_lot_size_filter or lot_size_filter
                if chosen:
                    entry["stepSize"] = float(chosen.get("stepSize", entry["stepSize"]))
                    entry["minQty"] = float(chosen.get("minQty", entry["minQty"]))

                # Piso de seguridad: nunca operar por debajo de MIN_NOTIONAL_USDT
                # aunque exchangeInfo reporte un valor menor.
                entry["min_notional"] = max(entry["min_notional"], MIN_NOTIONAL_USDT)
                new_cache[sym] = entry

            self._symbol_filters_cache = new_cache
            self._exchange_info_loaded_at = now
            log.info(f"exchangeInfo cargado de una sola vez: {len(new_cache)} símbolos cacheados")

    async def get_symbol_filters(self, symbol: str, force_refresh: bool = False) -> dict:
        """
        Devuelve los filtros de cantidad/notional del símbolo, usando el
        cache global poblado por _load_all_symbol_filters (una sola
        llamada REST para todos los símbolos en vez de una por símbolo).

        Si la carga falla (red caída, IP bloqueada por -1003, etc.) o el
        símbolo no aparece en exchangeInfo, se devuelve un default SEGURO
        de cantidad entera (stepSize=1): un entero siempre es múltiplo
        válido de cualquier stepSize más fino, así que es la opción que
        menos rechazos provoca cuando no tenemos el dato real — al revés
        (asumir decimales) es lo que dispara -1111.
        """
        safe_default = {"stepSize": 1.0, "minQty": 1.0, "qty_precision": 0, "min_notional": MIN_NOTIONAL_USDT}

        try:
            await self._load_all_symbol_filters(force_refresh=force_refresh)
        except Exception as e:
            log.warning(f"get_symbol_filters: no se pudo cargar exchangeInfo ({e}); usando default seguro (entero) para {symbol}")
            return safe_default

        cached = self._symbol_filters_cache.get(symbol)
        if cached:
            return cached

        log.error(f"get_symbol_filters: símbolo {symbol} no encontrado en exchangeInfo cacheado — usando default seguro (entero)")
        return safe_default

    async def create_market_order(
        self,
        symbol: str,
        side: str,
        quantity,
        position_side: Optional[str] = None,
        reduce_only: bool = False,
        new_order_resp_type: str = "RESULT",
    ) -> dict:
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            # Binance WS API exige los DECIMAL (price, quantity, etc.) como
            # strings, no como floats — enviar float puede introducir
            # ruido de precisión (p.ej. 0.1 + 0.2) que dispara -1111/-1013.
            "quantity": str(quantity),
            "newOrderRespType": new_order_resp_type,
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        result = await self._request("order.place", params=params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def close_position_market(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        position_side: Optional[str] = None,
    ) -> dict:
        direction = direction.upper()
        close_side = "SELL" if direction == "LONG" else "BUY"
        return await self.create_market_order(
            symbol=symbol,
            side=close_side,
            quantity=quantity,
            position_side=position_side,
            reduce_only=True,
            new_order_resp_type="RESULT",
        )

    async def close_all_positions(self, symbol: Optional[str] = None) -> list[dict]:
        positions = await self.position_information(symbol=symbol)
        closed = []
        for p in positions:
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if abs(amt) <= 0:
                continue

            sym = p.get("symbol", symbol or "")
            pos_side = p.get("positionSide") or None

            if pos_side in ("LONG", "SHORT"):
                close_side = "SELL" if pos_side == "LONG" else "BUY"
                qty = abs(amt)
                result = await self.create_market_order(
                    symbol=sym,
                    side=close_side,
                    quantity=qty,
                    position_side=pos_side,
                    reduce_only=True,
                    new_order_resp_type="RESULT",
                )
            else:
                close_side = "SELL" if amt > 0 else "BUY"
                qty = abs(amt)
                result = await self.create_market_order(
                    symbol=sym,
                    side=close_side,
                    quantity=qty,
                    position_side="BOTH",
                    reduce_only=True,
                    new_order_resp_type="RESULT",
                )
            closed.append(result)
        return closed


# ══════════════════════════════════════════════════════════
#  GESTOR DE EJECUCIÓN
# ══════════════════════════════════════════════════════════
class ExecutionManager:
    """Ejecuta y rastrea posiciones reales en Binance Futures vía WS."""

    def __init__(self, binance_api, price_ws):
        self.api = binance_api
        self.price_ws = price_ws
        # CLAVE: (symbol, direction) y NO solo symbol. Con HEDGE_MODE=true
        # Binance mantiene posiciones LONG y SHORT independientes para el
        # mismo símbolo (positionSide=LONG / SHORT). Si el diccionario
        # local sólo usaba `symbol` como clave, una posición SHORT podía
        # pisar/mezclarse con una LONG abierta del mismo símbolo (o
        # viceversa), aunque en Binance fueran dos posiciones separadas.
        # Con la clave compuesta, cada dirección vive en su propia entrada
        # y se abre/cierra/promedia de forma independiente.
        self._trades: dict[tuple[str, str], Trade] = {}
        self._closed: list[Trade] = []
        self._counter: int = 0
        self._lock = asyncio.Lock()
        self._balance: float = 0.0
        self._paper_id_map: dict[int, tuple[str, str]] = {}
        self.trading_enabled: bool = True

    async def refresh_balance(self):
        try:
            balances = await self.api.account_balance()
            for b in balances:
                if b.get("asset") == "USDT":
                    self._balance = float(b.get("availableBalance", b.get("balance", 0)))
                    return
        except Exception as e:
            log.error(f"refresh_balance: {e}")

    @property
    def balance(self) -> float:
        return self._balance

    @property
    def open_trades(self) -> list[Trade]:
        return list(self._trades.values())

    @property
    def closed_trades(self) -> list[Trade]:
        return list(self._closed)

    @property
    def open_longs(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "LONG"]

    @property
    def open_shorts(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "SHORT"]

    @property
    def active_symbols(self) -> set:
        return {sym for (sym, _direction) in self._trades.keys()}

    def trades_for_symbol(self, symbol: str) -> list[Trade]:
        """Todas las posiciones abiertas (LONG y/o SHORT) para un símbolo."""
        symbol = symbol.upper()
        return [t for (sym, _d), t in self._trades.items() if sym == symbol]

    def get_trade(self, symbol: str, direction: Optional[str] = None) -> Optional[Trade]:
        """Busca una posición por símbolo (+ dirección opcional).

        Pensado para los endpoints HTTP existentes que sólo mandan
        `symbol` (compatibilidad hacia atrás): si no se especifica
        `direction` y hay una sola posición abierta para ese símbolo, la
        devuelve sin ambigüedad. Si hay DOS (LONG y SHORT simultáneas en
        Hedge Mode) y no se especificó dirección, no se puede adivinar
        cuál quiere el llamador — devuelve None para forzar a que el
        cliente especifique `direction` en vez de operar a ciegas sobre
        la posición equivocada.
        """
        symbol = symbol.upper()
        if direction:
            return self._trades.get((symbol, direction.upper()))
        matches = self.trades_for_symbol(symbol)
        if len(matches) == 1:
            return matches[0]
        return None

    @property
    def total_realized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self._closed)

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.open_trades)

    @property
    def equity(self) -> float:
        return self._balance + self.unrealized_pnl

    def _sync_ws_symbols(self):
        try:
            self.price_ws.update_symbols(list(self.active_symbols))
        except Exception as e:
            log.error(f"_sync_ws_symbols: {e}")

    async def get_entry_reference_price(self, symbol: str, extra_symbols: Optional[list[str]] = None) -> float:
        """
        Resuelve el precio REAL de entrada — NUNCA confía en el `price`
        que llega en la señal, que es solo informativo/de cuando se
        generó la señal y puede llevar segundos de desfase.

        Orden de preferencia (siempre WS antes que REST):
        1. Caché WS ya activa para el símbolo — pero SÓLO si es reciente
           (< MAX_PRICE_AGE_S). Un precio cacheado viejo (p.ej. de una
           posición anterior ya cerrada en ese mismo símbolo, cuyo stream
           se desuscribió) es PEOR que no tener nada: produce un
           entry_price completamente fuera de mercado sin ningún error
           visible. Por eso aquí se exige freshness, no solo presencia.
        2. Si el símbolo aún no estaba suscrito (o el dato es viejo), se
           suscribe al WS de precios y se espera a que llegue un tick
           fresco — en vez de descartar la apertura o reusar algo stale.
        3. Si el WS no entrega nada a tiempo (símbolo nuevo, lag de
           suscripción, etc.), se cae a REST (ticker/price) como último
           recurso — pero nunca si REST ya está bajo ban de IP (-1003):
           en ese caso reintentar solo extiende el bloqueo. Si está
           baneado, se prefiere esperar más tiempo al WS.
        """
        try:
            p = self.price_ws.get_price(symbol, max_age_s=MAX_PRICE_AGE_S)
            if p and p > 0:
                return float(p)
        except Exception:
            pass

        try:
            wanted = self.active_symbols | {symbol}
            if extra_symbols:
                wanted |= set(extra_symbols)
            self.price_ws.update_symbols(list(wanted))
        except Exception as e:
            log.warning(f"get_entry_reference_price: no se pudo suscribir {symbol} al WS: {e}")

        rest_banned = False
        try:
            rest_banned = self.api._is_rest_banned()
        except Exception:
            pass

        # Si REST está baneado, le damos al WS mucho más margen (hasta
        # ~15s) antes de rendirnos, en vez de caer a REST y empeorar el
        # bloqueo de IP. Si REST está disponible, el margen normal (~3s)
        # es suficiente porque hay un respaldo razonable después.
        wait_iterations = 75 if rest_banned else 15
        for _ in range(wait_iterations):
            await asyncio.sleep(0.2)
            try:
                p = self.price_ws.get_price(symbol, max_age_s=MAX_PRICE_AGE_S)
                if p and p > 0:
                    return float(p)
            except Exception:
                pass

        if rest_banned:
            log.error(
                f"get_entry_reference_price: {symbol} sin precio WS tras esperar y REST sigue baneado "
                f"(~{self.api._rest_ban_remaining_s():.0f}s restantes) — apertura cancelada sin tocar REST"
            )
            return 0.0

        try:
            p = await self.api.get_rest_price(symbol)
            if p > 0:
                log.info(f"get_entry_reference_price: {symbol} resuelto por REST (el WS no entregó precio a tiempo)")
                return p
        except Exception as e:
            log.error(f"get_entry_reference_price: REST también falló para {symbol}: {e}")

        return 0.0

    async def _place_market_order_safe(
        self,
        symbol: str,
        side: str,
        qty_str: str,
        position_side: Optional[str],
        filters: dict,
        ref_price: float,
        reduce_only: bool = False,
    ) -> dict:
        """
        Envía la orden MARKET con la quantity ya calculada. Si Binance la
        rechaza específicamente por notional insuficiente (-4164) o por
        precisión/stepSize (-1013 / -1111) — típico cuando el precio se
        movió justo entre el cálculo y el envío — se recalcula la
        cantidad con un colchón de seguridad mayor sobre el notional
        mínimo y se reintenta UNA sola vez con un precio fresco.
        """
        try:
            return await self.api.create_market_order(
                symbol=symbol,
                side=side,
                quantity=qty_str,
                position_side=position_side,
                reduce_only=reduce_only,
                new_order_resp_type="RESULT",
            )
        except Exception as e:
            err = str(e)
            is_notional_or_precision = any(code in err for code in ("-4164", "-1013", "-1111", "Notional", "precision"))
            if not is_notional_or_precision:
                raise

            log.warning(f"_place_market_order_safe: {symbol} rechazada ({err}); recalculando con colchón mayor y reintentando una vez")

            try:
                fresh_price = float(self.price_ws.get_price(symbol) or 0.0) or ref_price
            except Exception:
                fresh_price = ref_price

            # Doble colchón de seguridad en el reintento (p.ej. 2% -> ~10%).
            retry_buffer = max(NOTIONAL_SAFETY_BUFFER_PCT * 5, 10.0)
            min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
            target_notional = min_notional * (1 + retry_buffer / 100.0)

            retry_qty, retry_notional = resolve_safe_quantity(target_notional, fresh_price, filters)
            retry_qty_str = format_qty(retry_qty, filters.get("stepSize", 0.001))

            log.info(f"_place_market_order_safe: reintento {symbol} qty={retry_qty_str} (notional≈${retry_notional:.4f}, precio={fresh_price})")

            return await self.api.create_market_order(
                symbol=symbol,
                side=side,
                quantity=retry_qty_str,
                position_side=position_side,
                reduce_only=reduce_only,
                new_order_resp_type="RESULT",
            )

    async def open_trade(
        self,
        symbol: str,
        direction: str,
        price: float,
        quantity: float,
        paper_trade_id: int = 0,
    ) -> Optional[Trade]:
        direction = direction.upper()
        side = "BUY" if direction == "LONG" else "SELL"
        position_side = direction if HEDGE_MODE else "BOTH"

        # Ya NO se bloquea si el símbolo ya tiene una posición abierta: la
        # señal se manda siempre. Si ya existía una posición en ese símbolo,
        # se fusiona con la nueva al registrar el trade (ver más abajo),
        # igual que hace Binance internamente con el neto por símbolo.

        order_assumed = False
        entry_order_id = ""

        # ── Resolución del precio REAL de entrada ──────────────────────
        # El `price` de la señal es solo orientativo (sirve para calcular
        # el notional deseado junto con `quantity`), pero NUNCA se usa
        # como precio de entrada ni se descarta la apertura por su
        # diferencia con el mercado. Siempre se solicita el precio real
        # — WS primero, REST como respaldo — y con ESE se calcula la
        # cantidad final y se registra la entrada.
        ref_price = await self.get_entry_reference_price(symbol)
        if ref_price <= 0:
            log.error(f"open_trade: no se pudo obtener un precio real para {symbol} (ni WS ni REST) — apertura cancelada")
            return None

        if price > 0 and abs(ref_price - price) / max(price, 1e-9) > 0.01:
            log.info(
                f"open_trade: precio de señal={price} es solo orientativo — se abre con precio real={ref_price} para {symbol}"
            )

        desired_notional = price * quantity if price > 0 else ref_price * quantity
        filled_price = ref_price

        # get_symbol_filters (exchangeInfo) y set_leverage (con escalera
        # de respaldo) son independientes entre sí: se disparan en
        # paralelo para no sumar sus latencias en el camino crítico.
        filters_result, leverage_result = await asyncio.gather(
            self.api.get_symbol_filters(symbol),
            self.api.set_leverage_with_fallback(symbol, LEVERAGE),
            return_exceptions=True,
        )

        if isinstance(filters_result, Exception):
            log.warning(f"open_trade: no se pudieron leer filtros de {symbol}, usando defaults ({filters_result})")
            filters = {"stepSize": 1.0, "minQty": 1.0, "min_notional": MIN_NOTIONAL_USDT}
        else:
            filters = filters_result

        if isinstance(leverage_result, Exception):
            log.warning(f"open_trade: no se pudo aplicar NINGÚN leverage de la escalera para {symbol}: {leverage_result}")
            applied_leverage = LEVERAGE
        else:
            applied_leverage = leverage_result

        try:
            send_qty, send_notional = resolve_safe_quantity(
                desired_notional, ref_price, filters, extra_buffer_pct=NOTIONAL_SAFETY_BUFFER_PCT
            )
        except Exception as e:
            log.error(f"open_trade: no se pudo calcular quantity segura para {symbol}: {e}")
            return None

        if abs(send_qty - quantity) > 1e-12:
            log.info(
                f"open_trade: quantity ajustada para {symbol} → señal={quantity} (notional≈${desired_notional:.4f}) "
                f"→ enviada={send_qty} (notional≈${send_notional:.4f}, precio_ref={ref_price})"
            )
        quantity = send_qty

        qty_str = format_qty(quantity, filters.get("stepSize", 0.001))
        log.info(f"open_trade: enviando MARKET por WS → {symbol} {side} qty={qty_str} (notional≈${send_notional:.4f})")
        try:
            result = await self._place_market_order_safe(
                symbol=symbol,
                side=side,
                qty_str=qty_str,
                position_side=position_side,
                filters=filters,
                ref_price=ref_price,
            )
            entry_order_id = str(result.get("orderId", result.get("clientOrderId", "WS_ORDER")))
            avg = result.get("avgPrice") or result.get("price")
            try:
                avg_f = float(avg)
                if avg_f > 0:
                    filled_price = avg_f
            except Exception:
                pass
            # Si la cantidad final se ajustó en el reintento, refleja el valor
            # realmente ejecutado en el trade que se registra.
            try:
                executed_qty = float(result.get("origQty") or result.get("executedQty") or quantity)
                if executed_qty > 0:
                    quantity = executed_qty
            except Exception:
                pass
            log.info(f"MARKET WS OK: {symbol} {side} qty={quantity} id={entry_order_id} avg={filled_price}")
        except Exception as e_ord:
            err = str(e_ord)
            if "-2019" in err or "Margin is insufficient" in err:
                log.warning(f"[ASUMIDA] MARKET WS -2019 para {symbol} — posición registrada como abierta: {e_ord}")
                entry_order_id = "MARGIN_INSUFFICIENT"
                order_assumed = True
            else:
                log.error(f"open_trade: fallo enviando MARKET WS para {symbol}: {e_ord}")
                return None

        async with self._lock:
            key = (symbol, direction)

            if HEDGE_MODE:
                # En Hedge Mode, Binance mantiene LONG y SHORT del mismo
                # símbolo como posiciones TOTALMENTE independientes
                # (positionSide). No hay neteo entre ellas: una señal LONG
                # nunca debe tocar la posición SHORT existente del mismo
                # símbolo, y viceversa. Por eso aquí sólo se busca/actualiza
                # la entrada con la MISMA clave (symbol, direction); jamás
                # se mira la dirección contraria.
                existing = self._trades.get(key)

                if existing is None:
                    self._counter += 1
                    trade = Trade(
                        id=self._counter,
                        symbol=symbol,
                        direction=direction,
                        entry_price=filled_price,
                        quantity=quantity,
                        open_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                        leverage=applied_leverage,
                        paper_trade_id=paper_trade_id,
                        entry_order_id=entry_order_id,
                        current_price=filled_price,
                        order_assumed=order_assumed,
                    )
                    self._trades[key] = trade
                    self._paper_id_map[paper_trade_id] = key
                    action_tag = "ABIERTO"
                else:
                    # Misma dirección ya abierta: amplía y promedia precio.
                    new_qty = existing.quantity + quantity
                    existing.entry_price = (
                        (existing.entry_price * existing.quantity) + (filled_price * quantity)
                    ) / new_qty
                    existing.quantity = new_qty
                    existing.leverage = applied_leverage
                    existing.entry_order_id = entry_order_id
                    existing.order_assumed = existing.order_assumed or order_assumed
                    self._paper_id_map[paper_trade_id] = key
                    trade = existing
                    action_tag = "AMPLIADO"
            else:
                # One-way mode (positionSide=BOTH): Binance mantiene UN
                # único neto por símbolo sin importar qué `side` se mande,
                # así que aquí también debe haber como máximo una entrada
                # local por símbolo (cualquiera sea su dirección actual).
                # Se busca la entrada existente para ESTE símbolo en
                # cualquier dirección — nunca puede haber dos en one-way.
                existing = next((t for (sym, _d), t in self._trades.items() if sym == symbol), None)

                if existing is None:
                    self._counter += 1
                    trade = Trade(
                        id=self._counter,
                        symbol=symbol,
                        direction=direction,
                        entry_price=filled_price,
                        quantity=quantity,
                        open_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                        leverage=applied_leverage,
                        paper_trade_id=paper_trade_id,
                        entry_order_id=entry_order_id,
                        current_price=filled_price,
                        order_assumed=order_assumed,
                    )
                    self._trades[key] = trade
                    self._paper_id_map[paper_trade_id] = key
                    action_tag = "ABIERTO"
                else:
                    old_key = (existing.symbol, existing.direction)
                    # Fusión con signo (+ LONG / - SHORT), igual que el
                    # neto real que mantiene Binance por símbolo.
                    old_signed = existing.quantity if existing.direction == "LONG" else -existing.quantity
                    delta_signed = quantity if direction == "LONG" else -quantity
                    new_signed = old_signed + delta_signed

                    if abs(new_signed) < 1e-9:
                        existing.status = "NETTED"
                        existing.close_price = filled_price
                        existing.close_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                        existing.update_unrealized(filled_price)
                        del self._trades[old_key]
                        self._paper_id_map.pop(existing.paper_trade_id, None)
                        self._closed.append(existing)
                        log.info(f"open_trade: {symbol} neteado a 0 con esta señal — posición cerrada")
                        trade = None
                        action_tag = "NETEADO"
                        try:
                            await self.api.cancel_symbol_orders(symbol)
                        except Exception as e:
                            log.warning(f"open_trade: no se pudieron cancelar órdenes residuales de {symbol} tras neteo a 0: {e}")
                    else:
                        new_direction = "LONG" if new_signed > 0 else "SHORT"
                        new_qty = abs(new_signed)
                        if new_direction == existing.direction:
                            existing.entry_price = (
                                (existing.entry_price * existing.quantity) + (filled_price * quantity)
                            ) / new_qty
                            action_tag = "AMPLIADO"
                        else:
                            existing.entry_price = filled_price
                            action_tag = "INVERTIDO"
                        existing.direction = new_direction
                        existing.quantity = new_qty
                        existing.leverage = applied_leverage
                        existing.entry_order_id = entry_order_id
                        existing.order_assumed = existing.order_assumed or order_assumed

                        new_key = (symbol, new_direction)
                        if new_key != old_key:
                            del self._trades[old_key]
                            self._trades[new_key] = existing
                        self._paper_id_map[paper_trade_id] = new_key
                        trade = existing

        self._sync_ws_symbols()
        await self.refresh_balance()

        if trade is None:
            return None

        assumed_tag = " [ASUMIDA — MARGIN INSUF]" if order_assumed else ""
        log.info(
            f"[REAL #{trade.id}] {action_tag} {trade.direction} {symbol} @ ${filled_price} | "
            f"Qty total: {trade.quantity} | Lev: {applied_leverage}x | OrderId: {entry_order_id} | "
            f"Paper#{paper_trade_id}{assumed_tag}"
        )
        return trade

    async def close_trade(self, trade: Trade, close_price: float, reason: str) -> bool:
        async with self._lock:
            key = (trade.symbol, trade.direction)
            if trade.status != "OPEN":
                return False
            if self._trades.get(key) is not trade:
                return False

            trade.status = reason
            trade.close_price = close_price
            trade.close_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            if trade.direction == "LONG":
                trade.pnl_usdt = (close_price - trade.entry_price) * trade.quantity
            else:
                trade.pnl_usdt = (trade.entry_price - close_price) * trade.quantity

            trade.roi_pct = (trade.pnl_usdt / trade.notional_usdt * 100) if trade.notional_usdt else 0.0

            del self._trades[key]
            self._paper_id_map.pop(trade.paper_trade_id, None)
            self._closed.append(trade)

        try:
            await self.api.cancel_symbol_orders(trade.symbol)
        except Exception as e:
            log.warning(f"close_trade: no se pudieron cancelar órdenes residuales de {trade.symbol}: {e}")

        self._sync_ws_symbols()

        log.info(
            f"[REAL #{trade.id}] CERRADO {reason} {trade.symbol} @ ${close_price} | "
            f"PnL: {trade.pnl_usdt:+.4f} USDT ({trade.roi_pct:+.2f}%)"
        )
        await self.refresh_balance()
        return True

    async def force_close_trade(self, trade: Trade, reason: str = "MAIN_BOT", close_price: Optional[float] = None) -> bool:
        close_price = close_price if close_price and close_price > 0 else (trade.current_price if trade.current_price > 0 else trade.entry_price)
        try:
            await self.api.cancel_symbol_orders(trade.symbol)
        except Exception as e:
            log.warning(f"force_close: no se pudieron cancelar órdenes previas de {trade.symbol}: {e}")
        try:
            await self.api.close_position_market(
                symbol=trade.symbol,
                direction=trade.direction,
                quantity=trade.quantity,
                position_side=(trade.direction if HEDGE_MODE else "BOTH"),
            )
            log.info(f"force_close: close_position_market OK para {trade.symbol}")
        except Exception as e:
            log.warning(f"force_close: error cerrando {trade.symbol}: {e}")
        return await self.close_trade(trade, close_price, reason)

    async def force_close_by_symbol(self, symbol: str) -> bool:
        try:
            await self.api.cancel_symbol_orders(symbol)
        except Exception as e:
            log.warning(f"force_close_by_symbol: no se pudieron cancelar órdenes previas de {symbol}: {e}")
        try:
            closed = await self.api.close_all_positions(symbol=symbol)
            log.info(f"force_close_by_symbol {symbol}: {closed}")
            return bool(closed)
        except Exception as e:
            log.error(f"force_close_by_symbol {symbol}: {e}")
            return False
        finally:
            try:
                await self.api.cancel_symbol_orders(symbol)
            except Exception as e:
                log.warning(f"force_close_by_symbol: no se pudieron cancelar órdenes residuales de {symbol}: {e}")

    async def close_all_global(self, reason: str = "CLOSE_ALL") -> list[Trade]:
        async with self._lock:
            trades_snapshot = list(self._trades.values())

        closed_trades = []
        for trade in trades_snapshot:
            try:
                closed = await self.force_close_trade(trade, reason=reason)
                if closed:
                    closed_trades.append(trade)
                    log.info(f"close_all_global: cerrado {trade.symbol} #{trade.id}")
            except Exception as e:
                log.error(f"close_all_global: error cerrando {trade.symbol}: {e}")

        log.info(f"close_all_global: {len(closed_trades)}/{len(trades_snapshot)} posiciones cerradas")
        return closed_trades

    async def poll_positions(self) -> list[Trade]:
        if not self._trades:
            return []

        try:
            positions = await self.api.position_information()
        except Exception as e:
            log.error(f"poll_positions: {e}")
            return []

        # En Hedge Mode Binance devuelve una entrada por (symbol,
        # positionSide); en one-way mode devuelve positionSide=BOTH. Se
        # indexa por (symbol, direction) para poder comparar 1:1 contra
        # las posiciones locales sin mezclar LONG y SHORT del mismo símbolo.
        pos_by_key: dict[tuple[str, str], dict] = {}
        for p in positions:
            sym = p.get("symbol", "")
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if not sym or abs(amt) <= 0:
                continue
            pos_side = p.get("positionSide", "BOTH")
            if pos_side == "BOTH":
                direction = "LONG" if amt > 0 else "SHORT"
            else:
                direction = pos_side
            pos_by_key[(sym, direction)] = p

        async with self._lock:
            open_copy = dict(self._trades)

        missing: list[Trade] = [
            trade for key, trade in open_copy.items() if key not in pos_by_key
        ]
        return missing

    def find_by_paper_id(self, paper_trade_id: int) -> Optional[Trade]:
        key = self._paper_id_map.get(paper_trade_id)
        if key:
            return self._trades.get(key)
        return None


# ══════════════════════════════════════════════════════════
#  INSTANCIAS GLOBALES
# ══════════════════════════════════════════════════════════
execution_manager: Optional[ExecutionManager] = None

executor_status = {
    "signals_received": 0,
    "signals_open": 0,
    "signals_close": 0,
    "signals_rejected": 0,
    "manual_closes": 0,
    "signals_tp_set": 0,
    "signals_tp_closed": 0,
    "signals_sl_set": 0,
    "signals_sl_closed": 0,
    "last_signal_time": "Esperando señales...",
    "last_signal_detail": "",
    "started_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
}


# ══════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════
async def send_telegram(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                log.error(f"Telegram error {resp.status}: {await resp.text()}")
    except Exception as e:
        log.error(f"Error Telegram: {e}")


def build_open_message(trade: Trade) -> str:
    emoji = "🟢" if trade.direction == "LONG" else "🔴"
    word = "LONG  ▲" if trade.direction == "LONG" else "SHORT ▼"
    base = trade.symbol.replace("USDT", "")
    assum = "\n⚠️ <i>Posición asumida (margin insuf., -2019)</i>" if trade.order_assumed else ""
    return (
        f"{emoji} <b>🏦 POSICIÓN REAL ABIERTA — {word}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>       <code>{trade.symbol}</code>\n"
        f"💰 <b>Entrada:</b>  <code>${trade.entry_price:,.6f}</code>\n"
        f"📦 <b>Cantidad:</b> <code>{trade.quantity} {base}</code>\n"
        f"💹 <b>Notional:</b> <code>{trade.notional_usdt:.4f} USDT</code>\n"
        f"⚡ <b>Leverage:</b> <code>{trade.leverage}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 <b>OrderId:</b>  <code>{trade.entry_order_id}</code>\n"
        f"🆔 Real <b>#{trade.id}</b>  |  Paper <b>#{trade.paper_trade_id}</b>{assum}\n"
        f"⏱ {trade.open_time}\n"
        f"💼 Balance: <code>{execution_manager.balance:.2f} USDT</code>"
    )


def build_close_message(trade: Trade) -> str:
    reason_map = {
        "TP": ("✅", "TAKE PROFIT 🎯"),
        "SL": ("❌", "STOP LOSS 🛑"),
        "CLOSED": ("🔄", "CIERRE EXTERNO"),
        "MAIN_BOT": ("🔄", "CIERRE SEÑAL PRINCIPAL"),
        "CLOSE_ALL": ("🛑", "CIERRE GLOBAL (SEÑAL)"),
        "MANUAL": ("🖐", "CIERRE MANUAL (DASHBOARD)"),
        "NETTED": ("➖", "NETEADA POR SEÑAL OPUESTA"),
    }
    emoji, reason_str = reason_map.get(trade.status, ("⚠️", trade.status))
    dir_str = "🟢 LONG" if trade.direction == "LONG" else "🔴 SHORT"
    pnl_emoji = "💚" if trade.pnl_usdt >= 0 else "❗"

    closed_all = execution_manager.closed_trades
    wins = sum(1 for t in closed_all if t.status == "TP")
    total = len(closed_all)
    wr = f"{wins / total * 100:.1f}% ({wins}✅/{total - wins}❌)" if total else "N/A"

    return (
        f"{emoji} <b>🏦 POSICIÓN REAL CERRADA — {reason_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>      <code>{trade.symbol}</code>  {dir_str}\n"
        f"💵 <b>Entrada:</b> <code>${trade.entry_price:,.6f}</code>\n"
        f"💵 <b>Salida:</b>  <code>${trade.close_price:,.6f}</code>\n"
        f"⚡ <b>Lev:</b>     <code>{trade.leverage}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{pnl_emoji} <b>PnL:</b>   <code>{trade.pnl_usdt:+.4f} USDT</code>\n"
        f"📊 <b>ROI:</b>   <code>{trade.roi_pct:+.2f}%</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Abierto:  {trade.open_time}\n"
        f"⏱ Cerrado:  {trade.close_time}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 <b>Balance:</b>  <code>{execution_manager.balance:.2f} USDT</code>\n"
        f"💼 <b>Equity:</b>   <code>{execution_manager.equity:.2f} USDT</code>\n"
        f"📈 <b>Win Rate:</b> <code>{wr}</code>\n"
        f"🆔 Real <b>#{trade.id}</b>  |  Paper <b>#{trade.paper_trade_id}</b>"
    )


# ══════════════════════════════════════════════════════════
#  MONITOR DE POSICIONES — SOLO ALERTA
# ══════════════════════════════════════════════════════════
async def position_monitor_loop(session: aiohttp.ClientSession):
    log.info(f"Position Monitor (sólo alertas) — poll cada {POSITION_POLL_S}s")
    await asyncio.sleep(10)
    alerted: set[str] = set()

    while True:
        try:
            missing = await execution_manager.poll_positions()
            missing_symbols = {t.symbol for t in missing}

            for trade in missing:
                if trade.symbol in alerted:
                    continue
                alerted.add(trade.symbol)
                log.warning(f"⚠️ {trade.symbol} (#{trade.id}) ya no aparece en Binance pero sigue OPEN localmente.")
                await send_telegram(
                    session,
                    f"⚠️ <b>POSIBLE CIERRE EXTERNO DETECTADO</b>\n"
                    f"📊 <code>{trade.symbol}</code> (Real #{trade.id} | Paper #{trade.paper_trade_id})\n"
                    f"Ya no aparece entre tus posiciones de Binance, pero el executor sigue registrándola como abierta.\n"
                    f"➡️ No se cerró automáticamente.",
                )

            alerted &= (missing_symbols | execution_manager.active_symbols)

        except Exception as e:
            log.error(f"position_monitor_loop: {e}")

        await asyncio.sleep(POSITION_POLL_S)


# ══════════════════════════════════════════════════════════
#  SYNC DE PRECIOS
# ══════════════════════════════════════════════════════════
async def price_sync_loop():
    log.info("Price Sync Loop — actualizando PnL desde caché WS cada 1s")
    while True:
        try:
            for trade in execution_manager.open_trades:
                price = execution_manager.price_ws.get_price(trade.symbol)
                if price:
                    trade.update_unrealized(price)
        except Exception as e:
            log.error(f"price_sync_loop: {e}")
        await asyncio.sleep(1)


# ══════════════════════════════════════════════════════════
#  HTTP SIGNAL HANDLER
# ══════════════════════════════════════════════════════════
async def signal_handler(request: web.Request) -> web.Response:
    secret = request.headers.get("X-Signal-Secret", "")
    if secret != SIGNAL_SECRET:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    action = data.get("action", "").lower()
    symbol = data.get("symbol", "").upper()
    trade_id = int(data.get("trade_id", 0))

    executor_status["signals_received"] += 1
    executor_status["last_signal_time"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    executor_status["last_signal_detail"] = f"{action.upper()} {symbol}"

    if action == "open":
        direction = data.get("direction", "").upper()
        price = float(data.get("price", 0))
        quantity = float(data.get("quantity", 0))

        if not symbol or not direction or price <= 0 or quantity <= 0:
            executor_status["signals_rejected"] += 1
            return web.json_response({"ok": False, "error": "missing or invalid open params"}, status=400)

        if not execution_manager.trading_enabled:
            executor_status["signals_rejected"] += 1
            log.warning(f"Señal OPEN ignorada para {symbol} — trading pausado manualmente desde el dashboard")
            return web.json_response({"ok": False, "error": "trading pausado manualmente desde el dashboard"}, status=200)

        async def _do_open():
            trade = await execution_manager.open_trade(symbol, direction, price, quantity, paper_trade_id=trade_id)
            if trade:
                executor_status["signals_open"] += 1
                async with aiohttp.ClientSession() as sess:
                    await send_telegram(sess, build_open_message(trade))
            else:
                executor_status["signals_rejected"] += 1

        asyncio.create_task(_do_open())
        return web.json_response({"ok": True, "action": "open", "symbol": symbol, "direction": direction})

    if action == "close":
        reason = data.get("reason", "MAIN_BOT").upper()
        close_price = float(data.get("close_price", 0))
        trade = execution_manager.find_by_paper_id(trade_id) or execution_manager.get_trade(symbol, data.get("direction"))

        async def _do_close():
            if trade:
                use_price = close_price if close_price > 0 else trade.current_price or trade.entry_price
                closed = await execution_manager.force_close_trade(trade, reason=reason, close_price=use_price)
                if closed:
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(sess, build_close_message(trade))
            else:
                closed = await execution_manager.force_close_by_symbol(symbol)
                if closed:
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(sess, f"🔄 <b>CIERRE FORZADO (sin estado local)</b>\n<code>{symbol}</code>")

        asyncio.create_task(_do_close())
        return web.json_response({"ok": True, "action": "close", "symbol": symbol})

    if action == "close_all":
        total_open = len(execution_manager.open_trades)

        async def _do_close_all():
            closed_trades = await execution_manager.close_all_global(reason="CLOSE_ALL")
            async with aiohttp.ClientSession() as sess:
                if not closed_trades:
                    await send_telegram(sess, "🛑 CIERRE GLOBAL ejecutado — no había posiciones abiertas.")
                    return
                for t in closed_trades:
                    await send_telegram(sess, build_close_message(t))
                await send_telegram(sess, f"🛑 CIERRE GLOBAL completado — {len(closed_trades)} posición(es) cerrada(s).")

        asyncio.create_task(_do_close_all())
        executor_status["signals_close"] += total_open
        return web.json_response({"ok": True, "action": "close_all", "positions_targeted": total_open})

    # ── TP/SL vía REST — funcionalidad NUEVA, independiente de
    # open/close/close_all (posiciones). Usa los mismos helpers que el
    # dashboard manual (_algo_set_tp_sl / _algo_cancel_tp_sl).
    if action in ("open_tp", "open_sl"):
        order_type = "TAKE_PROFIT_MARKET" if action == "open_tp" else "STOP_MARKET"
        trigger_price = float(data.get("trigger_price", 0))
        trade = execution_manager.get_trade(symbol, data.get("direction"))

        if not trade or trigger_price <= 0:
            executor_status["signals_rejected"] += 1
            return web.json_response(
                {"ok": False, "error": f"{action}: sin posición abierta para {symbol} o trigger_price inválido"},
                status=400,
            )

        async def _do_open_algo():
            try:
                await _algo_set_tp_sl(trade, trigger_price, order_type)
                key = "signals_tp_set" if action == "open_tp" else "signals_sl_set"
                executor_status[key] += 1
                emoji = "🎯" if action == "open_tp" else "🛑"
                label = "TP" if action == "open_tp" else "SL"
                async with aiohttp.ClientSession() as sess:
                    await send_telegram(sess, f"{emoji} <b>{label} actualizado</b>\n<code>{symbol}</code> @ {trigger_price}")
            except Exception as e:
                executor_status["signals_rejected"] += 1
                log.error(f"signal {action}: fallo para {symbol}: {e}")

        asyncio.create_task(_do_open_algo())
        return web.json_response({"ok": True, "action": action, "symbol": symbol, "trigger_price": trigger_price})

    if action in ("close_tp", "close_sl"):
        order_type = "TAKE_PROFIT_MARKET" if action == "close_tp" else "STOP_MARKET"

        if not symbol:
            executor_status["signals_rejected"] += 1
            return web.json_response({"ok": False, "error": f"{action}: falta symbol"}, status=400)

        async def _do_close_algo():
            try:
                n = await _algo_cancel_tp_sl(symbol, order_type)
                key = "signals_tp_closed" if action == "close_tp" else "signals_sl_closed"
                executor_status[key] += 1
                emoji = "🎯" if action == "close_tp" else "🛑"
                label = "TP" if action == "close_tp" else "SL"
                async with aiohttp.ClientSession() as sess:
                    await send_telegram(sess, f"{emoji} <b>{label} cancelado</b>\n<code>{symbol}</code> — {n} orden(es)")
            except Exception as e:
                executor_status["signals_rejected"] += 1
                log.error(f"signal {action}: fallo para {symbol}: {e}")

        asyncio.create_task(_do_close_algo())
        return web.json_response({"ok": True, "action": action, "symbol": symbol})

    executor_status["signals_rejected"] += 1
    return web.json_response({"ok": False, "error": f"unknown action: {action}"}, status=400)


def _check_dashboard_token(request: web.Request) -> bool:
    return request.headers.get("X-Dashboard-Token", "") == SIGNAL_SECRET


async def manual_close_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    symbol = data.get("symbol", "").upper()
    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade:
        return web.json_response({"ok": False, "error": f"no hay posición abierta registrada para {symbol} (si hay LONG y SHORT simultáneas, especifica 'direction')"}, status=404)

    async def _do_manual_close():
        closed = await execution_manager.force_close_trade(trade, reason="MANUAL")
        if closed:
            executor_status["signals_close"] += 1
            executor_status["manual_closes"] += 1
            async with aiohttp.ClientSession() as sess:
                await send_telegram(sess, build_close_message(trade))

    asyncio.create_task(_do_manual_close())
    return web.json_response({"ok": True, "action": "manual_close", "symbol": symbol})


async def manual_close_all_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    total_open = len(execution_manager.open_trades)

    async def _do_close_all():
        closed_trades = await execution_manager.close_all_global(reason="MANUAL")
        async with aiohttp.ClientSession() as sess:
            if not closed_trades:
                await send_telegram(sess, "🖐 Cierre manual global ejecutado — no había posiciones abiertas.")
                return
            for t in closed_trades:
                await send_telegram(sess, build_close_message(t))
            await send_telegram(sess, f"🖐 Cierre manual global completado — {len(closed_trades)} posición(es) cerrada(s).")

    asyncio.create_task(_do_close_all())
    executor_status["signals_close"] += total_open
    executor_status["manual_closes"] += total_open
    return web.json_response({"ok": True, "action": "manual_close_all", "positions_targeted": total_open})


def _position_side_for(direction: str) -> Optional[str]:
    return direction.upper() if HEDGE_MODE else "BOTH"


# ══════════════════════════════════════════════════════════
#  ALGO ORDERS TP/SL — FUNCIONALIDAD NUEVA E INDEPENDIENTE
#  (no toca open_trade/close_trade/force_close_*; la usan tanto el
#  dashboard manual como las nuevas acciones REST open_tp/close_tp/
#  open_sl/close_sl de signal_handler).
# ══════════════════════════════════════════════════════════
async def _algo_set_tp_sl(trade: "Trade", trigger_price: float, order_type: str) -> dict:
    """
    Crea un algo order STOP_MARKET (SL) o TAKE_PROFIT_MARKET (TP) para
    `trade`, cancelando primero cualquier algo order previo del MISMO
    tipo sobre ese símbolo (para no acumular condicionales duplicadas).
    Usa quantity+reduceOnly vía create_tp_sl_order (no closePosition)
    para evitar el -4509 'TIF GTE can only be used with open positions'.
    """
    symbol = trade.symbol
    close_side = "SELL" if trade.direction == "LONG" else "BUY"
    pos_side = _position_side_for(trade.direction)

    try:
        algo_orders = await execution_manager.api.get_open_algo_orders(symbol)
        for o in algo_orders:
            if o.get("type") == order_type:
                await execution_manager.api.cancel_algo_order(o.get("algoId"))
    except Exception as e:
        log.warning(f"_algo_set_tp_sl: no se pudo limpiar {order_type} previo de {symbol}: {e}")

    filters = await execution_manager.api.get_symbol_filters(symbol)
    qty = float(format_qty(trade.quantity, filters.get("stepSize", 0.001)))
    return await execution_manager.api.create_tp_sl_order(
        symbol=symbol, side=close_side, trigger_price=trigger_price,
        order_type=order_type, position_side=pos_side, quantity=qty,
    )


async def _algo_cancel_tp_sl(symbol: str, order_type: str) -> int:
    """Cancela solo los algo orders del tipo indicado (TAKE_PROFIT_MARKET
    o STOP_MARKET) para `symbol`. Devuelve cuántos se cancelaron."""
    algo_orders = await execution_manager.api.get_open_algo_orders(symbol)
    cancelled = 0
    for o in algo_orders:
        if o.get("type") == order_type:
            await execution_manager.api.cancel_algo_order(o.get("algoId"))
            cancelled += 1
    return cancelled


async def manual_set_tp_handler(request: web.Request) -> web.Response:
    """Crea un TAKE_PROFIT_MARKET vía Algo Order API (POST /fapi/v1/algoOrder
    — la WS API rechaza este tipo con -4120: 'use Algo Order endpoints').
    Se envía con quantity+reduceOnly (no closePosition=true) para evitar
    el -4509 'TIF GTE can only be used with open positions': ver detalle
    en BinanceAPI.create_tp_sl_order."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        trigger_price = float(data.get("trigger_price", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade:
        return web.json_response({"ok": False, "error": f"sin posición abierta para {symbol} (si hay LONG y SHORT simultáneas, especifica 'direction')"}, status=404)
    if trigger_price <= 0:
        return web.json_response({"ok": False, "error": "trigger_price inválido"}, status=400)

    try:
        result = await _algo_set_tp_sl(trade, trigger_price, "TAKE_PROFIT_MARKET")
        return web.json_response({"ok": True, "symbol": symbol, "tp": trigger_price, "result": result})
    except Exception as e:
        log.error(f"manual_set_tp: fallo creando TP para {symbol}: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_set_sl_handler(request: web.Request) -> web.Response:
    """Crea un STOP_MARKET vía Algo Order API. Se envía con
    quantity+reduceOnly (no closePosition=true) para evitar el -4509
    'TIF GTE can only be used with open positions': ver detalle en
    BinanceAPI.create_tp_sl_order."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        trigger_price = float(data.get("trigger_price", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade:
        return web.json_response({"ok": False, "error": f"sin posición abierta para {symbol} (si hay LONG y SHORT simultáneas, especifica 'direction')"}, status=404)
    if trigger_price <= 0:
        return web.json_response({"ok": False, "error": "trigger_price inválido"}, status=400)

    try:
        result = await _algo_set_tp_sl(trade, trigger_price, "STOP_MARKET")
        return web.json_response({"ok": True, "symbol": symbol, "sl": trigger_price, "result": result})
    except Exception as e:
        log.error(f"manual_set_sl: fallo creando SL para {symbol}: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_cancel_tp_sl_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        direction = data.get("direction")
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    # En Hedge Mode, cada algo order trae su propio positionSide (LONG/SHORT).
    # Si el cliente especifica `direction`, sólo se cancelan los TP/SL de ESE
    # lado, para no tocar accidentalmente el TP/SL de la posición opuesta.
    wanted_pos_side = direction.upper() if (direction and HEDGE_MODE) else None

    try:
        algo_orders = await execution_manager.api.get_open_algo_orders(symbol)
        cancelled = 0
        for o in algo_orders:
            if o.get("type") not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                continue
            if wanted_pos_side and o.get("positionSide") not in (wanted_pos_side, None):
                continue
            await execution_manager.api.cancel_algo_order(o.get("algoId"))
            cancelled += 1
        return web.json_response({"ok": True, "symbol": symbol, "cancelled": cancelled})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_limit_order_handler(request: web.Request) -> web.Response:
    """Coloca una orden LIMIT manual (reduceOnly opcional) para el símbolo."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        side = data.get("side", "").upper()  # BUY | SELL
        price = float(data.get("price", 0))
        quantity = float(data.get("quantity", 0))
        reduce_only = bool(data.get("reduce_only", False))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if not symbol or side not in ("BUY", "SELL") or price <= 0 or quantity <= 0:
        return web.json_response({"ok": False, "error": "parámetros inválidos"}, status=400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    pos_side = _position_side_for(trade.direction) if trade else ("BOTH" if not HEDGE_MODE else None)
    try:
        result = await execution_manager.api.create_limit_order(
            symbol=symbol, side=side, quantity=quantity, price=price,
            position_side=pos_side, reduce_only=reduce_only,
        )
        return web.json_response({"ok": True, "result": result})
    except Exception as e:
        log.error(f"manual_limit_order: fallo en LIMIT {symbol}: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_set_symbol_leverage_handler(request: web.Request) -> web.Response:
    """Cambia el leverage de UN símbolo puntual (con escalera de respaldo),
    sin afectar el leverage global por defecto."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        leverage = int(data.get("leverage", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if not symbol or leverage <= 0:
        return web.json_response({"ok": False, "error": "parámetros inválidos"}, status=400)

    try:
        applied = await execution_manager.api.set_leverage_with_fallback(symbol, leverage)
        # Aplica el nuevo leverage a TODAS las posiciones abiertas de este
        # símbolo (LONG y SHORT pueden coexistir en Hedge Mode).
        for trade in execution_manager.trades_for_symbol(symbol):
            trade.leverage = applied
        return web.json_response({"ok": True, "symbol": symbol, "requested": leverage, "applied": applied})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_get_position_mode_handler(request: web.Request) -> web.Response:
    """Consulta el modo de posición ACTUAL en la cuenta de Binance (fuente
    de verdad) además del flag local HEDGE_MODE que usa este proceso."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        account_hedge = await execution_manager.api.get_position_mode()
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=502)

    return web.json_response({
        "ok": True,
        "account_hedge_mode": account_hedge,   # lo que Binance tiene configurado de verdad
        "local_hedge_mode": HEDGE_MODE,         # lo que este proceso está usando
        "in_sync": account_hedge == HEDGE_MODE,
        "open_positions": len(execution_manager._trades),
    })


async def manual_set_position_mode_handler(request: web.Request) -> web.Response:
    """Cambia el modo de posición de la cuenta (Hedge <-> One-way) y, si
    Binance lo acepta, también actualiza el flag local HEDGE_MODE para
    que el bot empiece a operar en ese modo inmediatamente.

    Body esperado: {"hedge_mode": true}  o  {"hedge_mode": false}
    """
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
        hedge_mode = bool(data.get("hedge_mode"))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    # Binance rechaza el cambio si hay posiciones u órdenes abiertas — se
    # valida también localmente para dar un mensaje claro de inmediato,
    # antes de gastar la llamada REST.
    open_count = len(execution_manager._trades)
    if open_count > 0:
        return web.json_response(
            {
                "ok": False,
                "error": (
                    f"No se puede cambiar el modo de posición con {open_count} posición(es) "
                    f"abierta(s) localmente. Cierra todas las posiciones primero (Binance "
                    f"rechaza este cambio si la cuenta tiene posiciones u órdenes activas)."
                ),
            },
            status=409,
        )

    try:
        await execution_manager.api.set_position_mode(hedge_mode)
    except Exception as e:
        err = str(e)
        # -4059: "No need to change position side." -> ya estaba en ese modo
        if "-4059" in err:
            set_hedge_mode_runtime(hedge_mode)
            return web.json_response({"ok": True, "hedge_mode": hedge_mode, "note": "la cuenta ya estaba en ese modo"})
        # -4068 / similares: hay posiciones u órdenes abiertas en Binance
        # aunque localmente no se vieran (p.ej. quedaron huérfanas).
        return web.json_response(
            {"ok": False, "error": f"Binance rechazó el cambio: {err}"},
            status=409 if ("-4068" in err or "-4067" in err or "position" in err.lower()) else 502,
        )

    set_hedge_mode_runtime(hedge_mode)
    log.info(f"manual_set_position_mode_handler: modo de posición cambiado a {'HEDGE' if hedge_mode else 'ONE-WAY'}")
    return web.json_response({"ok": True, "hedge_mode": hedge_mode})


async def manual_set_margin_type_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        margin_type = data.get("margin_type", "").upper()  # ISOLATED | CROSSED
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if margin_type not in ("ISOLATED", "CROSSED"):
        return web.json_response({"ok": False, "error": "margin_type debe ser ISOLATED o CROSSED"}, status=400)

    try:
        result = await execution_manager.api.set_margin_type(symbol, margin_type)
        return web.json_response({"ok": True, "symbol": symbol, "margin_type": margin_type, "result": result})
    except Exception as e:
        # -4046 = "No need to change margin type" -> ya estaba en ese modo, no es un error real
        if "-4046" in str(e):
            return web.json_response({"ok": True, "symbol": symbol, "margin_type": margin_type, "note": "ya estaba en ese modo"})
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_modify_margin_handler(request: web.Request) -> web.Response:
    """Añade o retira margen aislado de una posición abierta."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        amount = float(data.get("amount", 0))
        add = bool(data.get("add", True))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade or amount <= 0:
        return web.json_response({"ok": False, "error": "posición no encontrada o monto inválido (si hay LONG y SHORT simultáneas, especifica 'direction')"}, status=400)

    pos_side = _position_side_for(trade.direction)
    try:
        result = await execution_manager.api.modify_position_margin(symbol, amount, position_side=pos_side, add=add)
        return web.json_response({"ok": True, "symbol": symbol, "amount": amount, "add": add, "result": result})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_get_orders_handler(request: web.Request) -> web.Response:
    """Devuelve las órdenes LIMIT abiertas + los Algo Orders (TP/SL)
    activos de un símbolo, usado por el modal de gestión del dashboard."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    symbol = request.query.get("symbol", "").upper()
    if not symbol:
        return web.json_response({"ok": False, "error": "symbol requerido"}, status=400)
    try:
        normal_orders, algo_orders = await asyncio.gather(
            execution_manager.api.get_open_orders(symbol),
            execution_manager.api.get_open_algo_orders(symbol),
            return_exceptions=True,
        )
        normal_orders = normal_orders if isinstance(normal_orders, list) else []
        algo_orders = algo_orders if isinstance(algo_orders, list) else []
        for o in algo_orders:
            o["_algo"] = True
        return web.json_response({"ok": True, "symbol": symbol, "orders": normal_orders + algo_orders})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=502)


async def manual_toggle_trading_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    execution_manager.trading_enabled = not execution_manager.trading_enabled
    state = "ACTIVADO 🟢" if execution_manager.trading_enabled else "PAUSADO 🔴 (no se enviarán nuevas posiciones)"
    log.warning(f"Trading {state} manualmente desde el dashboard")
    return web.json_response({"ok": True, "trading_enabled": execution_manager.trading_enabled})


async def manual_set_leverage_handler(request: web.Request) -> web.Response:
    global LEVERAGE
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
        new_lev = int(data.get("leverage", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if new_lev < 1 or new_lev > 125:
        return web.json_response({"ok": False, "error": "leverage debe estar entre 1 y 125"}, status=400)

    LEVERAGE = new_lev
    log.warning(f"Leverage por defecto cambiado desde el dashboard a {LEVERAGE}x (aplica a próximas posiciones)")
    return web.json_response({"ok": True, "leverage": LEVERAGE})


async def manual_clear_history_handler(request: web.Request) -> web.Response:
    """Borra el historial de operaciones cerradas y reinicia el PnL realizado."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    em = execution_manager
    count = len(em._closed)
    em._closed.clear()

    # Reinicia también los contadores de señales para coherencia visual
    executor_status["signals_open"] = 0
    executor_status["signals_close"] = 0
    executor_status["signals_received"] = 0
    executor_status["signals_rejected"] = 0
    executor_status["manual_closes"] = 0
    executor_status["signals_tp_set"] = 0
    executor_status["signals_tp_closed"] = 0
    executor_status["signals_sl_set"] = 0
    executor_status["signals_sl_closed"] = 0
    executor_status["last_signal_time"] = "Historial borrado"
    executor_status["last_signal_detail"] = ""

    log.warning(f"Historial de operaciones cerradas borrado desde el dashboard ({count} operaciones eliminadas)")
    return web.json_response({"ok": True, "cleared": count})


async def api_state_handler(request: web.Request) -> web.Response:
    em = execution_manager

    def ser(t: Trade) -> dict:
        return {
            "id": t.id,
            "paper_trade_id": t.paper_trade_id,
            "symbol": t.symbol,
            "direction": t.direction,
            "entry_price": t.entry_price,
            "quantity": t.quantity,
            "notional": t.notional_usdt,
            "leverage": t.leverage,
            "open_time": t.open_time,
            "current_price": t.current_price,
            "status": t.status,
            "close_price": t.close_price,
            "close_time": t.close_time,
            "pnl_usdt": t.pnl_usdt,
            "roi_pct": t.roi_pct,
            "order_assumed": t.order_assumed,
            "entry_order_id": t.entry_order_id,
        }

    closed = em.closed_trades
    wins = sum(1 for t in closed if t.status == "TP")
    total = len(closed)

    return web.json_response({
        "balance": em.balance,
        "equity": em.equity,
        "realized_pnl": em.total_realized_pnl,
        "unrealized_pnl": em.unrealized_pnl,
        "wins": wins,
        "losses": total - wins,
        "win_rate": (wins / total * 100) if total else None,
        "open_count": len(em.open_trades),
        "open_longs": len(em.open_longs),
        "open_shorts": len(em.open_shorts),
        "open_trades": [ser(t) for t in em.open_trades],
        "closed_trades": [ser(t) for t in closed],
        "executor_status": executor_status,
        "ws_symbols": ", ".join(sorted(em.active_symbols)) or "ninguno",
        "leverage": LEVERAGE,
        "trading_enabled": em.trading_enabled,
        "testnet": USE_TESTNET,
        "hedge_mode": HEDGE_MODE,
    })


DASHBOARD_JS_TEMPLATE = """
<script>
const DASH_TOKEN = __DASH_TOKEN__;

async function refresh() {
  try {
    const r = await fetch('/api/state');
    const d = await r.json();
    const q = id => document.getElementById(id);

    q('bal').textContent = d.balance.toFixed(2) + ' USDT';
    q('eq').textContent = d.equity.toFixed(2) + ' USDT';
    q('rpnl').textContent = (d.realized_pnl >= 0 ? '+' : '') + d.realized_pnl.toFixed(4) + ' USDT';
    q('upnl').textContent = (d.unrealized_pnl >= 0 ? '+' : '') + d.unrealized_pnl.toFixed(4) + ' USDT';
    q('wr').textContent = d.win_rate != null ? d.win_rate.toFixed(1) + '% (' + d.wins + '✅/' + d.losses + '❌)' : 'N/A';
    q('pos').textContent = d.open_count + ' — ' + d.open_longs + 'L / ' + d.open_shorts + 'S';
    q('lev').textContent = d.leverage + 'x';
    q('sig_rx').textContent = d.executor_status.signals_received;
    q('sig_ok').textContent = d.executor_status.signals_open + ' abiertas / ' + d.executor_status.signals_close + ' cerradas';
    q('sig_rej').textContent = d.executor_status.signals_rejected;
    q('last_sig').textContent = d.executor_status.last_signal_time + ' — ' + d.executor_status.last_signal_detail;
    q('ws_sym').textContent = 'WS activo (ws api): ' + d.ws_symbols;
    q('close_all_btn').disabled = d.open_count === 0;

    const tState = q('trading_state');
    const tBtn = q('trading_toggle_btn');
    if (tState && tBtn) {
      tState.textContent = d.trading_enabled ? '🟢 ACTIVO' : '🔴 PAUSADO';
      tState.style.color = d.trading_enabled ? '#3fb950' : '#f85149';
      tBtn.textContent = d.trading_enabled ? '⏸ Pausar nuevas posiciones' : '▶ Reactivar nuevas posiciones';
    }

    const ob = document.getElementById('open_body');
    if (!d.open_trades.length) {
      ob.innerHTML = '<tr><td colspan="12" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>';
    } else {
      ob.innerHTML = d.open_trades.map(t => {
        const dir = t.direction === 'LONG' ? '🟢 LONG' : '🔴 SHORT';
        const pnl = t.pnl_usdt >= 0 ? '+' + t.pnl_usdt.toFixed(4) : t.pnl_usdt.toFixed(4);
        const roi = t.roi_pct >= 0 ? '+' + t.roi_pct.toFixed(2) + '%' : t.roi_pct.toFixed(2) + '%';
        const assum = t.order_assumed ? ' ⚠️' : '';
        return `<tr>
          <td>#${t.id}</td><td><b>${t.symbol}</b></td><td>${dir}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.current_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${roi}</td>
          <td>${t.notional.toFixed(4)} USDT</td><td>${t.quantity}</td>
          <td>${t.open_time}${assum}</td>
          <td><button class="btn-close" onclick="closeTrade('${t.symbol}','${t.direction}')">Cerrar</button>
              <button class="btn-manage" onclick="openManageModal('${t.symbol}','${t.direction}',${t.entry_price},${t.quantity},${t.leverage})">⚙</button></td>
        </tr>`;
      }).join('');
    }

    const cb = document.getElementById('closed_body');
    const recent = d.closed_trades.slice(-30).reverse();
    if (!recent.length) {
      cb.innerHTML = '<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>';
    } else {
      cb.innerHTML = recent.map(t => {
        const pnl = t.pnl_usdt >= 0 ? '+' + t.pnl_usdt.toFixed(4) : t.pnl_usdt.toFixed(4);
        const res = t.status;
        return `<tr>
          <td>#${t.id}</td><td>${t.symbol}</td><td>${t.direction}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.close_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${t.roi_pct.toFixed(2)}%</td>
          <td>${res}</td>
        </tr>`;
      }).join('');
    }
  } catch(e) { console.error(e); }
}

async function closeTrade(symbol, direction) {
  if (!confirm('¿Cerrar manualmente la posición ' + symbol + (direction ? ' (' + direction + ')' : '') + '?')) return;
  try {
    const body = {symbol: symbol};
    if (direction) body.direction = direction;
    const r = await fetch('/manual/close', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!d.ok) alert('Error al cerrar ' + symbol + ': ' + (d.error || 'desconocido'));
    refresh();
  } catch(e) { alert('Error de red al cerrar ' + symbol); }
}

async function closeAllTrades() {
  if (!confirm('¿Cerrar TODAS las posiciones abiertas manualmente? Esta acción no se puede deshacer.')) return;
  try {
    const r = await fetch('/manual/close_all', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) alert('Error al cerrar todas las posiciones: ' + (d.error || 'desconocido'));
    refresh();
  } catch(e) { alert('Error de red al cerrar todas las posiciones'); }
}

async function toggleTrading() {
  const active = document.getElementById('trading_state').textContent.includes('ACTIVO');
  const msg = active
    ? '¿Pausar el envío de NUEVAS posiciones? Las posiciones ya abiertas seguirán gestionándose con normalidad (cierres, PnL, etc).'
    : '¿Reactivar el envío de nuevas posiciones?';
  if (!confirm(msg)) return;
  try {
    const r = await fetch('/manual/toggle_trading', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al cambiar el estado de trading'); }
}

async function setLeverage() {
  const val = parseInt(document.getElementById('lev_input').value, 10);
  if (!val || val < 1 || val > 125) { alert('Leverage inválido (debe ser entre 1 y 125)'); return; }
  if (!confirm('¿Cambiar el leverage por defecto a ' + val + 'x para las próximas posiciones?')) return;
  try {
    const r = await fetch('/manual/set_leverage', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({leverage: val})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al cambiar el leverage'); }
}

async function setPositionMode(hedge) {
  const label = hedge ? 'Hedge Mode (LONG y SHORT independientes)' : 'One-way Mode (posición neta única)';
  if (!confirm('¿Cambiar el modo de posición de la cuenta a ' + label + '? Binance exige que NO haya posiciones ni órdenes abiertas para permitir el cambio.')) return;
  try {
    const r = await fetch('/manual/set_position_mode', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({hedge_mode: hedge})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    alert('Modo de posición actualizado a ' + (d.hedge_mode ? 'Hedge Mode' : 'One-way Mode') + '.');
    refresh();
  } catch(e) { alert('Error de red al cambiar el modo de posición'); }
}

async function clearHistory() {
  if (!confirm('¿Borrar TODO el historial de operaciones cerradas y reiniciar el PnL realizado? Esta acción no se puede deshacer.')) return;
  try {
    const r = await fetch('/manual/clear_history', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    alert('Historial borrado (' + d.cleared + ' operaciones eliminadas). PnL realizado reiniciado a 0.');
    refresh();
  } catch(e) { alert('Error de red al borrar historial'); }
}

let manageSymbol = null, manageDirection = null, manageEntry = 0, manageQty = 0, manageLev = 1;

function openManageModal(symbol, direction, entry, qty, lev) {
  manageSymbol = symbol; manageDirection = direction; manageEntry = entry; manageQty = qty; manageLev = lev || 1;
  document.getElementById('mm_title').textContent = '⚙ Gestionar ' + symbol + ' (' + direction + ')';
  document.getElementById('mm_lev_input').value = lev;
  document.getElementById('mm_entry_info').textContent =
    'Entrada: $' + entry + ' | Cantidad: ' + qty + ' | Margen ≈ ' + (entry * qty / (lev || 1)).toFixed(2) + ' USDT (' + lev + 'x)';
  showManageTab('tp');
  document.getElementById('manage_modal').style.display = 'flex';
  loadManageOrders();
}

function closeManageModal() { document.getElementById('manage_modal').style.display = 'none'; }

function showManageTab(tab) {
  ['tp','sl','limit','margin','lev'].forEach(t => {
    document.getElementById('mm_tab_' + t).style.display = (t === tab ? 'block' : 'none');
    document.getElementById('mm_btn_' + t).classList.toggle('active', t === tab);
  });
}

async function mmFetch(path, body) {
  try {
    const r = await fetch(path, {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return null; }
    return d;
  } catch(e) { alert('Error de red'); return null; }
}

async function loadManageOrders() {
  try {
    const r = await fetch('/manual/orders?symbol=' + manageSymbol, {headers: {'X-Dashboard-Token': DASH_TOKEN}});
    const d = await r.json();
    const box = document.getElementById('mm_orders_box');
    if (!d.ok || !d.orders.length) { box.textContent = 'Sin órdenes TP/SL/LIMIT activas.'; return; }
    box.innerHTML = d.orders.map(o => {
      const priceShown = o.triggerPrice || o.stopPrice || o.price;
      const id = o._algo ? o.algoId : o.orderId;
      const cancelFn = o._algo ? 'mmCancelAlgoOrder' : 'mmCancelOrder';
      return `<div>${o.type} ${o.side} @ ${priceShown} ` +
        `<button class="btn-mini-close" onclick="${cancelFn}('${id}')">✕</button></div>`;
    }).join('');
  } catch(e) {}
}

async function mmCancelOrder(orderId) {
  // Cancela TODOS los TP/SL (algo orders) del símbolo/dirección de un golpe.
  const r = await mmFetch('/manual/cancel_tp_sl', {symbol: manageSymbol, direction: manageDirection});
  if (r) { loadManageOrders(); }
}

async function mmCancelAlgoOrder(algoId) {
  const r = await mmFetch('/manual/cancel_tp_sl', {symbol: manageSymbol, direction: manageDirection});
  if (r) { loadManageOrders(); }
}

// ── Calculadora de precio TP/SL a partir de ganancia($) o ROI(%) ──
// LONG: precio_objetivo = entrada + ganancia/qty
// SHORT: precio_objetivo = entrada - ganancia/qty
// ganancia (a partir de ROI%) = (roi/100) * margen = (roi/100) * entrada*qty/leverage
function mmClampPrice(price) {
  const n = Number(price);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0.00001, n);
}

function mmCalcPriceFromUsdt(profitUsdt) {
  if (!manageQty || manageQty <= 0) return 0;
  const signedProfit = manageDirection === 'LONG' ? profitUsdt : -profitUsdt;
  return mmClampPrice(manageEntry + (signedProfit / manageQty));
}
function mmCalcPriceFromRoi(roiPct) {
  if (!manageQty || manageQty <= 0) return 0;
  const margin = (manageEntry * manageQty) / (manageLev || 1);
  const profitUsdt = (roiPct / 100) * margin;
  return mmCalcPriceFromUsdt(profitUsdt);
}

function mmCalcTPFromUsdt() {
  const v = parseFloat(document.getElementById('mm_tp_usdt').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa una ganancia en USDT'); return; }
  const price = mmCalcPriceFromUsdt(Math.abs(v));
  if (!price) { alert('No se pudo calcular el TP'); return; }
  document.getElementById('mm_tp_price').value = price.toFixed(8);
}
function mmCalcTPFromRoi() {
  const v = parseFloat(document.getElementById('mm_tp_roi').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa un ROI % objetivo'); return; }
  const price = mmCalcPriceFromRoi(Math.abs(v));
  if (!price) { alert('No se pudo calcular el TP'); return; }
  document.getElementById('mm_tp_price').value = price.toFixed(8);
}
function mmCalcSLFromUsdt() {
  const v = parseFloat(document.getElementById('mm_sl_usdt').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa la pérdida máxima en USDT'); return; }
  const price = mmCalcPriceFromUsdt(-Math.abs(v));
  if (!price) { alert('No se pudo calcular el SL'); return; }
  document.getElementById('mm_sl_price').value = price.toFixed(8);
}
function mmCalcSLFromRoi() {
  const v = parseFloat(document.getElementById('mm_sl_roi').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa la pérdida máxima en ROI %'); return; }
  const price = mmCalcPriceFromRoi(-Math.abs(v));
  if (!price) { alert('No se pudo calcular el SL'); return; }
  document.getElementById('mm_sl_price').value = price.toFixed(8);
}

async function mmSetTP() {
  const p = parseFloat(document.getElementById('mm_tp_price').value);
  if (!p || p <= 0) { alert('Precio de TP inválido'); return; }
  const d = await mmFetch('/manual/set_tp', {symbol: manageSymbol, trigger_price: p, direction: manageDirection});
  if (d) { alert('TP configurado en $' + p); loadManageOrders(); }
}

async function mmSetSL() {
  const p = parseFloat(document.getElementById('mm_sl_price').value);
  if (!p || p <= 0) { alert('Precio de SL inválido'); return; }
  const d = await mmFetch('/manual/set_sl', {symbol: manageSymbol, trigger_price: p, direction: manageDirection});
  if (d) { alert('SL configurado en $' + p); loadManageOrders(); }
}

async function mmCancelAllTpSl() {
  if (!confirm('¿Cancelar TODOS los TP/SL activos de ' + manageSymbol + ' (' + manageDirection + ')?')) return;
  const d = await mmFetch('/manual/cancel_tp_sl', {symbol: manageSymbol, direction: manageDirection});
  if (d) { alert('TP/SL cancelados (' + d.cancelled + ')'); loadManageOrders(); }
}

async function mmLimitOrder() {
  const side = document.getElementById('mm_limit_side').value;
  const price = parseFloat(document.getElementById('mm_limit_price').value);
  const qty = parseFloat(document.getElementById('mm_limit_qty').value);
  const reduceOnly = document.getElementById('mm_limit_reduce').checked;
  if (!price || !qty) { alert('Precio/cantidad inválidos'); return; }
  const d = await mmFetch('/manual/limit_order', {symbol: manageSymbol, side, price, quantity: qty, reduce_only: reduceOnly, direction: manageDirection});
  if (d) alert('Orden LIMIT enviada');
}

async function mmModifyMargin(add) {
  const amount = parseFloat(document.getElementById('mm_margin_amount').value);
  if (!amount || amount <= 0) { alert('Monto inválido'); return; }
  const d = await mmFetch('/manual/modify_margin', {symbol: manageSymbol, amount, add, direction: manageDirection});
  if (d) alert((add ? 'Margen añadido' : 'Margen retirado') + ': ' + amount + ' USDT');
}

async function mmSetMarginType(type) {
  if (!confirm('¿Cambiar tipo de margen de ' + manageSymbol + ' a ' + type + '?')) return;
  const d = await mmFetch('/manual/set_margin_type', {symbol: manageSymbol, margin_type: type});
  if (d) alert('Tipo de margen: ' + type);
}

async function mmSetSymbolLeverage() {
  const val = parseInt(document.getElementById('mm_lev_input').value, 10);
  if (!val || val < 1 || val > 125) { alert('Leverage inválido'); return; }
  const d = await mmFetch('/manual/set_symbol_leverage', {symbol: manageSymbol, leverage: val});
  if (d) { alert('Leverage aplicado: ' + d.applied + 'x' + (d.applied !== val ? ' (rechazado ' + val + 'x, se usó la escalera de respaldo)' : '')); refresh(); }
}

refresh();
setInterval(refresh, 5000);
</script>
"""

async def dashboard_handler(request: web.Request) -> web.Response:
    em = execution_manager
    es = executor_status
    env = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"

    closed = em.closed_trades
    wins = sum(1 for t in closed if t.status == "TP")
    losses = len(closed) - wins
    wr_str = f"{wins / len(closed) * 100:.1f}%" if closed else "N/A"
    eq_col = "#3fb950" if em.equity >= em.balance else "#f85149"
    rp_col = "#3fb950" if em.total_realized_pnl >= 0 else "#f85149"
    up_col = "#3fb950" if em.unrealized_pnl >= 0 else "#f85149"
    dashboard_js = DASHBOARD_JS_TEMPLATE.replace("__DASH_TOKEN__", json.dumps(SIGNAL_SECRET))

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Futures Executor WS</title>
  <style>
    body{{font-family:Arial,Helvetica,sans-serif;background:#0d1117;color:#c9d1d9;padding:1.2rem}}
    h1{{color:#f0883e;margin-bottom:.8rem;font-size:1.35rem}}
    h2{{color:#58a6ff;margin:.9rem 0 .5rem;font-size:.95rem;display:flex;align-items:center;gap:.6rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.6rem;margin-bottom:1.2rem}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:.75rem}}
    .card .label{{color:#8b949e;font-size:.7rem;margin-bottom:.25rem;text-transform:uppercase;letter-spacing:.04em}}
    .card .value{{color:#f0f6fc;font-size:.95rem;font-weight:bold}}
    .wrap{{overflow-x:auto;margin-bottom:1.2rem}}
    table{{width:100%;border-collapse:collapse;font-size:.78rem;min-width:700px}}
    th{{color:#8b949e;text-align:left;padding:.35rem .45rem;border-bottom:1px solid #30363d;white-space:nowrap;font-size:.71rem}}
    td{{padding:.3rem .45rem;border-bottom:1px solid #1c2128;white-space:nowrap}}
    tr:hover td{{background:#161b22}}
    .dot{{display:inline-block;width:8px;height:8px;background:#3fb950;border-radius:50%;margin-right:5px;animation:blink 1.5s infinite}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .info-banner{{background:#161b22;border:1px solid #58a6ff;border-radius:8px;padding:.7rem 1rem;margin-bottom:1rem;color:#58a6ff;font-size:.82rem}}
    .btn-close{{background:#f85149;color:#fff;border:none;border-radius:4px;padding:.25rem .6rem;font-size:.7rem;cursor:pointer}}
    .btn-close-all{{background:#f85149;color:#fff;border:none;border-radius:5px;padding:.4rem .9rem;font-size:.78rem;cursor:pointer;font-weight:bold}}
    .btn-close-all:disabled{{background:#30363d;color:#6e7681;cursor:not-allowed}}
    .btn-toggle{{border:none;border-radius:5px;padding:.35rem .7rem;font-size:.72rem;cursor:pointer;font-weight:bold;width:100%;margin-top:.4rem;background:#30363d;color:#f0f6fc}}
    .lev-row{{display:flex;gap:.35rem;margin-top:.4rem}}
    .lev-row input{{width:55px;background:#0d1117;border:1px solid #30363d;border-radius:4px;color:#c9d1d9;padding:.2rem .3rem;font-size:.8rem}}
    .lev-row button{{background:#58a6ff;color:#0d1117;border:none;border-radius:4px;padding:.2rem .5rem;font-size:.72rem;cursor:pointer;font-weight:bold}}
    .btn-manage{{background:#30363d;color:#f0f6fc;border:none;border-radius:4px;padding:.25rem .5rem;font-size:.7rem;cursor:pointer;margin-left:.3rem}}
    .modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:50;align-items:center;justify-content:center}}
    .modal-box{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:1rem 1.2rem;width:min(480px,92vw);max-height:85vh;overflow-y:auto}}
    .modal-box h3{{color:#f0883e;margin:0 0 .7rem;font-size:1.05rem;display:flex;justify-content:space-between}}
    .mm-tabs{{display:flex;gap:.3rem;margin-bottom:.8rem;flex-wrap:wrap}}
    .mm-tab-btn{{background:#0d1117;color:#8b949e;border:1px solid #30363d;border-radius:6px;padding:.3rem .6rem;font-size:.72rem;cursor:pointer}}
    .mm-tab-btn.active{{background:#58a6ff;color:#0d1117;border-color:#58a6ff}}
    .mm-field{{margin-bottom:.6rem}}
    .mm-field label{{display:block;color:#8b949e;font-size:.72rem;margin-bottom:.2rem}}
    .mm-field input,.mm-field select{{width:100%;background:#0d1117;border:1px solid #30363d;border-radius:4px;color:#c9d1d9;padding:.35rem .5rem;font-size:.85rem;box-sizing:border-box}}
    .mm-field input[type=checkbox]{{width:auto}}
    .mm-action{{background:#3fb950;color:#0d1117;border:none;border-radius:5px;padding:.4rem .8rem;font-size:.8rem;cursor:pointer;font-weight:bold;margin-right:.4rem;margin-top:.2rem}}
    .mm-action.danger{{background:#f85149}}
    .btn-mini-close{{background:#f85149;color:#fff;border:none;border-radius:3px;padding:0 .35rem;font-size:.68rem;cursor:pointer;margin-left:.4rem}}
    #mm_orders_box{{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:.5rem;font-size:.74rem;margin-bottom:.8rem;color:#c9d1d9}}
  </style>
</head>
<body>
  <h1>⚡ Futures Executor WS — Binance USDT Perpetuos [{env}]</h1>
  <div class="info-banner">
    📡 Trading por <b>WebSocket API</b>. Precios en tiempo real vía <b>ws.py</b>. 
    Cambios de leverage por <b>REST</b> (la WS API no lo soporta). Leverage configurado: <b>{LEVERAGE}x</b>{' | Modo Hedge' if HEDGE_MODE else ' | Modo One-way'}.
  </div>

  <div class="grid">
    <div class="card"><div class="label">Balance USDT</div><div class="value" id="bal">{em.balance:.2f} USDT</div></div>
    <div class="card"><div class="label">Equity total</div><div class="value" id="eq" style="color:{eq_col}">{em.equity:.2f} USDT</div></div>
    <div class="card"><div class="label">PnL realizado</div><div class="value" id="rpnl" style="color:{rp_col}">{em.total_realized_pnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">PnL no realizado</div><div class="value" id="upnl" style="color:{up_col}">{em.unrealized_pnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value" id="wr">{wr_str} ({wins}✅/{losses}❌)</div></div>
    <div class="card"><div class="label">Posiciones abiertas</div><div class="value" id="pos">{len(em.open_trades)} — {len(em.open_longs)}L / {len(em.open_shorts)}S</div></div>
    <div class="card">
      <div class="label">Leverage</div><div class="value" id="lev">{LEVERAGE}x</div>
      <div class="lev-row">
        <input id="lev_input" type="number" min="1" max="125" value="{LEVERAGE}">
        <button onclick="setLeverage()">Aplicar</button>
      </div>
    </div>
    <div class="card">
      <div class="label">Trading</div>
      <div class="value" id="trading_state" style="color:{'#3fb950' if em.trading_enabled else '#f85149'}">{'🟢 ACTIVO' if em.trading_enabled else '🔴 PAUSADO'}</div>
      <button class="btn-toggle" id="trading_toggle_btn" onclick="toggleTrading()">{'⏸ Pausar nuevas posiciones' if em.trading_enabled else '▶ Reactivar nuevas posiciones'}</button>
    </div>
    <div class="card">
      <div class="label">Modo de posición</div>
      <div class="value" id="pos_mode_value">{'🔀 Hedge Mode' if HEDGE_MODE else '➡️ One-way Mode'}</div>
      <div class="lev-row">
        <button onclick="setPositionMode(false)" {"disabled" if not HEDGE_MODE else ""}>One-way</button>
        <button onclick="setPositionMode(true)" {"disabled" if HEDGE_MODE else ""}>Hedge</button>
      </div>
      <div style="font-size:.68rem;color:#8b949e;margin-top:.3rem">Requiere 0 posiciones/órdenes abiertas en Binance para poder cambiarlo.</div>
    </div>
  </div>

  <h2>📡 Señales Recibidas</h2>
  <div class="grid">
    <div class="card"><div class="label">Total recibidas</div><div class="value" id="sig_rx">{es['signals_received']}</div></div>
    <div class="card"><div class="label">Ejecutadas</div><div class="value" id="sig_ok">{es['signals_open']} abiertas / {es['signals_close']} cerradas</div></div>
    <div class="card"><div class="label">Rechazadas</div><div class="value" id="sig_rej">{es['signals_rejected']}</div></div>
    <div class="card" style="grid-column:span 2"><div class="label">Última señal</div><div class="value" id="last_sig" style="font-size:.8rem">{es['last_signal_time']} — {es['last_signal_detail']}</div></div>
  </div>

  <h2>
    <span class="dot"></span>📊 Posiciones Reales Abiertas
    <button class="btn-close-all" id="close_all_btn" onclick="closeAllTrades()" {"disabled" if not em.open_trades else ""}>🛑 Cerrar TODO</button>
  </h2>
  <p id="ws_sym" style="color:#484f58;font-size:.72rem;margin-bottom:.4rem">WS activo: {", ".join(sorted(em.active_symbols)) or "ninguno"}</p>
  <div class="wrap"><table>
    <thead><tr>
      <th>ID</th><th>Par</th><th>Dirección</th><th>Lev</th><th>Entrada</th><th>Actual</th>
      <th>PnL</th><th>ROI%</th><th>Notional</th><th>Qty</th><th>Abierto</th><th>Acción</th>
    </tr></thead>
    <tbody id="open_body">
      <tr><td colspan="12" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>
    </tbody>
  </table></div>

  <h2>📋 Operaciones Cerradas (últimas 30)
    <button class="btn-close-all" style="background:#8b949e;font-size:.72rem;padding:.3rem .7rem" onclick="clearHistory()">🗑 Borrar historial y PnL</button>
  </h2>
  <div class="wrap"><table>
    <thead><tr>
      <th>#</th><th>Par</th><th>Dir</th><th>Lev</th><th>Entrada</th><th>Salida</th><th>PnL</th><th>ROI%</th><th>Resultado</th>
    </tr></thead>
    <tbody id="closed_body">
      <tr><td colspan="9" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>
    </tbody>
  </table></div>

  <p style="color:#484f58;margin-top:.6rem;font-size:.7rem">
    Executor WS | Iniciado: {es['started_at']} | Actualizado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
  </p>

  <div class="modal-overlay" id="manage_modal">
    <div class="modal-box">
      <h3><span id="mm_title">⚙ Gestionar</span><span style="cursor:pointer" onclick="closeManageModal()">✕</span></h3>
      <p id="mm_entry_info" style="color:#8b949e;font-size:.74rem;margin:-.3rem 0 .6rem"></p>
      <div id="mm_orders_box">Cargando órdenes...</div>
      <div class="mm-tabs">
        <button class="mm-tab-btn" id="mm_btn_tp" onclick="showManageTab('tp')">🎯 TP</button>
        <button class="mm-tab-btn" id="mm_btn_sl" onclick="showManageTab('sl')">🛡 SL</button>
        <button class="mm-tab-btn" id="mm_btn_limit" onclick="showManageTab('limit')">📋 Limit</button>
        <button class="mm-tab-btn" id="mm_btn_margin" onclick="showManageTab('margin')">💰 Margen</button>
        <button class="mm-tab-btn" id="mm_btn_lev" onclick="showManageTab('lev')">⚡ Leverage</button>
      </div>

      <div id="mm_tab_tp">
        <div class="mm-field"><label>Calcular por ganancia deseada (USDT)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_tp_usdt" step="any" placeholder="ej. 7">
            <button class="mm-action" style="margin:0" onclick="mmCalcTPFromUsdt()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Calcular por ROI % deseado (sobre el margen)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_tp_roi" step="any" placeholder="ej. 50">
            <button class="mm-action" style="margin:0" onclick="mmCalcTPFromRoi()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Precio de disparo (Take Profit)</label><input type="number" id="mm_tp_price" step="any"></div>
        <button class="mm-action" onclick="mmSetTP()">Establecer TP</button>
      </div>
      <div id="mm_tab_sl" style="display:none">
        <div class="mm-field"><label>Calcular por pérdida máxima (USDT)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_sl_usdt" step="any" placeholder="ej. 5">
            <button class="mm-action" style="margin:0" onclick="mmCalcSLFromUsdt()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Calcular por ROI % de pérdida máxima (sobre el margen)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_sl_roi" step="any" placeholder="ej. 20">
            <button class="mm-action" style="margin:0" onclick="mmCalcSLFromRoi()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Precio de disparo (Stop Loss)</label><input type="number" id="mm_sl_price" step="any"></div>
        <button class="mm-action danger" onclick="mmSetSL()">Establecer SL</button>
        <button class="mm-action" style="background:#8b949e" onclick="mmCancelAllTpSl()">Cancelar TP/SL</button>
      </div>
      <div id="mm_tab_limit" style="display:none">
        <div class="mm-field"><label>Lado</label>
          <select id="mm_limit_side"><option value="BUY">BUY</option><option value="SELL">SELL</option></select>
        </div>
        <div class="mm-field"><label>Precio</label><input type="number" id="mm_limit_price" step="any"></div>
        <div class="mm-field"><label>Cantidad</label><input type="number" id="mm_limit_qty" step="any"></div>
        <div class="mm-field"><label><input type="checkbox" id="mm_limit_reduce"> Reduce Only (cerrar parcial)</label></div>
        <button class="mm-action" onclick="mmLimitOrder()">Enviar LIMIT</button>
      </div>
      <div id="mm_tab_margin" style="display:none">
        <div class="mm-field"><label>Monto (USDT)</label><input type="number" id="mm_margin_amount" step="any"></div>
        <button class="mm-action" onclick="mmModifyMargin(true)">➕ Añadir margen</button>
        <button class="mm-action danger" onclick="mmModifyMargin(false)">➖ Retirar margen</button>
        <div class="mm-field" style="margin-top:.8rem"><label>Tipo de margen (símbolo sin posición abierta)</label></div>
        <button class="mm-action" onclick="mmSetMarginType('ISOLATED')">ISOLATED</button>
        <button class="mm-action" onclick="mmSetMarginType('CROSSED')">CROSSED</button>
      </div>
      <div id="mm_tab_lev" style="display:none">
        <div class="mm-field"><label>Leverage para este símbolo (con escalera de respaldo 10x→4x)</label>
          <input type="number" id="mm_lev_input" min="1" max="125">
        </div>
        <button class="mm-action" onclick="mmSetSymbolLeverage()">Aplicar Leverage</button>
      </div>
    </div>
  </div>

  {dashboard_js}
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def start_http_server():
    app = web.Application()
    app.router.add_post("/signal", signal_handler)
    app.router.add_post("/manual/close", manual_close_handler)
    app.router.add_post("/manual/close_all", manual_close_all_handler)
    app.router.add_post("/manual/toggle_trading", manual_toggle_trading_handler)
    app.router.add_post("/manual/set_leverage", manual_set_leverage_handler)
    app.router.add_post("/manual/clear_history", manual_clear_history_handler)
    app.router.add_post("/manual/set_tp", manual_set_tp_handler)
    app.router.add_post("/manual/set_sl", manual_set_sl_handler)
    app.router.add_post("/manual/cancel_tp_sl", manual_cancel_tp_sl_handler)
    app.router.add_post("/manual/limit_order", manual_limit_order_handler)
    app.router.add_post("/manual/set_symbol_leverage", manual_set_symbol_leverage_handler)
    app.router.add_post("/manual/set_margin_type", manual_set_margin_type_handler)
    app.router.add_get("/manual/position_mode", manual_get_position_mode_handler)
    app.router.add_post("/manual/set_position_mode", manual_set_position_mode_handler)
    app.router.add_post("/manual/modify_margin", manual_modify_margin_handler)
    app.router.add_get("/manual/orders", manual_get_orders_handler)
    app.router.add_get("/", dashboard_handler)
    app.router.add_get("/health", dashboard_handler)
    app.router.add_get("/api/state", api_state_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Executor HTTP activo en http://0.0.0.0:{PORT}")


async def main():
    global execution_manager

    env_tag = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Futures Executor WS — Binance USDT Perpetuos       ║")
    log.info(f"║   Entorno: {env_tag:<44}║")
    log.info(f"║   Leverage: {LEVERAGE}x | Poll cierre ext.: {POSITION_POLL_S}s              ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.critical("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")
        return

    try:
        try:
            from WS import SymbolWebSocketPriceCache
        except ImportError:
            from ws import SymbolWebSocketPriceCache
    except ImportError:
        log.critical("No se puede importar SymbolWebSocketPriceCache desde ws.py / WS.py")
        return

    try:
        api = BinanceAPI(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        price_ws = SymbolWebSocketPriceCache([])
        price_ws.start()
        execution_manager = ExecutionManager(api, price_ws)
        if HEDGE_MODE:
            log.warning("HEDGE_MODE=true: asegúrate de que tu cuenta esté en Hedge Mode.")
    except Exception as e:
        log.critical(f"Error inicializando BinanceAPI WS / precios: {e}")
        return

    await execution_manager.refresh_balance()
    log.info(f"Balance USDT Futures: ${execution_manager.balance:.2f}")

    async with aiohttp.ClientSession() as sess:
        await send_telegram(
            sess,
            f"⚡ <b>Futures Executor WS iniciado</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance USDT:</b> <code>{execution_manager.balance:.2f} USDT</code>\n"
            f"⚡ <b>Leverage:</b> <code>{LEVERAGE}x</code>\n"
            f"📡 <b>Órdenes:</b> WebSocket API\n"
            f"📡 <b>Precios:</b> WebSocket (ws.py)\n"
            f"🔒 <b>Cierre:</b> señal explícita o botón manual\n"
            f"⚠️ <b>Error -2019:</b> posición puede registrarse como asumida",
        )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            start_http_server(),
            price_sync_loop(),
            position_monitor_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
