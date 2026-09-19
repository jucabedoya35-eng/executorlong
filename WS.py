"""
WS.py — Caché de precios por WebSocket para Binance USDⓈ-M Futures.

REESCRITO para eliminar la latencia de suscripción que provocaba
"sin precio WS tras esperar" al abrir una posición en un símbolo nuevo.

Qué cambió, en una línea: antes, añadir un símbolo CERRABA las dos
conexiones y las reabría desde cero (con backoff que llegaba a 60 s);
ahora se manda un SUBSCRIBE por el socket que ya está abierto, y además
hay un stream de mercado completo que mantiene un precio disponible para
CUALQUIER símbolo desde el segundo cero.

Streams:
  • !markPrice@arr        → mark price de TODOS los símbolos (cada 3 s).
                            Es lo que hace que un símbolo nuevo tenga
                            precio de inmediato, sin esperar suscripción.
                            Desactivable con WS_ALL_MARKET=false.
  • <symbol>@markPrice@1s → 1 s de frescura para los símbolos seguidos.
  • <symbol>@ticker       → cambio 24 h, high/low, volumen.

API pública (compatible con la versión anterior):
  start() / stop()
  update_symbols(symbols)            — reemplaza la lista seguida
  ensure_symbols(symbols)            — AÑADE sin quitar (no destructivo)
  get_price(symbol, max_age_s=None)
  wait_for_price(symbol, timeout)    — bloqueante, con Condition
  get_all_prices() / get_ticker() / get_change_24h() / get_all_tickers()
  get_all_changes_24h() / get_stale_symbols() / get_stats()
"""

import asyncio
import websockets
import json
import threading
import time
import math
from datetime import datetime
from typing import NamedTuple, Iterable, Optional
import os


# ── Helpers de módulo ──────────────────────────────────────────────────────

def _safe_float(d: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except (ValueError, TypeError):
        return default


class TickerData(NamedTuple):
    change_pct: float   # % cambio 24h  (campo 'P')
    change_abs: float   # cambio abs 24h (campo 'p')
    last_price: float   # último precio  (campo 'c')
    high_24h:   float   # máximo 24h     (campo 'h')
    low_24h:    float   # mínimo 24h     (campo 'l')
    volume_24h: float   # volumen base   (campo 'v')
    quote_vol:  float   # volumen cotizado(campo 'q')
    ts:         float   # timestamp UNIX


# Límite de Binance: 1024 streams por conexión combinada.
_BINANCE_MAX_STREAMS = 1024

# Stream de mercado completo. Con "@1s" refresca cada segundo pero manda
# ~90 KB por mensaje (todos los símbolos); sin sufijo refresca cada 3 s,
# que sigue estando muy por debajo de MAX_PRICE_AGE_S (5 s) del executor
# y consume un tercio del ancho de banda. Los símbolos con posición
# abierta tienen además su propio stream a 1 s, así que la frescura donde
# importa no se pierde.
_ALL_MARKET_ENABLED  = os.environ.get("WS_ALL_MARKET", "true").lower() == "true"
_ALL_MARKET_INTERVAL = os.environ.get("WS_ALL_MARKET_INTERVAL", "3s").lower()
_ALL_MARKET_STREAM   = "!markPrice@arr@1s" if _ALL_MARKET_INTERVAL == "1s" else "!markPrice@arr"

_WS_BASE_URL = os.environ.get("WS_FSTREAM_URL", "wss://fstream.binance.com/stream")


class SymbolWebSocketPriceCache:
    """Caché de mark price + ticker 24 h por WebSocket, con suscripción
    dinámica en caliente (sin reconectar).

    Arreglo central respecto a la versión anterior
    ----------------------------------------------
    `update_symbols()` ya NO cierra las conexiones. Binance acepta
    comandos SUBSCRIBE / UNSUBSCRIBE por el mismo socket ya abierto, que
    se resuelven en milisegundos. Antes, cada símbolo nuevo disparaba:

        update_symbols → _force_reconnect → ws.close()
                       → _reconnect_loop lo veía como EXCEPCIÓN
                       → consecutive_errors += 1 → backoff

    y como `consecutive_errors` sólo se reseteaba si `_connect()`
    terminaba SIN excepción (algo que en operación normal no pasa nunca,
    porque el bucle interno sólo sale lanzando ConnectionClosed), a
    partir de la quinta posición abierta en la vida del proceso CADA
    resuscripción esperaba 60 segundos antes de reconectar. De ahí el
    "sin precio WS tras esperar" con los 10 s de margen del executor.
    """

    _WS_MAX_SIZE  = 8 * 1024 * 1024   # 8 MB: !markPrice@arr con ~500 símbolos ronda los 90 KB
    _WS_MAX_QUEUE = 256

    def __init__(self, symbols: list[str], symbols_per_connection: int | None = None):
        if symbols_per_connection is not None:
            print(
                f"[WS] ⚠️  symbols_per_connection={symbols_per_connection} ignorado — "
                f"todos los símbolos van en una sola conexión (límite Binance: 1024)."
            )

        self.symbols = sorted({s.upper() for s in symbols})
        self._check_limit(self.symbols)

        # price_cache:  symbol -> (mark_price, timestamp)
        self.price_cache:  dict[str, tuple[float, float]] = {}
        # ticker_cache: symbol -> TickerData (incluye ts)
        self.ticker_cache: dict[str, TickerData] = {}

        self.tasks: list = []
        # RLock en vez de Lock: get_stats() y otros helpers se llaman
        # entre sí y un Lock simple se autobloquearía al anidarse.
        self.lock = threading.RLock()
        # Condition sobre el mismo lock: permite que wait_for_price()
        # despierte en cuanto ENTRA el tick, en vez de sondear cada
        # 200 ms como hacía el executor (que en el peor caso añadía
        # 200 ms de retraso a cada apertura).
        self._price_cv = threading.Condition(self.lock)

        self.running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Socket único y estado de suscripción
        self._ws = None
        self._ws_ready = threading.Event()
        self._subscribed: set[str] = set()   # nombres de stream ya suscritos
        self._req_id = 0

        self.connection_stats: dict[str, dict] = {
            "stream": {"reconnects": 0, "last_error": None, "connected_since": None},
        }

    # ──────────────────────────────────────────────────────────────────────
    # Utilidades internas
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _check_limit(symbols: list[str]):
        # 2 streams por símbolo (markPrice + ticker) + 1 del mercado completo.
        needed = len(symbols) * 2 + (1 if _ALL_MARKET_ENABLED else 0)
        if needed > _BINANCE_MAX_STREAMS:
            raise ValueError(
                f"Binance permite máximo {_BINANCE_MAX_STREAMS} streams por conexión. "
                f"{len(symbols)} símbolos necesitan {needed}. Divide en dos instancias."
            )

    @staticmethod
    def _streams_for(symbol: str) -> list[str]:
        s = symbol.lower()
        return [f"{s}@markPrice@1s", f"{s}@ticker"]

    def _desired_streams(self) -> set[str]:
        with self.lock:
            symbols_snapshot = list(self.symbols)
        wanted: set[str] = set()
        if _ALL_MARKET_ENABLED:
            wanted.add(_ALL_MARKET_STREAM)
        for sym in symbols_snapshot:
            wanted.update(self._streams_for(sym))
        return wanted

    async def _send_command(self, method: str, params: list[str]):
        """Manda SUBSCRIBE / UNSUBSCRIBE por el socket ya abierto."""
        if not params or self._ws is None:
            return
        self._req_id += 1
        payload = {"method": method, "params": sorted(params), "id": self._req_id}
        # Binance limita el tamaño del mensaje; se trocea por si la lista
        # de símbolos es muy larga.
        chunk = 200
        for i in range(0, len(payload["params"]), chunk):
            part = dict(payload)
            part["params"] = payload["params"][i:i + chunk]
            self._req_id += 1
            part["id"] = self._req_id
            await self._ws.send(json.dumps(part))

    async def _sync_subscriptions(self):
        """Alinea las suscripciones del socket con la lista de símbolos
        actual, mandando sólo las diferencias. Esto es lo que sustituye
        al ciclo cerrar/reabrir conexión de la versión anterior."""
        if self._ws is None:
            return
        wanted = self._desired_streams()
        to_add = sorted(wanted - self._subscribed)
        to_del = sorted(self._subscribed - wanted)

        if to_add:
            await self._send_command("SUBSCRIBE", to_add)
            self._subscribed |= set(to_add)
            print(f"➕ [WS] SUBSCRIBE {len(to_add)} stream(s): {to_add[:4]}{'…' if len(to_add) > 4 else ''}")
        if to_del:
            await self._send_command("UNSUBSCRIBE", to_del)
            self._subscribed -= set(to_del)
            print(f"➖ [WS] UNSUBSCRIBE {len(to_del)} stream(s)")

    # ──────────────────────────────────────────────────────────────────────
    # Procesamiento de mensajes
    # ──────────────────────────────────────────────────────────────────────

    def _store_price(self, symbol: str, price: float, now: float):
        if not symbol or not math.isfinite(price) or price <= 0:
            return
        with self._price_cv:
            self.price_cache[symbol] = (price, now)
            # Despierta a quien esté esperando este símbolo en
            # wait_for_price(): la apertura arranca en cuanto llega el
            # tick, no en el siguiente sondeo.
            self._price_cv.notify_all()

    def _handle_payload(self, data: dict):
        now = time.time()

        # Respuesta a SUBSCRIBE/UNSUBSCRIBE: {"result": null, "id": N}
        if "result" in data and "stream" not in data:
            if data.get("result") not in (None, []):
                print(f"🔶 [WS] Respuesta inesperada a comando: {data}")
            return

        payload = data.get("data", data)

        # !markPrice@arr llega como LISTA de objetos, uno por símbolo.
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                self._store_price(str(item.get("s", "")).upper(), _safe_float(item, "p"), now)
            return

        if not isinstance(payload, dict):
            return

        event = payload.get("e")
        symbol = str(payload.get("s", "")).upper()

        if event == "markPriceUpdate":
            self._store_price(symbol, _safe_float(payload, "p"), now)
        elif event == "24hrTicker":
            if not symbol:
                return
            with self.lock:
                self.ticker_cache[symbol] = TickerData(
                    change_pct=_safe_float(payload, "P"),
                    change_abs=_safe_float(payload, "p"),
                    last_price=_safe_float(payload, "c"),
                    high_24h=_safe_float(payload, "h"),
                    low_24h=_safe_float(payload, "l"),
                    volume_24h=_safe_float(payload, "v"),
                    quote_vol=_safe_float(payload, "q"),
                    ts=now,
                )

    # ──────────────────────────────────────────────────────────────────────
    # Conexión única con reconexión
    # ──────────────────────────────────────────────────────────────────────

    def _ws_connect(self):
        return websockets.connect(
            _WS_BASE_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=self._WS_MAX_SIZE,
            max_queue=self._WS_MAX_QUEUE,
            compression="deflate",   # recorta mucho el tráfico de !markPrice@arr
        )

    async def _stream_loop(self):
        """Mantiene UNA conexión viva. El backoff sólo crece ante fallos
        reales y se resetea en cuanto la conexión se sostiene: antes,
        `consecutive_errors` no se reseteaba nunca en operación normal y
        el retardo se quedaba clavado en 60 s."""
        reconnect_delay = 1.0
        consecutive_errors = 0

        while self.running:
            connected_at = 0.0
            try:
                async with self._ws_connect() as ws:
                    self._ws = ws
                    self._subscribed = set()
                    connected_at = time.time()
                    self.connection_stats["stream"]["connected_since"] = connected_at

                    await self._sync_subscriptions()
                    self._ws_ready.set()
                    print(
                        f"✅ [WS] Conectado — {len(self._subscribed)} stream(s)"
                        f"{' (incluye mercado completo)' if _ALL_MARKET_ENABLED else ''}"
                    )

                    # Reset inmediato: la conexión se estableció y las
                    # suscripciones se aceptaron, así que lo anterior ya
                    # no cuenta como racha de errores.
                    reconnect_delay = 1.0
                    consecutive_errors = 0

                    while self.running:
                        msg = await ws.recv()
                        try:
                            self._handle_payload(json.loads(msg))
                        except json.JSONDecodeError:
                            continue

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._ws_ready.clear()
                self._ws = None
                self.connection_stats["stream"]["last_error"] = str(e)
                self.connection_stats["stream"]["reconnects"] += 1

                if not self.running:
                    break

                # Si la conexión había aguantado un rato, se trata como
                # un corte aislado (reconexión inmediata) y no como parte
                # de una racha: reabrir rápido es justo lo que hace falta.
                if connected_at and (time.time() - connected_at) > 30:
                    consecutive_errors = 0
                    reconnect_delay = 1.0
                else:
                    consecutive_errors += 1
                    reconnect_delay = min(reconnect_delay * 1.5, 15.0)

                print(f"🔴 [WS] Error: {e} — reconectando en {reconnect_delay:.1f}s (racha #{consecutive_errors})")
                await asyncio.sleep(reconnect_delay)
            finally:
                self._ws_ready.clear()
                self._ws = None

    # ──────────────────────────────────────────────────────────────────────
    # Monitor de salud
    # ──────────────────────────────────────────────────────────────────────

    async def _monitor_health(self):
        while self.running:
            await asyncio.sleep(60)
            now = time.time()
            stale_price, stale_ticker = [], []

            with self.lock:
                symbols_snapshot = list(self.symbols)
                for symbol in symbols_snapshot:
                    entry = self.price_cache.get(symbol)
                    if entry is None or now - entry[1] > 120:
                        stale_price.append(symbol)
                    ticker = self.ticker_cache.get(symbol)
                    if ticker is None or now - ticker.ts > 120:
                        stale_ticker.append(symbol)

            if stale_price:
                print(f"⚠️ [markPrice] Sin actualización ({len(stale_price)}): "
                      f"{stale_price[:5]}{'…' if len(stale_price) > 5 else ''}")
            if stale_ticker:
                print(f"⚠️ [ticker24h] Sin actualización ({len(stale_ticker)}): "
                      f"{stale_ticker[:5]}{'…' if len(stale_ticker) > 5 else ''}")

            # Red de seguridad: si algún símbolo seguido quedó sin
            # suscribir (p.ej. un SUBSCRIBE perdido), se re-sincroniza
            # sin tocar la conexión.
            if stale_price and self._ws is not None and self._loop is not None:
                try:
                    await self._sync_subscriptions()
                except Exception as e:
                    print(f"⚠️ [WS] re-sync de suscripciones falló: {e}")

    # ──────────────────────────────────────────────────────────────────────
    # Actualización dinámica de símbolos (sin reconectar)
    # ──────────────────────────────────────────────────────────────────────

    def _apply_symbols(self, new_symbols: list[str], purge_removed: bool):
        with self.lock:
            if new_symbols == self.symbols:
                return False
            removed = set(self.symbols) - set(new_symbols)
            self.symbols = new_symbols
            if purge_removed and not _ALL_MARKET_ENABLED:
                # Sólo hace falta purgar si NO hay stream de mercado
                # completo. Con él, el símbolo sigue recibiendo precio
                # aunque deje de seguirse, así que el dato nunca se queda
                # congelado y purgarlo sería perder información útil.
                for sym in removed:
                    self.price_cache.pop(sym, None)
                    self.ticker_cache.pop(sym, None)
        return True

    def _schedule_sync(self):
        """Programa la re-sincronización de suscripciones en el loop del
        WS. Seguro desde cualquier hilo. NO cierra la conexión."""
        if not self.running or self._loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(self._sync_subscriptions(), self._loop)
        except Exception as e:
            print(f"⚠️ [WS] no se pudo programar la suscripción: {e}")

    def update_symbols(self, symbols: list[str]):
        """Reemplaza la lista de símbolos seguidos y sincroniza las
        suscripciones EN CALIENTE (SUBSCRIBE/UNSUBSCRIBE), sin cerrar la
        conexión ni afectar a los precios de los demás símbolos."""
        new_symbols = sorted({s.upper() for s in symbols})
        self._check_limit(new_symbols)
        if not self._apply_symbols(new_symbols, purge_removed=True):
            return
        preview = new_symbols[:5]
        print(f"🔄 [WS] Símbolos seguidos → {len(new_symbols)} "
              f"({preview}{'…' if len(new_symbols) > 5 else ''})")
        self._schedule_sync()

    def ensure_symbols(self, symbols: Iterable[str]):
        """Añade símbolos a la lista seguida SIN quitar ninguno.

        Es lo que debe usar el executor al resolver el precio de entrada:
        `update_symbols` con una lista incompleta daría de baja los
        símbolos de las posiciones abiertas que no estuvieran en ella.
        """
        extra = {s.upper() for s in symbols}
        with self.lock:
            merged = sorted(set(self.symbols) | extra)
        self._check_limit(merged)
        if not self._apply_symbols(merged, purge_removed=False):
            return
        self._schedule_sync()

    # ──────────────────────────────────────────────────────────────────────
    # Getters – markPrice
    # ──────────────────────────────────────────────────────────────────────

    def get_price(self, symbol: str, max_age_s: float | None = None) -> float | None:
        """Último mark price cacheado. Si se pasa `max_age_s`, un precio
        más viejo que ese umbral se considera no confiable y devuelve
        None, para que el llamador no opere con un dato obsoleto."""
        with self.lock:
            entry = self.price_cache.get(symbol.upper())
            if not entry:
                return None
            price, ts = entry
            if max_age_s is not None and (time.time() - ts) > max_age_s:
                return None
            return price

    def wait_for_price(self, symbol: str, timeout: float = 5.0,
                       max_age_s: float | None = None) -> float | None:
        """Espera BLOQUEANTE hasta que haya un precio fresco del símbolo.

        Despierta en cuanto entra el tick (Condition), no por sondeo.
        Pensado para llamarse desde el executor con `asyncio.to_thread`,
        para no bloquear su event loop.
        """
        symbol = symbol.upper()
        deadline = time.time() + timeout
        self.ensure_symbols([symbol])
        with self._price_cv:
            while True:
                entry = self.price_cache.get(symbol)
                if entry:
                    price, ts = entry
                    if max_age_s is None or (time.time() - ts) <= max_age_s:
                        return price
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                self._price_cv.wait(timeout=min(remaining, 0.5))

    def get_price_age(self, symbol: str) -> float | None:
        """Antigüedad en segundos del precio cacheado, o None si no hay."""
        with self.lock:
            entry = self.price_cache.get(symbol.upper())
            return (time.time() - entry[1]) if entry else None

    def get_all_prices(self) -> dict[str, float]:
        with self.lock:
            return {sym: v[0] for sym, v in self.price_cache.items()}

    # ──────────────────────────────────────────────────────────────────────
    # Getters – Ticker 24h
    # ──────────────────────────────────────────────────────────────────────

    def get_change_24h(self, symbol: str) -> float | None:
        with self.lock:
            t = self.ticker_cache.get(symbol.upper())
            return t.change_pct if t else None

    def get_ticker(self, symbol: str) -> dict | None:
        with self.lock:
            t = self.ticker_cache.get(symbol.upper())
            if t is None:
                return None
            return {
                "change_pct": t.change_pct,
                "change_abs": t.change_abs,
                "last_price": t.last_price,
                "high_24h":   t.high_24h,
                "low_24h":    t.low_24h,
                "volume_24h": t.volume_24h,
                "quote_vol":  t.quote_vol,
            }

    def get_all_changes_24h(self) -> dict[str, float]:
        with self.lock:
            result = {sym: t.change_pct for sym, t in self.ticker_cache.items()}
        return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))

    def get_all_tickers(self) -> dict[str, dict]:
        with self.lock:
            return {
                sym: {
                    "change_pct": t.change_pct,
                    "change_abs": t.change_abs,
                    "last_price": t.last_price,
                    "high_24h":   t.high_24h,
                    "low_24h":    t.low_24h,
                    "volume_24h": t.volume_24h,
                    "quote_vol":  t.quote_vol,
                }
                for sym, t in self.ticker_cache.items()
            }

    # ──────────────────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────────────────

    def get_stale_symbols(self, max_age_seconds: int = 60) -> list[str]:
        now = time.time()
        with self.lock:
            return [
                s for s in self.symbols
                if now - self.price_cache.get(s, (0, 0))[1] > max_age_seconds
            ]

    def is_connected(self) -> bool:
        return self._ws_ready.is_set()

    def wait_until_connected(self, timeout: float = 10.0) -> bool:
        """Bloquea hasta que el socket esté listo y suscrito. Útil al
        arrancar, para no mandar la primera señal a ciegas."""
        return self._ws_ready.wait(timeout=timeout)

    def get_stats(self) -> dict:
        with self.lock:
            active_prices = len(self.price_cache)
            active_tickers = len(self.ticker_cache)
            total_symbols = len(self.symbols)
            subscribed = len(self._subscribed)

        return {
            "total_symbols":     total_symbols,
            "active_prices":     active_prices,
            "active_tickers":    active_tickers,
            "subscribed_streams": subscribed,
            "all_market_stream": _ALL_MARKET_STREAM if _ALL_MARKET_ENABLED else None,
            "connected":         self.is_connected(),
            "stale_symbols":     len(self.get_stale_symbols()),
            "connection_stats":  self.connection_stats,
        }

    # ──────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ──────────────────────────────────────────────────────────────────────

    def start(self):
        """Inicia la conexión WebSocket en un hilo propio."""
        if self.running:
            return
        self.running = True

        loop = asyncio.new_event_loop()
        self._loop = loop
        threading.Thread(target=loop.run_forever, daemon=True,
                         name="ws-price-cache").start()

        def submit(coro):
            self.tasks.append(asyncio.run_coroutine_threadsafe(coro, loop))

        submit(self._stream_loop())
        submit(self._monitor_health())

        print(
            f"✅ WebSocket cache iniciado — {len(self.symbols)} símbolo(s) seguidos, "
            f"1 conexión{', mercado completo ' + _ALL_MARKET_STREAM if _ALL_MARKET_ENABLED else ''}"
        )

    def stop(self):
        print("🛑 Deteniendo WebSocket cache…")
        self.running = False

        ws_obj = self._ws
        if ws_obj is not None and self._loop is not None:
            try:
                asyncio.run_coroutine_threadsafe(ws_obj.close(), self._loop).result(timeout=5)
            except Exception:
                pass

        for task in self.tasks:
            try:
                task.cancel()
            except Exception:
                pass

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        with self._price_cv:
            self._price_cv.notify_all()

        print("✅ WebSocket cache detenido")


# ══════════════════════════════════════════════════════════════════════════
# Ejemplo / prueba rápida
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    cache = SymbolWebSocketPriceCache([])
    cache.start()

    print("⏳ Esperando conexión…")
    cache.wait_until_connected(timeout=15)

    # Prueba del caso que fallaba: pedir un símbolo que NO estaba seguido
    # y medir cuánto tarda en haber precio.
    for sym in ("BMTUSDT", "BTCUSDT", "DOGEUSDT"):
        t0 = time.time()
        price = cache.wait_for_price(sym, timeout=10, max_age_s=5)
        dt = (time.time() - t0) * 1000
        print(f"  {sym:<12} → {price}   ({dt:.0f} ms)")

    try:
        while True:
            time.sleep(5)
            s = cache.get_stats()
            now = datetime.now().strftime("%H:%M:%S")
            print(f"[{now}] conectado={s['connected']} precios={s['active_prices']} "
                  f"streams={s['subscribed_streams']} obsoletos={s['stale_symbols']} "
                  f"reconexiones={s['connection_stats']['stream']['reconnects']}")
    except KeyboardInterrupt:
        cache.stop()
