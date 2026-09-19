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
import time
from datetime import datetime, timezone
import os
import json
import uuid
import hmac
import hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation, getcontext
from typing import Optional

# Precisión amplia para todo el cálculo de cantidades/precios: se trabaja
# con Decimal (no con float) para que el redondeo al stepSize sea EXACTO.
# Este es el origen del bug de "aproxima hacia abajo": con float,
# 5.1/0.001 puede dar 5099.999999999999 y cualquier truncado posterior
# se come un step entero, dejando el notional por debajo del mínimo.
getcontext().prec = 28

# ══════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "j65vqKTAEvJtOZMCQbSiH5GZXfzyg1W70dWvhnb5DHxMOlLaW1JlrohJtYf8hJMH")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "qBqVSu0b0stLoN5hWEo5TAeK0IyfI4bNP1kQh7X3JoXVlzBOVutMSr0CWtvTua0O")
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
BALANCE_POLL_S  = int(os.environ.get("BALANCE_POLL_S", "60"))

MIN_NOTIONAL_USDT = float(os.environ.get("MIN_NOTIONAL_USDT", "5.1"))
# Colchón por encima del notional mínimo. Se sube de 2% a 5% porque
# Binance valida el notional de una MARKET contra el precio de ejecución
# (no contra el precio con el que el bot calculó la cantidad): con 2% una
# vela adversa de ~3% ya dejaba la orden por debajo de 5 USDT y disparaba
# el rechazo -4164.
NOTIONAL_SAFETY_BUFFER_PCT = float(os.environ.get("NOTIONAL_SAFETY_BUFFER_PCT", "5.0"))
MAX_PRICE_AGE_S = float(os.environ.get("MAX_PRICE_AGE_S", "5.0"))
MIN_VALID_PRICE = 0.00001

# ── Filtros reales por símbolo (LOT_SIZE / MIN_NOTIONAL / PRICE_FILTER) ──
# Se leen de /fapi/v1/exchangeInfo, que es un endpoint PÚBLICO (sin firma
# ni API key) y de peso 1. Se consulta UNA sola vez al arrancar y se
# refresca cada EXCHANGE_INFO_TTL_S segundos, así que no tiene ningún
# impacto real en el rate limit (a diferencia de consultarlo por símbolo
# en cada señal, que era lo que se quiso evitar al eliminarlo).
# Si falla o se desactiva, se cae al default seguro de siempre.
USE_EXCHANGE_INFO   = os.environ.get("USE_EXCHANGE_INFO", "true").lower() == "true"
EXCHANGE_INFO_TTL_S = float(os.environ.get("EXCHANGE_INFO_TTL_S", "21600"))  # 6 h

WS_API_URL = os.environ.get(
    "BINANCE_WS_FAPI_URL",
    "wss://testnet.binancefuture.com/ws-fapi/v1" if USE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1",
)

REST_FAPI_URL = os.environ.get(
    "BINANCE_REST_FAPI_URL",
    "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com",
)

_DEFAULT_PROXY_URLS = [
    "http://fixie:CuLSweHyTOG4Lg3@54.195.3.54:80",
    "http://fixie:CuLSweHyTOG4Lg3@54.217.142.99:80",
]

# BUG CORREGIDO: antes, la lista de arriba se descartaba SIEMPRE — si
# PROXY_URLS no estaba en el entorno se caía al FIXIE_URL legacy y las
# dos IPs fijas nunca se usaban (perdiendo el failover entre IPs).
# Orden de prioridad: PROXY_URLS (env) → FIXIE_URL (env) → lista default.
_raw_proxy_urls = os.environ.get("PROXY_URLS", "").strip()
if _raw_proxy_urls:
    PROXY_URLS = [u.strip() for u in _raw_proxy_urls.split(",") if u.strip()]
else:
    # Retrocompatibilidad: si solo existe FIXIE_URL (una única salida),
    # se usa como único elemento de la lista.
    _legacy_fixie = os.environ.get("FIXIE_URL", "").strip()
    PROXY_URLS = [_legacy_fixie] if _legacy_fixie else list(_DEFAULT_PROXY_URLS)

# Se mantiene por compatibilidad con el resto del código/dashboard que
# solo necesita saber "¿hay algún proxy configurado?".
FIXIE_URL = PROXY_URLS[0] if PROXY_URLS else ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Executor")


# ══════════════════════════════════════════════════════════
#  MULTIPLICADOR DE TAMAÑO POR NIVELES (con histéresis)
# ══════════════════════════════════════════════════════════
# Escala el `quantity` de cada señal según un valor de referencia (el
# balance en USDT, real o ficticio) subiendo de nivel cada MULTIPLIER_STEP_USDT
# y bajando solo si el valor cae por debajo de la MITAD del umbral de
# entrada del nivel actual — así, un valor que oscila cerca de un umbral
# (p.ej. 200) no hace parpadear el multiplicador entre x1 y x2.
MULTIPLIER_STEP_USDT = float(os.environ.get("MULTIPLIER_STEP_USDT", "100"))
MULTIPLIER_MAX_LEVEL = int(os.environ.get("MULTIPLIER_MAX_LEVEL", "50"))
MULTIPLIER_MODE_DEFAULT = os.environ.get("MULTIPLIER_MODE", "auto").lower()  # "auto" | "manual"
MULTIPLIER_BALANCE_SOURCE_DEFAULT = os.environ.get("MULTIPLIER_BALANCE_SOURCE", "real").lower()  # "real" | "ficticio"
MULTIPLIER_MANUAL_DEFAULT = float(os.environ.get("MULTIPLIER_MANUAL_VALUE", "1"))
MULTIPLIER_FICTITIOUS_DEFAULT = float(os.environ.get("MULTIPLIER_FICTITIOUS_BALANCE", "0"))


class PositionMultiplier:
    """
    Multiplicador de tamaño de posición por NIVELES con histéresis.

    Reglas (con MULTIPLIER_STEP_USDT=100 por defecto):
      - Nivel 1 → x1: nivel base, siempre disponible (valor >= 0).
      - Nivel N (N>=2) → xN: se ENTRA cuando el valor alcanza N*step
        (nivel 2 → 200, nivel 3 → 300, nivel 4 → 400, ...).
      - Una vez dentro del nivel N, se BAJA a N-1 solo si el valor cae
        por debajo de la MITAD del umbral de entrada de N, es decir
        N*step/2 (nivel 2 exige caer por debajo de 100 para bajar a
        nivel 1; nivel 3 exige caer por debajo de 150 para bajar a
        nivel 2; y así sucesivamente).
      - La comprobación es en cascada nivel a nivel: una subida o
        bajada brusca del valor puede mover varios niveles de golpe.

    También soporta:
      - Modo AUTO (se calcula solo, como arriba) o MANUAL (el usuario
        fija el multiplicador directamente desde el dashboard/API).
      - Fuente del valor de referencia: balance REAL (el de
        ExecutionManager, consultado a Binance) o balance FICTICIO (un
        número fijado a mano desde el dashboard, para poder probar el
        comportamiento del multiplicador sin arriesgar dinero real).
    """

    def __init__(self):
        self.step: float = MULTIPLIER_STEP_USDT
        self.max_level: int = MULTIPLIER_MAX_LEVEL
        self.mode: str = MULTIPLIER_MODE_DEFAULT if MULTIPLIER_MODE_DEFAULT in ("auto", "manual") else "auto"
        self.balance_source: str = (
            MULTIPLIER_BALANCE_SOURCE_DEFAULT
            if MULTIPLIER_BALANCE_SOURCE_DEFAULT in ("real", "ficticio")
            else "real"
        )
        self.manual_value: float = max(MULTIPLIER_MANUAL_DEFAULT, 0.0)
        self.fictitious_balance: float = max(MULTIPLIER_FICTITIOUS_DEFAULT, 0.0)
        self._level: int = 1
        # Niveles independientes para el multiplicador REAL (intento
        # principal, el que se manda de verdad a Binance) y el
        # multiplicador FICTICIO (fallback cuando hay margen insuficiente).
        # Se mantienen aparte porque el balance real y el ficticio
        # evolucionan de forma independiente, y cada uno necesita su
        # propia histéresis de subida/bajada de nivel.
        self._level_real: int = 1
        self._level_ficticio: int = 1
        self._lock = asyncio.Lock()

    def _entry_threshold(self, level: int) -> float:
        return level * self.step

    def _exit_threshold(self, level: int) -> float:
        return (level * self.step) / 2.0

    def _recompute_level(self, level: int, value: float) -> int:
        value = max(value, 0.0)
        while level < self.max_level and value >= self._entry_threshold(level + 1):
            level += 1
        while level > 1 and value < self._exit_threshold(level):
            level -= 1
        return level

    def reference_value(self, real_balance: float) -> float:
        """Balance a usar como referencia según la fuente elegida."""
        if self.balance_source == "ficticio":
            return self.fictitious_balance
        return real_balance

    async def current_multiplier(self, real_balance: float) -> float:
        """
        Multiplicador EFECTIVO a aplicar ahora mismo sobre el `quantity`
        de una señal. El nivel automático se recalcula siempre (incluso
        en modo manual, para que al volver a AUTO no arranque en 1), pero
        en modo manual se devuelve directamente `manual_value`.
        """
        async with self._lock:
            value = self.reference_value(real_balance)
            self._level = self._recompute_level(self._level, value)
            if self.mode == "manual":
                return self.manual_value
            return float(self._level)

    async def current_multiplier_real(self, real_balance: float) -> float:
        """
        Multiplicador para el intento PRINCIPAL de apertura: la orden que
        se manda de verdad a Binance. Se calcula SIEMPRE sobre el balance
        REAL (sin importar lo que tenga configurado `balance_source`),
        con su propio nivel de histéresis independiente (`_level_real`).
        """
        async with self._lock:
            self._level_real = self._recompute_level(self._level_real, real_balance)
            if self.mode == "manual":
                return self.manual_value
            return float(self._level_real)

    async def current_multiplier_ficticio(self) -> float:
        """
        Multiplicador de RESPALDO. Se usa únicamente cuando Binance
        rechaza la apertura real por margen insuficiente (-2019): en ese
        caso NO se manda ninguna otra orden a Binance — la posición se
        registra localmente como "asumida" (paper trade) con la quantity
        que resulte de este multiplicador, calculado sobre el balance
        FICTICIO configurado desde el dashboard, con su propio nivel de
        histéresis independiente (`_level_ficticio`).
        """
        async with self._lock:
            self._level_ficticio = self._recompute_level(self._level_ficticio, self.fictitious_balance)
            if self.mode == "manual":
                return self.manual_value
            return float(self._level_ficticio)

    def snapshot(self, real_balance: float) -> dict:
        value = self.reference_value(real_balance)
        return {
            "mode": self.mode,
            "balance_source": self.balance_source,
            "manual_value": self.manual_value,
            "fictitious_balance": self.fictitious_balance,
            "step": self.step,
            "level": self._level,
            "reference_value": value,
            "effective_multiplier": self.manual_value if self.mode == "manual" else float(self._level),
            "next_level_at": self._entry_threshold(self._level + 1) if self._level < self.max_level else None,
            "drop_level_at": self._exit_threshold(self._level) if self._level > 1 else None,
            # Multiplicador REAL (intento principal enviado a Binance) y
            # multiplicador FICTICIO (fallback solo ante margen
            # insuficiente), cada uno con su propio nivel independiente.
            "level_real": self._level_real,
            "effective_multiplier_real": self.manual_value if self.mode == "manual" else float(self._level_real),
            "level_ficticio": self._level_ficticio,
            "effective_multiplier_ficticio": self.manual_value if self.mode == "manual" else float(self._level_ficticio),
        }


position_multiplier = PositionMultiplier()


# ══════════════════════════════════════════════════════════
#  AJUSTE DE CANTIDAD / NOTIONAL MÍNIMO
# ══════════════════════════════════════════════════════════
def _to_decimal(value, default: str = "0") -> Decimal:
    """Convierte cualquier cosa (float, int, str de Binance) a Decimal sin
    arrastrar el ruido binario del float: se pasa siempre por `repr`, que
    da la representación decimal más corta que reproduce el float."""
    if isinstance(value, Decimal):
        return value
    try:
        if isinstance(value, float):
            return Decimal(repr(value))
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def _plain_decimal(value) -> str:
    """Texto decimal plano (nunca notación científica) para mandar a
    Binance. `str(1e-05)` devuelve "1e-05" y la API lo rechaza con -1111;
    esto devuelve "0.00001"."""
    if isinstance(value, str):
        # Ya viene formateado por format_qty / round_price_to_tick.
        return value
    d = _to_decimal(value)
    return format(d.normalize(), "f")


def _step_decimals(step) -> int:
    """Cantidad de decimales implicada por un stepSize (p.ej. 0.001 -> 3).

    Se calcula con Decimal y normalize(), así funciona igual reciba el
    step como float (0.001), como int (1) o como el string que manda
    Binance en exchangeInfo ("0.00100000").
    """
    d = _to_decimal(step)
    if d <= 0:
        return 8
    exponent = d.normalize().as_tuple().exponent
    return max(0, -int(exponent))


def floor_to_step(value, step) -> Decimal:
    """Baja `value` al múltiplo de `step` inmediatamente inferior (exacto)."""
    d_step = _to_decimal(step)
    d_value = _to_decimal(value)
    if d_step <= 0:
        return d_value
    units = (d_value / d_step).to_integral_value(rounding=ROUND_FLOOR)
    return (units * d_step).quantize(d_step.normalize())


def ceil_to_step(value, step) -> Decimal:
    """Sube `value` al múltiplo de `step` inmediatamente superior (exacto).

    Clave del fix: aquí NO se hace ningún `round(value/step, 8)` previo.
    Ese round era el que, con un ratio como 5.000000001, podía devolver
    un múltiplo por debajo del objetivo. Con Decimal el cociente es
    exacto y ROUND_CEILING nunca deja la cantidad corta.
    """
    d_step = _to_decimal(step)
    d_value = _to_decimal(value)
    if d_step <= 0:
        return d_value
    units = (d_value / d_step).to_integral_value(rounding=ROUND_CEILING)
    return (units * d_step).quantize(d_step.normalize())


def clamp_price(value: float, minimum: float = MIN_VALID_PRICE) -> float:
    """Asegura que un precio nunca quede por debajo del mínimo válido."""
    try:
        price = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(price):
        return 0.0
    return max(minimum, price)


def round_price_to_tick(price, tick_size, side_up: bool = True) -> str:
    """Ajusta un precio al tickSize del símbolo y lo devuelve como STRING
    ya formateado. Sin esto, un triggerPrice de TP/SL con más decimales
    de los que acepta el símbolo se rechaza con -1111 'Precision is over
    the maximum defined for this asset'."""
    d_tick = _to_decimal(tick_size)
    d_price = _to_decimal(price)
    if d_tick <= 0:
        return f"{float(d_price):f}".rstrip("0").rstrip(".") or "0"
    adjusted = ceil_to_step(d_price, d_tick) if side_up else floor_to_step(d_price, d_tick)
    decimals = _step_decimals(d_tick)
    return f"{adjusted:.{decimals}f}"


def format_qty(value, step) -> str:
    """Formatea la cantidad como STRING con los decimales exactos del
    stepSize, sin notación científica.

    BUG CORREGIDO: la versión anterior usaba `f"{value:.Nf}"`, que
    REDONDEA — y por tanto podía bajar la cantidad medio step por debajo
    de lo calculado (p.ej. 0.0014 -> "0.001"), dejando el notional por
    debajo del mínimo justo en el string que se manda a Binance, después
    de que la validación de notional ya hubiese pasado. Y con decimales=0
    usaba `int(round(...))`, que aplica redondeo bancario (round(4.5)==4).
    Ahora se sube al múltiplo de step (ROUND_CEILING), así el string
    nunca vale menos que el valor validado.
    """
    d_step = _to_decimal(step, "1")
    if d_step <= 0:
        d_step = Decimal("1")
    adjusted = ceil_to_step(value, d_step)
    decimals = _step_decimals(d_step)
    return f"{adjusted:.{decimals}f}"


def resolve_safe_quantity(
    desired_notional: float,
    price: float,
    filters: dict,
    extra_buffer_pct: float = 0.0,
) -> tuple[float, float, str]:
    """
    Calcula la cantidad final a enviar a Binance a partir de un notional
    (tamaño de orden en USDT) objetivo y el precio de referencia, en vez
    de confiar ciegamente en la `quantity` que llega en la señal.

    - Convierte notional -> quantity con el precio más fresco disponible.
    - Redondea SIEMPRE HACIA ARRIBA al stepSize (LOT_SIZE) del símbolo:
      hacia abajo se pierde hasta un step entero y el notional se queda
      corto (-4164); hacia arriba, como mucho, se paga un step de más.
    - Sube la cantidad hasta que el notional cumpla el mínimo exigido
      (MIN_NOTIONAL_USDT o el real del símbolo, con colchón).
    - Devuelve TAMBIÉN el string ya formateado que se va a enviar, y
      valida el notional sobre ESE string — no sobre un float intermedio
      que luego el formateo podía recortar.

    Devuelve (quantity, notional_final, quantity_str).
    """
    d_price = _to_decimal(price)
    if d_price <= 0:
        raise ValueError("price debe ser > 0 para calcular la cantidad")

    # Si no se conoce el stepSize real del símbolo, se asume cantidad
    # entera (stepSize=1): un entero siempre es múltiplo válido de
    # cualquier stepSize más fino (0.1, 0.01, 0.001...), así que es el
    # fallback universalmente seguro frente a -1111. Cuando
    # USE_EXCHANGE_INFO está activo esto casi nunca se usa, porque se
    # conoce el stepSize de verdad.
    d_step = _to_decimal(filters.get("stepSize", 1.0), "1")
    if d_step <= 0:
        d_step = Decimal("1")
    d_min_qty = _to_decimal(filters.get("minQty", d_step), "0")

    d_min_notional = max(
        _to_decimal(filters.get("min_notional", MIN_NOTIONAL_USDT)),
        _to_decimal(MIN_NOTIONAL_USDT),
    )
    d_min_notional *= (Decimal("1") + _to_decimal(extra_buffer_pct) / Decimal("100"))

    # Objetivo: nunca por debajo del mínimo exigido.
    d_target = max(_to_decimal(desired_notional), d_min_notional)

    qty = ceil_to_step(d_target / d_price, d_step)
    if qty < d_min_qty:
        qty = ceil_to_step(d_min_qty, d_step)

    # Verificación final SOBRE EL STRING que realmente se va a enviar.
    # Se sube de step en step hasta que el notional del string cumpla el
    # mínimo. El bucle está acotado: cada iteración añade un step, y el
    # primer cálculo ya deja la cantidad prácticamente en el objetivo.
    qty_str = format_qty(qty, d_step)
    guard = 0
    while _to_decimal(qty_str) * d_price < d_min_notional and guard < 1000:
        qty = _to_decimal(qty_str) + d_step
        qty_str = format_qty(qty, d_step)
        guard += 1

    final_qty = _to_decimal(qty_str)

    # Techo del símbolo (MARKET_LOT_SIZE.maxQty, o LOT_SIZE.maxQty): pasarse
    # se rechaza con -1013. Se recorta hacia abajo al step y se avisa, porque
    # la posición resultante será menor que la pedida.
    d_max = _to_decimal(filters.get("market_max_qty") or filters.get("maxQty") or 0, "0")
    if d_max > 0 and final_qty > d_max:
        capped = floor_to_step(d_max, d_step)
        log.warning(
            f"resolve_safe_quantity: cantidad {qty_str} supera el máximo del símbolo ({d_max}); "
            f"se recorta a {capped}"
        )
        qty_str = format_qty(capped, d_step)
        if _to_decimal(qty_str) > d_max:
            qty_str = f"{capped:.{_step_decimals(d_step)}f}"
        final_qty = _to_decimal(qty_str)

    notional = final_qty * d_price
    return float(final_qty), float(notional), qty_str


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
    # Modo con el que se logró (o se asumió) la orden de ENTRADA de esta
    # posición puntual: True = Hedge (positionSide=LONG/SHORT), False =
    # One-way (positionSide=BOTH). Ya no se decide por el flag global
    # HEDGE_MODE sino por lo que realmente aceptó Binance para esta
    # posición (ver ExecutionManager._place_entry_order). Se usa luego
    # para cerrar / poner TP-SL con el positionSide correcto.
    hedge_mode: bool = True

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

        # Caché de filtros reales por símbolo (stepSize / minQty /
        # minNotional / tickSize) leídos de /fapi/v1/exchangeInfo, que es
        # público y se consulta una vez cada EXCHANGE_INFO_TTL_S. Si no
        # se puede cargar, se cae a SAFE_DEFAULT_FILTERS (cantidad
        # entera), que sigue siendo el fallback seguro frente a -1111.
        self._filters_cache: dict[str, dict] = {}
        self._filters_loaded_at: float = 0.0
        self._filters_lock = asyncio.Lock()

        # Cache de leverage aplicado por símbolo, para no repetir la
        # llamada REST de set_leverage si el valor no cambió (velocidad).
        self._leverage_cache: dict[str, int] = {}
        self._leverage_lock = asyncio.Lock()

        # Freno de bloqueo de IP (Binance -1003 / HTTP 418): si Binance ya
        # nos banea por exceso de requests, dejamos de pegarle a REST hasta
        # que pase el tiempo indicado en el propio mensaje de error, en vez
        # de seguir reintentando y empeorar/alargar el bloqueo.
        self._rest_ban_until_ms: float = 0.0

        # Freno de bloqueo POR PROXY/IP para la llamada de leverage: cada
        # URL de PROXY_URLS tiene su propio timestamp de baneo, así una
        # IP baneada no tumba a las demás — se salta a la siguiente.
        self._proxy_ban_until_ms: dict[str, float] = {}

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

    @staticmethod
    def _proxy_label(proxy_url: Optional[str]) -> str:
        """Etiqueta legible sin credenciales, solo para logs (host:puerto)."""
        if not proxy_url:
            return "directo (sin proxy)"
        try:
            return proxy_url.split("@", 1)[-1]
        except Exception:
            return "proxy"

    def _is_proxy_banned(self, proxy_url: str) -> bool:
        until = self._proxy_ban_until_ms.get(proxy_url, 0.0)
        return until > datetime.now(timezone.utc).timestamp() * 1000

    def _proxy_ban_remaining_s(self, proxy_url: str) -> float:
        until = self._proxy_ban_until_ms.get(proxy_url, 0.0)
        return max(0.0, until / 1000 - datetime.now(timezone.utc).timestamp())

    def _note_possible_proxy_ban(self, proxy_url: str, response_text: str):
        """
        Igual que _note_possible_ip_ban pero por proxy individual: si
        Binance devuelve -1003 usando `proxy_url`, guarda el timestamp de
        baneo SOLO para esa IP, dejando libres las demás de PROXY_URLS.
        """
        if "-1003" not in response_text:
            return
        match = re.search(r"banned until (\d+)", response_text)
        if not match:
            return
        until_ms = float(match.group(1))
        if until_ms > self._proxy_ban_until_ms.get(proxy_url, 0.0):
            self._proxy_ban_until_ms[proxy_url] = until_ms
            until_dt = datetime.fromtimestamp(until_ms / 1000, tz=timezone.utc)
            log.error(
                f"⛔ IP {self._proxy_label(proxy_url)} bloqueada por Binance (-1003) hasta "
                f"{until_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} (~{self._proxy_ban_remaining_s(proxy_url):.0f}s) "
                f"— se saltará a la siguiente IP de PROXY_URLS si hay alguna disponible"
            )

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
            elif msg.type in (
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.ERROR,
            ):
                # CLOSING/CLOSE faltaban: sin ellos, receive() devolvía
                # el mismo mensaje una y otra vez y el reader se quedaba
                # girando en vacío al 100% de CPU en vez de reconectar.
                break

        for fut in list(self._pending.values()):
            if not fut.done():
                # Excepción nueva por future: compartir una sola instancia
                # hace que Python reporte "exception was never retrieved"
                # para todas menos una.
                fut.set_exception(ConnectionError("WebSocket API desconectado"))
        self._pending.clear()

    async def _request(self, method: str, params: Optional[dict] = None, signed: bool = False, timeout: float = 20.0, _retry: bool = True) -> dict:
        await self.connect()

        # Los params originales se guardan SIN firmar para poder
        # re-firmarlos en el reintento. BUG CORREGIDO: antes el reintento
        # se hacía con `signed=False` sobre los params ya firmados, así
        # que reenviaba la firma y el timestamp viejos — Binance lo
        # rechazaba con -1021 (timestamp fuera de recvWindow) o -1022
        # (firma inválida), y el "reintento" nunca servía de nada.
        raw_params = dict(params or {})
        params = dict(raw_params)
        if signed:
            params["apiKey"] = self.api_key
            params["timestamp"] = int(datetime.now(timezone.utc).timestamp() * 1000)
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
                log.warning(f"_request: fallo enviando ({e!r}); reconectando y reintentando una vez (re-firmando)")
                return await self._request(method, raw_params, signed=signed, timeout=timeout, _retry=False)
            raise

        try:
            response = await asyncio.wait_for(fut, timeout=timeout)
        except Exception as e:
            self._pending.pop(req_id, None)
            # Un reintento automático de `order.place` es peligroso: la
            # orden original pudo haberse ejecutado y no haber llegado la
            # respuesta, así que reenviarla abriría el DOBLE de posición.
            # Sólo se reintentan métodos idempotentes (consultas).
            is_order_write = method.startswith(("order.", "algoOrder."))
            if _retry and not self._ws_alive() and not is_order_write:
                log.warning(f"_request: sin respuesta ({e!r}); conexión muerta, reconectando y reintentando una vez (re-firmando)")
                return await self._request(method, raw_params, signed=signed, timeout=timeout, _retry=False)
            if is_order_write and not self._ws_alive():
                log.error(
                    f"_request: {method} sin respuesta y WS caído — NO se reintenta automáticamente "
                    f"para no duplicar la orden; verifica el estado real en Binance"
                )
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

            if not PROXY_URLS:
                # Sin ningún proxy configurado: sale con la IP directa del proceso.
                data = await self._set_leverage_via_proxy(symbol, leverage, proxy_url=None)
                self._leverage_cache[symbol] = leverage
                return data

            # Solo se intenta con las IPs que ahora mismo NO están marcadas
            # como baneadas por Binance (-1003).
            candidates = [p for p in PROXY_URLS if not self._is_proxy_banned(p)]
            if not candidates:
                soonest = min(self._proxy_ban_remaining_s(p) for p in PROXY_URLS)
                raise RuntimeError(
                    f"REST omitida: IP bloqueada por Binance (-1003), quedan ~{soonest:.0f}s "
                    f"(las {len(PROXY_URLS)} IP(s) de PROXY_URLS están bloqueadas ahora mismo)"
                )

            last_err: Optional[Exception] = None
            for proxy_url in candidates:
                try:
                    data = await self._set_leverage_via_proxy(symbol, leverage, proxy_url=proxy_url)
                    self._leverage_cache[symbol] = leverage
                    return data
                except Exception as e:
                    last_err = e
                    if self._is_proxy_banned(proxy_url):
                        log.warning(
                            f"set_leverage: {symbol} — IP {self._proxy_label(proxy_url)} quedó bloqueada; "
                            f"probando con la siguiente IP de PROXY_URLS"
                        )
                        continue
                    if isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError)):
                        log.warning(
                            f"set_leverage: {symbol} — fallo de conexión con {self._proxy_label(proxy_url)} "
                            f"({e!r}); probando con la siguiente IP de PROXY_URLS"
                        )
                        continue
                    # Rechazo que no tiene que ver con bloqueo de IP (p.ej. -4028
                    # leverage inválido para el símbolo): cambiar de IP no lo va a
                    # resolver, así que se propaga tal cual para que lo maneje
                    # set_leverage_with_fallback (escalera de leverage).
                    raise
            # Se agotaron todas las IPs candidatas por errores de conexión/baneo.
            raise last_err if last_err else RuntimeError("set_leverage: sin IPs disponibles en PROXY_URLS")

    async def _set_leverage_via_proxy(self, symbol: str, leverage: int, proxy_url: Optional[str]) -> dict:
        """Ejecuta el POST /fapi/v1/leverage a través de una IP concreta
        (o directo si proxy_url es None). No cachea leverage ni maneja
        reintentos entre IPs — eso lo hace set_leverage()."""
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

        # Única llamada REST del bot que se mantiene fuera de la WS API
        # (Binance no expone equivalente para cambiar leverage).
        request_kwargs = {"headers": headers, "timeout": aiohttp.ClientTimeout(total=10)}
        if proxy_url:
            request_kwargs["proxy"] = proxy_url

        async with session.post(url, **request_kwargs) as resp:
            text = await resp.text()
            if resp.status != 200:
                if proxy_url:
                    self._note_possible_proxy_ban(proxy_url, text)
                else:
                    self._note_possible_ip_ban(text)
                raise RuntimeError(f"REST set_leverage error {resp.status}: {text}")
            try:
                data = json.loads(text)
            except Exception:
                data = {"raw": text}
            log.info(f"Leverage REST OK vía {self._proxy_label(proxy_url)}: {symbol} → {data.get('leverage', leverage)}x")
            return data

    # ── Escalera de leverage de respaldo ───────────────────────────────
    # Algunos símbolos rechazan el leverage configurado por defecto
    # (cada símbolo tiene su propio límite máximo según el notional, y
    # no podemos conocerlos todos de antemano, menos aún con limitaciones
    # de IP que impiden golpear /leverageBracket por cada símbolo). En
    # vez de abortar la apertura, se prueba esta escalera 10x→4x hasta
    # que Binance acepte uno.
    LEVERAGE_FALLBACK_LADDER = [ 4]

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

    async def get_open_orders(self, symbol: Optional[str] = None) -> list[dict]:
        """
        SIN EQUIVALENTE EN LA WS API: ws-fapi solo ofrece order.status
        (consulta de UNA orden puntual), no un listado de abiertas. Se
        mantiene como REST firmado, sin pasar por Fixie.
        """
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = await self._rest_signed("GET", "/fapi/v1/openOrders", params)
        return result if isinstance(result, list) else []

    async def cancel_order(self, symbol: str, order_id) -> dict:
        """Cancela una orden individual — migrado a la WS API (order.cancel),
        que sí tiene equivalente documentado por Binance."""
        result = await self._request("order.cancel", {"symbol": symbol, "orderId": order_id}, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def cancel_all_open_orders(self, symbol: str) -> dict:
        """
        SIN EQUIVALENTE EN LA WS API: Binance no expone un método
        'cancelar todas las órdenes del símbolo' en ws-fapi (solo existe
        order.cancel para una orden a la vez). Se mantiene como REST
        firmado (DELETE /fapi/v1/allOpenOrders), SIN pasar por Fixie
        (por decisión explícita, Fixie se reserva solo para leverage).
        """
        return await self._rest_signed("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    async def create_tp_sl_order(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        order_type: str,  # "STOP_MARKET" (SL) o "TAKE_PROFIT_MARKET" (TP)
        position_side: Optional[str] = None,
        close_position: bool = True,
        quantity=None,   # float o string ya formateado al stepSize
        time_in_force: str = "GTC",
    ) -> dict:
        """
        TP/SL de Binance Futures. order.place (la WS API de órdenes
        normales) NO acepta STOP_MARKET/TAKE_PROFIT_MARKET con
        closePosition (-4120) — exige el endpoint dedicado de Algo Order,
        que ahora también está disponible en la WS API (método
        `algoOrder.place`, ver create_algo_order), así que se envían por
        ahí — ya no hace falta REST para esto.

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

        # BUG CORREGIDO: `str(0.000012)` da "1.2e-05" y Binance rechaza la
        # notación científica. Se formatea siempre en decimal plano.
        trigger_str = format(_to_decimal(trigger_price).normalize(), "f")

        use_close_position = close_position and quantity is None

        # Se decide reduceOnly según el positionSide REAL de esta orden
        # puntual (no el flag global HEDGE_MODE, que puede no coincidir
        # si esta posición cayó al fallback One-way): con LONG/SHORT
        # (Hedge) Binance rechaza reduceOnly porque side+positionSide ya
        # implica reducción; con BOTH (One-way) sí hace falta para no
        # abrir posición nueva.
        is_hedge_side = (position_side or "BOTH") in ("LONG", "SHORT")
        reduce_only = None
        if not use_close_position and not is_hedge_side:
            reduce_only = "true"

        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            triggerPrice=trigger_str,
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
            "quantity": _plain_decimal(quantity),
            "price": _plain_decimal(price),
            "timeInForce": time_in_force,
            "newOrderRespType": "RESULT",
        }
        if position_side:
            params["positionSide"] = position_side
        # Mismo motivo que en create_market_order: reduceOnly no se puede
        # enviar junto con positionSide=LONG/SHORT (Hedge) — Binance lo
        # rechaza con -1106.
        if reduce_only and position_side not in ("LONG", "SHORT"):
            params["reduceOnly"] = "true"
        result = await self._request("order.place", params=params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def create_algo_order(self, symbol: str, side: str, order_type: str, **extra) -> dict:
        """
        Algo Order condicional — MIGRADO a la WS API: Binance añadió el
        método `algoOrder.place` (antes solo existía como REST POST
        /fapi/v1/algoOrder, necesario para STOP_MARKET/TAKE_PROFIT_MARKET
        con closePosition porque order.place los rechazaba con -4120).
        `extra` admite cualquier param adicional (triggerPrice,
        positionSide, closePosition, quantity, reduceOnly, timeInForce,
        price, workingType...); las claves con valor None se omiten.
        """
        params = {"symbol": symbol, "side": side, "algoType": "CONDITIONAL", "type": order_type}
        for k, v in extra.items():
            if v is not None:
                params[k] = v
        result = await self._request("algoOrder.place", params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def get_open_algo_orders(self, symbol: Optional[str] = None) -> list[dict]:
        """
        SIN EQUIVALENTE EN LA WS API: no existe un método ws-fapi para
        listar algo orders abiertas (solo algoOrder.place/algoOrder.cancel
        de a una). Se mantiene como REST firmado, sin pasar por Fixie.
        """
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = await self._rest_signed("GET", "/fapi/v1/algoOpenOrders", params)
        if isinstance(result, dict):
            return result.get("orders") or result.get("algoOrders") or []
        return result if isinstance(result, list) else []

    async def cancel_algo_order(self, algo_id) -> dict:
        """Cancela un algo order individual — migrado a la WS API
        (algoOrder.cancel)."""
        result = await self._request("algoOrder.cancel", {"algoId": algo_id}, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def cancel_all_algo_orders(self, symbol: str) -> dict:
        """
        SIN EQUIVALENTE EN LA WS API: no existe un 'cancelar todas las
        algo orders del símbolo' en ws-fapi. Se mantiene como REST
        firmado (DELETE /fapi/v1/algoOpenOrders), sin pasar por Fixie.
        """
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
        """margin_type: 'ISOLATED' o 'CROSSED'.
        SIN EQUIVALENTE EN LA WS API — se mantiene como REST firmado,
        sin pasar por Fixie."""
        return await self._rest_signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})

    async def get_position_mode(self) -> bool:
        """Consulta el modo actual de la CUENTA en Binance (no el de este
        proceso). True = Hedge Mode (dualSidePosition), False = One-way.
        SIN EQUIVALENTE EN LA WS API — se mantiene como REST firmado,
        sin pasar por Fixie."""
        result = await self._rest_signed("GET", "/fapi/v1/positionSide/dual", {})
        return bool(result.get("dualSidePosition"))

    async def set_position_mode(self, hedge: bool) -> dict:
        """Cambia el modo de posición de la CUENTA en Binance.

        IMPORTANTE: Binance rechaza este cambio (-4059 / -4068) si hay
        posiciones abiertas u órdenes activas en la cuenta — hay que
        cerrar todo primero. Esto es una restricción de Binance, no de
        este bot.
        SIN EQUIVALENTE EN LA WS API — se mantiene como REST firmado,
        sin pasar por Fixie.
        """
        return await self._rest_signed(
            "POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "true" if hedge else "false"}
        )

    async def modify_position_margin(self, symbol: str, amount: float, position_side: str = "BOTH", add: bool = True) -> dict:
        """type=1 añade margen, type=2 lo retira (solo válido en ISOLATED).
        SIN EQUIVALENTE EN LA WS API — se mantiene como REST firmado,
        sin pasar por Fixie."""
        params = {
            "symbol": symbol,
            "amount": str(abs(amount)),
            "type": 1 if add else 2,
            "positionSide": position_side,
        }
        return await self._rest_signed("POST", "/fapi/v1/positionMargin", params)

    # FALLBACK, ya no el caso normal: se usa sólo si exchangeInfo no se
    # pudo cargar (o si USE_EXCHANGE_INFO=false). Asume cantidad entera
    # (stepSize=1) porque un entero siempre es múltiplo válido de
    # cualquier stepSize más fino, así que es lo que menos rechazos
    # -1111/-1013 provoca sin tener el dato exacto. Su desventaja es
    # grande y por eso ya no es el default: en símbolos que aceptan
    # fracciones obliga a saltar al entero siguiente, lo que en un
    # símbolo de precio alto dispara el notional a cientos de dólares.
    SAFE_DEFAULT_FILTERS: dict = {
        "stepSize": 1.0,
        "minQty": 1.0,
        "tickSize": 0.0,
        "qty_precision": 0,
        "min_notional": MIN_NOTIONAL_USDT,
    }

    # ── Filtros REALES por símbolo ────────────────────────────────────
    # Se recuperan de /fapi/v1/exchangeInfo, endpoint PÚBLICO (sin firma,
    # sin API key, peso 1) que se consulta UNA vez y se cachea
    # EXCHANGE_INFO_TTL_S segundos (6 h por defecto). Eso son ~4
    # requests al día: no es comparable a consultarlo por símbolo en
    # cada señal, que era el motivo original para quitarlo.
    #
    # Por qué importa para el bug de redondeo: sin el stepSize real hay
    # que asumir stepSize=1 y entonces la cantidad sólo puede ser entera.
    # En un símbolo de precio alto eso dispara el notional a cientos de
    # dólares (margen insuficiente), y en uno de precio bajo obliga a
    # saltos gruesos que descuadran el tamaño pedido. Con el stepSize
    # real, la cantidad cae exactamente donde debe.
    async def _fetch_exchange_info(self) -> dict:
        session = await self._ensure_http_session()
        url = f"{REST_FAPI_URL}/fapi/v1/exchangeInfo"
        last_err: Optional[Exception] = None
        # Se intenta primero por las IPs de PROXY_URLS no baneadas y, si
        # no hay ninguna, directo. Es un endpoint público: funciona igual.
        attempts: list[Optional[str]] = [p for p in PROXY_URLS if not self._is_proxy_banned(p)] or [None]
        for proxy_url in attempts:
            kwargs: dict = {"timeout": aiohttp.ClientTimeout(total=20)}
            if proxy_url:
                kwargs["proxy"] = proxy_url
            try:
                async with session.get(url, **kwargs) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        if proxy_url:
                            self._note_possible_proxy_ban(proxy_url, text)
                        else:
                            self._note_possible_ip_ban(text)
                        raise RuntimeError(f"exchangeInfo HTTP {resp.status}: {text[:200]}")
                    return json.loads(text)
            except Exception as e:
                last_err = e
                continue
        raise last_err if last_err else RuntimeError("exchangeInfo: sin salida disponible")

    @staticmethod
    def _parse_symbol_filters(sym_info: dict) -> dict:
        parsed = dict(BinanceAPI.SAFE_DEFAULT_FILTERS)
        for f in sym_info.get("filters", []):
            ftype = f.get("filterType")
            if ftype == "LOT_SIZE":
                parsed["stepSize"] = f.get("stepSize", parsed["stepSize"])
                parsed["minQty"] = f.get("minQty", parsed["minQty"])
                parsed["maxQty"] = f.get("maxQty")
            elif ftype == "MARKET_LOT_SIZE":
                # Para órdenes MARKET manda este filtro, no LOT_SIZE.
                parsed["market_min_qty"] = f.get("minQty")
                parsed["market_max_qty"] = f.get("maxQty")
            elif ftype == "MIN_NOTIONAL":
                parsed["min_notional"] = f.get("notional", parsed["min_notional"])
            elif ftype == "PRICE_FILTER":
                parsed["tickSize"] = f.get("tickSize", parsed["tickSize"])
        parsed["qty_precision"] = _step_decimals(parsed.get("stepSize", 1.0))
        # El notional mínimo efectivo nunca baja del configurado por el
        # usuario (MIN_NOTIONAL_USDT), que ya lleva su propio margen.
        try:
            parsed["min_notional"] = max(float(parsed["min_notional"]), MIN_NOTIONAL_USDT)
        except Exception:
            parsed["min_notional"] = MIN_NOTIONAL_USDT
        # El minQty de MARKET, si existe y es mayor, es el que aplica.
        try:
            if parsed.get("market_min_qty") and float(parsed["market_min_qty"]) > float(parsed["minQty"]):
                parsed["minQty"] = parsed["market_min_qty"]
        except Exception:
            pass
        return parsed

    async def _refresh_symbol_filters(self, force: bool = False) -> None:
        if not USE_EXCHANGE_INFO:
            return
        now = time.monotonic()
        if not force and self._filters_cache and (now - self._filters_loaded_at) < EXCHANGE_INFO_TTL_S:
            return
        async with self._filters_lock:
            now = time.monotonic()
            if not force and self._filters_cache and (now - self._filters_loaded_at) < EXCHANGE_INFO_TTL_S:
                return
            try:
                info = await self._fetch_exchange_info()
            except Exception as e:
                log.warning(
                    f"_refresh_symbol_filters: no se pudo leer exchangeInfo ({e}) — "
                    f"se seguirá usando {'la caché previa' if self._filters_cache else 'SAFE_DEFAULT_FILTERS (cantidad entera)'}"
                )
                # Se reintenta en el próximo uso, pero no en bucle cerrado.
                self._filters_loaded_at = time.monotonic() - EXCHANGE_INFO_TTL_S + 60
                return

            cache: dict[str, dict] = {}
            for sym_info in info.get("symbols", []):
                symbol = sym_info.get("symbol")
                if not symbol:
                    continue
                if sym_info.get("status") not in (None, "TRADING"):
                    continue
                try:
                    cache[symbol] = self._parse_symbol_filters(sym_info)
                except Exception as e:
                    log.debug(f"_refresh_symbol_filters: {symbol} ignorado: {e}")

            if cache:
                self._filters_cache = cache
                self._filters_loaded_at = time.monotonic()
                log.info(f"exchangeInfo cargado: filtros reales de {len(cache)} símbolos (TTL {EXCHANGE_INFO_TTL_S:.0f}s)")

    async def get_symbol_filters(self, symbol: str) -> dict:
        """Filtros reales del símbolo, o el default seguro si no se
        pudieron cargar. NUNCA lanza: la apertura no debe caerse porque
        exchangeInfo no respondiera."""
        try:
            await self._refresh_symbol_filters()
        except Exception as e:
            log.warning(f"get_symbol_filters: refresco falló para {symbol}: {e}")
        filters = self._filters_cache.get(symbol.upper())
        if filters:
            return dict(filters)
        if USE_EXCHANGE_INFO and self._filters_cache:
            log.warning(f"get_symbol_filters: {symbol} no está en exchangeInfo — se usa el default seguro (cantidad entera)")
        return dict(self.SAFE_DEFAULT_FILTERS)

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
            # `_plain_decimal` evita además la notación científica que
            # produce str() con cantidades pequeñas (1e-05).
            "quantity": _plain_decimal(quantity),
            "newOrderRespType": new_order_resp_type,
        }
        if position_side:
            params["positionSide"] = position_side
        # En Hedge Mode (positionSide=LONG/SHORT) Binance RECHAZA el
        # parámetro reduceOnly con -1106 "Parameter 'reduceonly' sent
        # when not required" — side+positionSide ya implica reducción
        # por sí solo. Sólo tiene sentido enviarlo en One-way
        # (positionSide=BOTH o ausente). Se filtra aquí, centralizado,
        # para que ningún caller (close_position_market,
        # close_all_positions, etc.) tenga que acordarse de esto.
        is_hedge_side = position_side in ("LONG", "SHORT")
        if reduce_only and not is_hedge_side:
            params["reduceOnly"] = "true"
        result = await self._request("order.place", params=params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def close_position_market(
        self,
        symbol: str,
        direction: str,
        quantity,
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
        self._last_balance_refresh: float = 0.0
        self._balance_refresh_lock = asyncio.Lock()

    async def refresh_balance(self, force: bool = False):
        """
        Consulta el balance (account.balance, WS API). Autolimitada a un
        máximo de 1 consulta real cada BALANCE_POLL_S segundos: así,
        aunque open_trade/close_trade sigan llamando a esto tras cada
        operación, no se dispara una petición nueva si ya se refrescó
        hace poco — evita sumarse a la ráfaga leverage(REST)+orden(WS)
        que salía junta en el mismo instante. balance_sync_loop() se
        encarga de mantenerlo al día (force=True) cada BALANCE_POLL_S
        segundos de forma independiente a la actividad de trading.
        """
        now = time.monotonic()
        if not force and (now - self._last_balance_refresh) < BALANCE_POLL_S:
            return
        async with self._balance_refresh_lock:
            now = time.monotonic()
            if not force and (now - self._last_balance_refresh) < BALANCE_POLL_S:
                return
            try:
                balances = await self.api.account_balance()
                for b in balances:
                    if b.get("asset") == "USDT":
                        self._balance = float(b.get("availableBalance", b.get("balance", 0)))
                        break
                self._last_balance_refresh = time.monotonic()
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

    async def get_entry_reference_price(
        self,
        symbol: str,
        extra_symbols: Optional[list[str]] = None,
        fallback_price: float = 0.0,
    ) -> float:
        """
        Resuelve el precio REAL de entrada — NUNCA confía en el `price`
        que llega en la señal salvo como ÚLTIMO recurso (ver punto 3),
        ya que normalmente es solo informativo/de cuando se generó la
        señal y puede llevar segundos de desfase.

        100% WebSocket — ya NO hay fallback REST de precio (get_rest_price
        se eliminó por decisión explícita: se quitaron todas las llamadas
        REST salvo la de leverage). Orden de preferencia:

        1. Caché WS ya activa para el símbolo — pero SÓLO si es reciente
           (< MAX_PRICE_AGE_S). Un precio cacheado viejo (p.ej. de una
           posición anterior ya cerrada en ese mismo símbolo, cuyo stream
           se desuscribió) es PEOR que no tener nada: produce un
           entry_price completamente fuera de mercado sin ningún error
           visible. Por eso aquí se exige freshness, no solo presencia.
        2. Si el símbolo aún no estaba suscrito (o el dato es viejo), se
           suscribe al WS de precios y se espera a que llegue un tick
           fresco, re-suscribiendo periódicamente por si el símbolo se
           cayó del stream o el primer mensaje de suscripción se perdió.
        3. Si tras esperar el WS el tiempo máximo sigue sin entregar nada
           Y se dispone de un `fallback_price` (típicamente el precio que
           traía la señal de entrada), se usa ESE como último recurso en
           vez de cancelar la apertura — dejando bien claro en el log que
           es un precio aproximado y no confirmado contra mercado.
           Cancelar la operación solo ocurre si no hay absolutamente
           ningún precio disponible (ni WS ni fallback).
        """
        try:
            p = self.price_ws.get_price(symbol, max_age_s=MAX_PRICE_AGE_S)
            if p and p > 0:
                return float(p)
        except Exception:
            pass

        def _subscribe():
            try:
                wanted = self.active_symbols | {symbol}
                if extra_symbols:
                    wanted |= set(extra_symbols)
                self.price_ws.update_symbols(list(wanted))
            except Exception as e:
                log.warning(f"get_entry_reference_price: no se pudo suscribir {symbol} al WS: {e}")

        _subscribe()

        # Margen de espera al tick fresco del WS antes de recurrir al
        # fallback_price. Re-suscribe periódicamente por si el primer
        # intento de suscripción se perdió.
        # time.monotonic() en vez de asyncio.get_event_loop(): esta
        # última está deprecada fuera de una corrutina en ejecución y en
        # Python 3.12 emite DeprecationWarning.
        deadline = time.monotonic() + 10.0
        resub_every_s = 3.0
        last_resub = time.monotonic()
        while time.monotonic() < deadline:
            await asyncio.sleep(0.2)
            try:
                p = self.price_ws.get_price(symbol, max_age_s=MAX_PRICE_AGE_S)
                if p and p > 0:
                    return float(p)
            except Exception:
                pass
            now = time.monotonic()
            if now - last_resub >= resub_every_s:
                _subscribe()
                last_resub = now

        if fallback_price and fallback_price > 0:
            log.warning(
                f"get_entry_reference_price: {symbol} sin precio WS tras esperar — se usa el precio de la señal "
                f"({fallback_price}) como último recurso para NO cancelar la apertura"
            )
            return float(fallback_price)

        log.error(
            f"get_entry_reference_price: {symbol} sin precio WS y sin fallback_price disponible "
            f"— no es posible abrir sin ningún precio"
        )
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
        target_notional: float = 0.0,
    ) -> dict:
        """
        Envía la orden MARKET con la quantity ya calculada. Si Binance la
        rechaza específicamente por notional insuficiente (-4164) o por
        precisión/stepSize (-1013 / -1111) — típico cuando el precio se
        movió justo entre el cálculo y el envío — se recalcula la
        cantidad con un colchón de seguridad mayor y se reintenta UNA
        sola vez con un precio fresco.

        BUG CORREGIDO: el reintento recalculaba la cantidad SOLO a partir
        del notional MÍNIMO, ignorando el tamaño realmente pedido. Es
        decir, si una orden de 200 USDT se rechazaba por un -1111 de
        precisión, el reintento la reemplazaba por una de ~5.6 USDT y la
        posición quedaba 35 veces más pequeña de lo previsto, sin ningún
        error visible. Ahora `target_notional` conserva el tamaño
        objetivo y el colchón sólo se aplica como suelo, nunca como techo.
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

            # Colchón mayor en el reintento (p.ej. 5% -> 15%), pero
            # aplicado sobre el tamaño OBJETIVO, no sobre el mínimo.
            retry_buffer = max(NOTIONAL_SAFETY_BUFFER_PCT * 3, 15.0)
            min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)

            # Si no se pasó el objetivo, se reconstruye desde la qty que
            # ya se había intentado enviar (así tampoco se encoge).
            if target_notional <= 0:
                try:
                    target_notional = float(_to_decimal(qty_str)) * float(fresh_price)
                except Exception:
                    target_notional = min_notional
            target_notional = max(target_notional, min_notional)

            retry_qty, retry_notional, retry_qty_str = resolve_safe_quantity(
                target_notional, fresh_price, filters, extra_buffer_pct=retry_buffer
            )

            log.info(f"_place_market_order_safe: reintento {symbol} qty={retry_qty_str} (notional≈${retry_notional:.4f}, precio={fresh_price})")

            return await self.api.create_market_order(
                symbol=symbol,
                side=side,
                quantity=retry_qty_str,
                position_side=position_side,
                reduce_only=reduce_only,
                new_order_resp_type="RESULT",
            )

    @staticmethod
    def _is_margin_insufficient(err: str) -> bool:
        return "-2019" in err or "Margin is insufficient" in err

    async def _place_entry_order(
        self,
        symbol: str,
        side: str,
        qty_str: str,
        direction: str,
        filters: dict,
        ref_price: float,
        target_notional: float = 0.0,
    ) -> tuple[Optional[dict], bool, bool]:
        """
        Envía la orden de ENTRADA. Como se opera LONG y SHORT del mismo
        símbolo al mismo tiempo, SIEMPRE se intenta primero en modo
        Hedge (positionSide=LONG/SHORT):

        - Si falla por margen insuficiente (-2019): NO se reintenta en
          otro modo — se trata igual que siempre (posición "asumida"),
          porque el problema es de margen, no de positionSide.
        - Si falla por cualquier OTRO motivo (típicamente -4061 "order's
          position side does not match user's setting", es decir la
          cuenta en Binance no está realmente en Hedge Mode) se
          reintenta UNA vez en modo One-way (positionSide=BOTH). Lo que
          haya funcionado en ESTE intento es lo que se registra como
          modo real de esta posición puntual (Trade.hedge_mode).

        Devuelve (result_o_None, used_hedge, order_assumed).
        """
        try:
            result = await self._place_market_order_safe(
                symbol=symbol, side=side, qty_str=qty_str,
                position_side=direction, filters=filters, ref_price=ref_price,
                target_notional=target_notional,
            )
            return result, True, False
        except Exception as e_hedge:
            err_hedge = str(e_hedge)
            if self._is_margin_insufficient(err_hedge):
                log.warning(f"_place_entry_order: {symbol} margen insuficiente en modo Hedge — posición asumida: {e_hedge}")
                return None, True, True

            log.warning(
                f"_place_entry_order: {symbol} rechazada en modo Hedge ({e_hedge}) por un motivo "
                f"distinto a margen — reintentando como One-way (positionSide=BOTH)"
            )
            try:
                result = await self._place_market_order_safe(
                    symbol=symbol, side=side, qty_str=qty_str,
                    position_side="BOTH", filters=filters, ref_price=ref_price,
                    target_notional=target_notional,
                )
                return result, False, False
            except Exception as e_oneway:
                err_oneway = str(e_oneway)
                if self._is_margin_insufficient(err_oneway):
                    log.warning(f"_place_entry_order: {symbol} margen insuficiente en reintento One-way — posición asumida: {e_oneway}")
                    return None, False, True
                log.error(f"_place_entry_order: {symbol} falló tanto en Hedge como en One-way: {e_oneway}")
                raise

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

        # ── Multiplicador de tamaño por niveles (histéresis) ────────────
        # Se calculan DOS multiplicadores independientes (ver
        # PositionMultiplier):
        #   - mult_real: sobre el balance REAL. Es el que se aplica al
        #     `quantity` que se manda de verdad a Binance en el intento
        #     principal, más abajo.
        #   - mult_ficticio: sobre el balance FICTICIO configurado en el
        #     dashboard. NO se usa para enviar órdenes — solo se calcula
        #     y se aplica más abajo si Binance rechaza la apertura por
        #     margen insuficiente, para registrar la posición "asumida"
        #     (paper trade) con esa quantity en vez de la real.
        original_quantity = quantity
        try:
            mult_real = await position_multiplier.current_multiplier_real(self.balance)
        except Exception as e:
            log.error(f"open_trade: fallo calculando multiplicador REAL para {symbol}: {e} — se usa x1")
            mult_real = 1.0
        if mult_real and abs(mult_real - 1.0) > 1e-9:
            quantity = original_quantity * mult_real
            log.info(
                f"open_trade: multiplicador REAL x{mult_real:g} aplicado para {symbol} "
                f"(modo={position_multiplier.mode}) → quantity {original_quantity} → {quantity}"
            )

        # El positionSide de ENTRADA ya no se decide con el flag global
        # HEDGE_MODE: se intenta siempre en modo Hedge primero y, si
        # Binance la rechaza por un motivo distinto a margen, se cae a
        # One-way automáticamente (ver _place_entry_order más abajo).

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
        # — 100% WebSocket, con el precio de la señal como último recurso
        # si el WS no entrega nada a tiempo — y con ESE se calcula la
        # cantidad final y se registra la entrada.
        ref_price = await self.get_entry_reference_price(symbol, fallback_price=price)
        if ref_price <= 0:
            log.error(
                f"open_trade: no se pudo obtener NINGÚN precio para {symbol} (ni WS ni precio de señal) "
                f"— apertura cancelada"
            )
            return None

        if price > 0 and abs(ref_price - price) / max(price, 1e-9) > 0.01:
            log.info(
                f"open_trade: precio de señal={price} es solo orientativo — se abre con precio real={ref_price} para {symbol}"
            )

        desired_notional = price * quantity if price > 0 else ref_price * quantity
        filled_price = ref_price

        # Filtros REALES del símbolo (stepSize / minQty / minNotional)
        # desde la caché de exchangeInfo. Si la caché no está disponible
        # devuelve el default seguro de siempre (cantidad entera), así
        # que esto nunca puede tumbar una apertura.
        filters = await self.api.get_symbol_filters(symbol)

        # set_leverage sigue siendo la única llamada REST (vía Fixie), con
        # su propia escalera de respaldo 5x→4x si el símbolo la rechaza.
        try:
            leverage_result = await self.api.set_leverage_with_fallback(symbol, LEVERAGE)
        except Exception as e:
            leverage_result = e

        if isinstance(leverage_result, Exception):
            log.warning(f"open_trade: no se pudo aplicar NINGÚN leverage de la escalera para {symbol}: {leverage_result}")
            applied_leverage = LEVERAGE
        else:
            applied_leverage = leverage_result

        try:
            # resolve_safe_quantity devuelve TAMBIÉN el string exacto que
            # se va a enviar, ya redondeado hacia arriba al stepSize y ya
            # verificado contra el notional mínimo. No se vuelve a
            # formatear después: ese doble formateo era justo lo que
            # podía recortar la cantidad por debajo de lo validado.
            send_qty, send_notional, qty_str = resolve_safe_quantity(
                desired_notional, ref_price, filters, extra_buffer_pct=NOTIONAL_SAFETY_BUFFER_PCT
            )
        except Exception as e:
            log.error(f"open_trade: no se pudo calcular quantity segura para {symbol}: {e}")
            return None

        min_notional_req = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
        if send_notional < min_notional_req:
            # Red de seguridad final. Si esto salta alguna vez, es un bug
            # y hay que verlo en los logs, no dejar que Binance lo
            # rechace con -4164 sin explicación.
            log.error(
                f"open_trade: {symbol} la cantidad calculada da notional ${send_notional:.4f} < mínimo "
                f"${min_notional_req:.4f} — apertura cancelada (revisar stepSize/precio)"
            )
            return None

        if abs(send_qty - quantity) > 1e-12:
            log.info(
                f"open_trade: quantity ajustada para {symbol} → señal={quantity} (notional≈${desired_notional:.4f}) "
                f"→ enviada={qty_str} (notional≈${send_notional:.4f}, precio_ref={ref_price}, "
                f"stepSize={filters.get('stepSize')}, minNotional={min_notional_req})"
            )
        quantity = send_qty

        log.info(f"open_trade: enviando MARKET por WS → {symbol} {side} qty={qty_str} (notional≈${send_notional:.4f}) [intento Hedge]")
        try:
            result, used_hedge, order_assumed = await self._place_entry_order(
                symbol=symbol,
                side=side,
                qty_str=qty_str,
                direction=direction,
                filters=filters,
                ref_price=ref_price,
                target_notional=send_notional,
            )
        except Exception as e_ord:
            log.error(f"open_trade: fallo enviando MARKET (Hedge y fallback One-way) para {symbol}: {e_ord}")
            return None

        if order_assumed:
            # Margen insuficiente con dinero REAL: no se reintenta contra
            # Binance. Se recalcula la quantity con el multiplicador
            # FICTICIO (sobre fictitious_balance) y se registra la
            # posición localmente como "asumida" (paper trade), sin
            # enviar ninguna otra orden al exchange.
            try:
                mult_ficticio = await position_multiplier.current_multiplier_ficticio()
            except Exception as e:
                log.error(f"open_trade: fallo calculando multiplicador FICTICIO para {symbol}: {e} — se usa x1")
                mult_ficticio = 1.0
            quantity = original_quantity * mult_ficticio if mult_ficticio else original_quantity
            log.warning(
                f"[ASUMIDA] {symbol} — margen insuficiente con dinero real (x{mult_real:g}) — "
                f"se registra como posición asumida (paper) con multiplicador FICTICIO x{mult_ficticio:g} "
                f"→ quantity {original_quantity} → {quantity}"
            )
            entry_order_id = "MARGIN_INSUFFICIENT"
        else:
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
                # executedQty primero: es lo REALMENTE ejecutado. origQty
                # es lo solicitado y, en un fill parcial, sobreestima la
                # posición (y con ella el PnL y la qty de cierre).
                executed_qty = float(result.get("executedQty") or result.get("origQty") or quantity)
                if executed_qty > 0:
                    quantity = executed_qty
            except Exception:
                pass
            log.info(
                f"MARKET WS OK: {symbol} {side} qty={quantity} id={entry_order_id} avg={filled_price} "
                f"modo={'Hedge' if used_hedge else 'One-way (fallback)'}"
            )

        async with self._lock:
            key = (symbol, direction)

            if used_hedge:
                # Esta entrada se logró (o se asumió) en Hedge Mode: Binance
                # mantiene LONG y SHORT del mismo
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
                        hedge_mode=True,
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
                    existing.hedge_mode = True
                    self._paper_id_map[paper_trade_id] = key
                    trade = existing
                    action_tag = "AMPLIADO"
            else:
                # Esta entrada se logró (o se asumió) en One-way (fallback,
                # positionSide=BOTH): Binance mantiene UN
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
                        hedge_mode=False,
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
                        existing.hedge_mode = False

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
        # La cantidad de cierre también debe respetar el stepSize, y
        # redondeando HACIA ABAJO: pedir cerrar más de lo que hay en la
        # posición hace que Binance rechace la orden reduceOnly.
        try:
            close_filters = await self.api.get_symbol_filters(trade.symbol)
            close_step = close_filters.get("stepSize", 1.0)
            close_qty_dec = floor_to_step(trade.quantity, close_step)
            if close_qty_dec <= 0:
                close_qty_dec = _to_decimal(trade.quantity)
            close_qty = f"{close_qty_dec:.{_step_decimals(close_step)}f}"
        except Exception as e:
            log.warning(f"force_close: no se pudieron leer filtros de {trade.symbol} ({e}); se usa la cantidad tal cual")
            close_qty = _plain_decimal(trade.quantity)

        try:
            await self.api.close_position_market(
                symbol=trade.symbol,
                direction=trade.direction,
                quantity=close_qty,
                position_side=(trade.direction if trade.hedge_mode else "BOTH"),
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
#  SYNC DE BALANCE
# ══════════════════════════════════════════════════════════
async def balance_sync_loop():
    """
    Único responsable de mantener el balance actualizado. Corre
    independiente de las señales de trading, así que la consulta de
    balance queda espaciada en el tiempo y no coincide con el instante
    en que se manda leverage (REST) + orden (WS) al abrir una posición.
    """
    log.info(f"Balance Sync Loop — refrescando balance cada {BALANCE_POLL_S}s")
    while True:
        await execution_manager.refresh_balance(force=True)
        await asyncio.sleep(BALANCE_POLL_S)


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

    # Parseo tolerante: un `trade_id` nulo o no numérico hacía que
    # int(...) lanzara y el endpoint devolviera un 500 sin explicación.
    action = str(data.get("action") or "").lower()
    symbol = str(data.get("symbol") or "").upper()
    try:
        trade_id = int(data.get("trade_id") or 0)
    except (TypeError, ValueError):
        return web.json_response({"ok": False, "error": "trade_id inválido"}, status=400)

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
    # Usa el modo REAL con el que se abrió esta posición puntual, no el
    # flag global HEDGE_MODE — pueden diferir si esta entrada cayó al
    # fallback One-way (ver ExecutionManager._place_entry_order).
    pos_side = trade.direction if trade.hedge_mode else "BOTH"

    try:
        algo_orders = await execution_manager.api.get_open_algo_orders(symbol)
        for o in algo_orders:
            if o.get("type") == order_type:
                await execution_manager.api.cancel_algo_order(o.get("algoId"))
    except Exception as e:
        log.warning(f"_algo_set_tp_sl: no se pudo limpiar {order_type} previo de {symbol}: {e}")

    # Filtros reales del símbolo. Antes se usaba SAFE_DEFAULT_FILTERS
    # (stepSize=1) y se redondeaba la cantidad de la posición a un ENTERO:
    # en un símbolo con posición de 0.4 unidades el TP/SL salía con
    # quantity=1 (o se rechazaba), y con 12.6 cerraba de más o de menos.
    filters = await execution_manager.api.get_symbol_filters(symbol)
    step = filters.get("stepSize", 1.0)
    # Para cerrar hay que redondear HACIA ABAJO: pedir más cantidad de la
    # que existe en la posición hace que Binance rechace el reduceOnly.
    qty_dec = floor_to_step(trade.quantity, step)
    if qty_dec <= 0:
        qty_dec = _to_decimal(filters.get("minQty", step))
    qty_str = f"{qty_dec:.{_step_decimals(step)}f}"

    # El triggerPrice también debe respetar el tickSize del símbolo o
    # Binance lo rechaza con -1111.
    tick = filters.get("tickSize", 0.0)
    trigger_str = round_price_to_tick(trigger_price, tick, side_up=(close_side == "BUY")) if tick else str(trigger_price)

    return await execution_manager.api.create_tp_sl_order(
        symbol=symbol, side=close_side, trigger_price=float(trigger_str),
        order_type=order_type, position_side=pos_side, quantity=qty_str,
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
    """Crea un TAKE_PROFIT_MARKET vía Algo Order (WS API: método
    algoOrder.place — order.place rechaza este tipo con -4120).
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
    # Se usa el modo REAL del trade rastreado (puede diferir del flag
    # global HEDGE_MODE si esa entrada cayó al fallback One-way); si no
    # hay trade local rastreado, se usa el flag global como mejor estimado.
    _trade_for_dir = execution_manager.get_trade(symbol, direction) if direction else None
    if _trade_for_dir is not None:
        wanted_pos_side = direction.upper() if _trade_for_dir.hedge_mode else None
    else:
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
    pos_side = (trade.direction if trade.hedge_mode else "BOTH") if trade else ("BOTH" if not HEDGE_MODE else None)

    # Ajuste a stepSize/tickSize reales: sin esto, una LIMIT manual con
    # decimales "a ojo" se rechaza con -1111 / -1013.
    try:
        lim_filters = await execution_manager.api.get_symbol_filters(symbol)
        qty_send = format_qty(quantity, lim_filters.get("stepSize", 1.0))
        tick = lim_filters.get("tickSize", 0.0)
        price_send = round_price_to_tick(price, tick, side_up=(side == "SELL")) if tick else _plain_decimal(price)
    except Exception as e:
        log.warning(f"manual_limit_order: sin filtros para {symbol} ({e}); se envían los valores tal cual")
        qty_send, price_send = _plain_decimal(quantity), _plain_decimal(price)

    try:
        result = await execution_manager.api.create_limit_order(
            symbol=symbol, side=side, quantity=qty_send, price=price_send,
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

    # Se usa el modo REAL con el que se abrió ESTA posición, no el flag
    # global HEDGE_MODE: pueden diferir si la entrada cayó al fallback
    # One-way, y mandar positionSide=LONG contra una posición BOTH hace
    # que Binance rechace el ajuste de margen.
    pos_side = trade.direction if trade.hedge_mode else "BOTH"
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


async def manual_set_multiplier_mode_handler(request: web.Request) -> web.Response:
    """Cambia el modo del multiplicador de tamaño: 'auto' (por niveles/histéresis) o 'manual'."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        mode = str(data.get("mode", "")).lower()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if mode not in ("auto", "manual"):
        return web.json_response({"ok": False, "error": "mode debe ser 'auto' o 'manual'"}, status=400)

    position_multiplier.mode = mode
    log.warning(f"Multiplicador: modo cambiado a {mode.upper()} desde el dashboard")
    return web.json_response({"ok": True, "mode": mode})


async def manual_set_multiplier_manual_handler(request: web.Request) -> web.Response:
    """Fija el valor del multiplicador a usar cuando el modo es 'manual'."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        value = float(data.get("value", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if value < 0:
        return web.json_response({"ok": False, "error": "value debe ser >= 0"}, status=400)

    position_multiplier.manual_value = value
    log.warning(f"Multiplicador: valor manual fijado a x{value:g} desde el dashboard")
    return web.json_response({"ok": True, "manual_value": value})


async def manual_set_multiplier_balance_source_handler(request: web.Request) -> web.Response:
    """Cambia la fuente del balance de referencia para el multiplicador: 'real' (Binance) o 'ficticio' (pruebas)."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        source = str(data.get("source", "")).lower()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if source not in ("real", "ficticio"):
        return web.json_response({"ok": False, "error": "source debe ser 'real' o 'ficticio'"}, status=400)

    position_multiplier.balance_source = source
    log.warning(f"Multiplicador: fuente de balance cambiada a {source.upper()} desde el dashboard")
    return web.json_response({"ok": True, "balance_source": source})


async def manual_set_multiplier_fictitious_balance_handler(request: web.Request) -> web.Response:
    """Fija el balance ficticio (USDT) usado como referencia cuando la fuente es 'ficticio'."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        data = await request.json()
        value = float(data.get("value", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if value < 0:
        return web.json_response({"ok": False, "error": "value debe ser >= 0"}, status=400)

    position_multiplier.fictitious_balance = value
    log.warning(f"Multiplicador: balance ficticio fijado a {value:.2f} USDT desde el dashboard")
    return web.json_response({"ok": True, "fictitious_balance": value})


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
        "multiplier": position_multiplier.snapshot(em.balance),
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

    const m = d.multiplier;
    if (m) {
      const effStr = 'x' + (Math.round(m.effective_multiplier * 100) / 100);
      q('mult_effective').textContent = effStr;
      let detail = 'Nivel ' + m.level + ' · ref: ' + m.reference_value.toFixed(2) + ' USDT';
      if (m.next_level_at != null) detail += ' · sube a x' + (m.level + 1) + ' en ' + m.next_level_at.toFixed(0);
      if (m.drop_level_at != null) detail += ' · baja si cae de ' + m.drop_level_at.toFixed(0);
      q('mult_detail').textContent = detail;
      q('mult_mode_value').textContent = m.mode === 'manual' ? 'MANUAL' : 'AUTO';
      q('mult_source_value').textContent = m.balance_source === 'ficticio' ? 'FICTICIO (pruebas)' : 'REAL (Binance)';
      const autoBtn = q('mult_mode_auto_btn'), manBtn = q('mult_mode_manual_btn');
      if (autoBtn && manBtn) { autoBtn.disabled = m.mode === 'auto'; manBtn.disabled = m.mode === 'manual'; }
      const realBtn = q('mult_src_real_btn'), fakeBtn = q('mult_src_ficticio_btn');
      if (realBtn && fakeBtn) { realBtn.disabled = m.balance_source === 'real'; fakeBtn.disabled = m.balance_source === 'ficticio'; }
      if (document.activeElement !== q('mult_manual_input')) q('mult_manual_input').value = m.manual_value;
      if (document.activeElement !== q('mult_fake_balance_input')) q('mult_fake_balance_input').value = m.fictitious_balance;
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

async function setMultiplierMode(mode) {
  try {
    const r = await fetch('/manual/set_multiplier_mode', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({mode})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al cambiar el modo del multiplicador'); }
}

async function setMultiplierManual() {
  const val = parseFloat(document.getElementById('mult_manual_input').value);
  if (isNaN(val) || val < 0) { alert('Multiplicador manual inválido'); return; }
  try {
    const r = await fetch('/manual/set_multiplier_manual', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({value: val})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al fijar el multiplicador manual'); }
}

async function setMultiplierSource(source) {
  try {
    const r = await fetch('/manual/set_multiplier_balance_source', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({source})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al cambiar la fuente del balance del multiplicador'); }
}

async function setMultiplierFake() {
  const val = parseFloat(document.getElementById('mult_fake_balance_input').value);
  if (isNaN(val) || val < 0) { alert('Balance ficticio inválido'); return; }
  try {
    const r = await fetch('/manual/set_multiplier_fictitious_balance', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({value: val})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al fijar el balance ficticio'); }
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
    mult_snap = position_multiplier.snapshot(em.balance)
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
    Cambios de leverage por <b>REST{f' vía proxy ({len(PROXY_URLS)} IP(s))' if PROXY_URLS else ''}</b> (la WS API no lo soporta). Leverage configurado: <b>{LEVERAGE}x</b>{' | Modo Hedge' if HEDGE_MODE else ' | Modo One-way'}.
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

  <h2>🔢 Multiplicador de Tamaño (por niveles)</h2>
  <div class="info-banner">
    Escala el <code>quantity</code> de cada señal según el balance (real o ficticio): sube de nivel cada
    <b>+{mult_snap['step']:.0f} USDT</b> (nivel 2 en {mult_snap['step']*2:.0f}, nivel 3 en {mult_snap['step']*3:.0f}, ...) y solo
    BAJA de nivel si el valor cae por debajo de la <b>mitad</b> del umbral de entrada del nivel actual
    (histéresis — evita que un valor oscilando cerca de un umbral, p.ej. 200, haga parpadear el multiplicador).
  </div>
  <div class="grid">
    <div class="card">
      <div class="label">Multiplicador efectivo</div>
      <div class="value" id="mult_effective">x{mult_snap['effective_multiplier']:g}</div>
      <div style="font-size:.68rem;color:#8b949e;margin-top:.2rem" id="mult_detail">Nivel {mult_snap['level']} · ref: {mult_snap['reference_value']:.2f} USDT</div>
    </div>
    <div class="card">
      <div class="label">Modo</div>
      <div class="value" id="mult_mode_value">{'MANUAL' if mult_snap['mode'] == 'manual' else 'AUTO'}</div>
      <div class="lev-row">
        <button id="mult_mode_auto_btn" onclick="setMultiplierMode('auto')" {"disabled" if mult_snap['mode']=='auto' else ""}>Auto</button>
        <button id="mult_mode_manual_btn" onclick="setMultiplierMode('manual')" {"disabled" if mult_snap['mode']=='manual' else ""}>Manual</button>
      </div>
    </div>
    <div class="card">
      <div class="label">Multiplicador manual</div>
      <div class="lev-row">
        <input id="mult_manual_input" type="number" min="0" step="0.1" value="{mult_snap['manual_value']:g}">
        <button onclick="setMultiplierManual()">Aplicar</button>
      </div>
      <div style="font-size:.68rem;color:#8b949e;margin-top:.3rem">Solo se usa cuando el modo está en Manual.</div>
    </div>
    <div class="card">
      <div class="label">Fuente del balance</div>
      <div class="value" id="mult_source_value">{'FICTICIO (pruebas)' if mult_snap['balance_source'] == 'ficticio' else 'REAL (Binance)'}</div>
      <div class="lev-row">
        <button id="mult_src_real_btn" onclick="setMultiplierSource('real')" {"disabled" if mult_snap['balance_source']=='real' else ""}>Real</button>
        <button id="mult_src_ficticio_btn" onclick="setMultiplierSource('ficticio')" {"disabled" if mult_snap['balance_source']=='ficticio' else ""}>Ficticio</button>
      </div>
    </div>
    <div class="card">
      <div class="label">Balance ficticio (USDT)</div>
      <div class="lev-row">
        <input id="mult_fake_balance_input" type="number" min="0" step="any" value="{mult_snap['fictitious_balance']:g}">
        <button onclick="setMultiplierFake()">Aplicar</button>
      </div>
      <div style="font-size:.68rem;color:#8b949e;margin-top:.3rem">Solo aplica cuando la fuente es "Ficticio" — úsalo para probar el multiplicador sin arriesgar dinero real.</div>
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
    app.router.add_post("/manual/set_multiplier_mode", manual_set_multiplier_mode_handler)
    app.router.add_post("/manual/set_multiplier_manual", manual_set_multiplier_manual_handler)
    app.router.add_post("/manual/set_multiplier_balance_source", manual_set_multiplier_balance_source_handler)
    app.router.add_post("/manual/set_multiplier_fictitious_balance", manual_set_multiplier_fictitious_balance_handler)
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
    if PROXY_URLS:
        _labels = ", ".join(BinanceAPI._proxy_label(p) for p in PROXY_URLS)
        log.info(
            f"Proxy(s) configurado(s) para leverage ({len(PROXY_URLS)}): {_labels}. "
            f"Si Binance banea una IP (-1003), se salta a la siguiente automáticamente."
        )
    else:
        log.warning(
            "PROXY_URLS / FIXIE_URL no están configuradas — la llamada REST de leverage "
            "saldrá con la IP directa del proceso (sin whitelisting de IP fija)."
        )

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

    # Precarga de filtros reales por símbolo (una sola request pública,
    # peso 1). Si falla, el bot sigue con SAFE_DEFAULT_FILTERS.
    if USE_EXCHANGE_INFO:
        try:
            await api._refresh_symbol_filters(force=True)
        except Exception as e:
            log.warning(f"main: no se pudo precargar exchangeInfo ({e}) — se usará el default seguro hasta el próximo intento")
    else:
        log.warning(
            "USE_EXCHANGE_INFO=false — sin stepSize real por símbolo, las cantidades se "
            "redondearán a enteros (puede sobredimensionar mucho las posiciones)"
        )

    await execution_manager.refresh_balance(force=True)
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
            f"🌐 <b>Leverage (REST):</b> {f'vía proxy ({len(PROXY_URLS)} IP(s), failover automático)' if PROXY_URLS else '⚠️ sin proxy configurado'}\n"
            f"🔒 <b>Cierre:</b> señal explícita o botón manual\n"
            f"⚠️ <b>Error -2019:</b> posición puede registrarse como asumida",
        )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            start_http_server(),
            price_sync_loop(),
            position_monitor_loop(session),
            balance_sync_loop(),
        )


if __name__ == "__main__":
    asyncio.run(main())
