# Futures Executor

`futures_executor.py` es el servicio que recibe las señales HTTP de `app.py` y las ejecuta en Binance USDT-M Futures.

## Comportamiento actual

- Ejecuta únicamente órdenes indicadas por `app.py` mediante `POST /signal`.
- No calcula take profit.
- No calcula stop loss.
- No crea órdenes bracket/condicionales.
- No invierte direcciones: `LONG` abre compra y `SHORT` abre venta.
- No aplica máximos artificiales de posiciones, notionals ni multiplicadores de cantidad.
- No calcula quantities propias: la cantidad debe venir en el mensaje de `app.py` como `quantity` o `qty`.
- Los cierres llegan desde `app.py` y se ejecutan a mercado con `reduceOnly=True`.
- Incluye dashboard web para ver señales, posiciones abiertas, cierres recientes y cerrar manualmente una operación abierta.

## Variables de entorno

| Variable | Uso |
| --- | --- |
| `BINANCE_API_KEY` | API key de Binance Futures. |
| `BINANCE_API_SECRET` | API secret de Binance Futures. |
| `SIGNAL_SECRET` | Secreto compartido enviado en el header `X-Signal-Secret`. |
| `USE_TESTNET` | `true` para usar testnet; por defecto `false`. |
| `PORT` | Puerto HTTP; por defecto `10000`. |
| `LEVERAGE` / `DEFAULT_LEVERAGE` | Opcional. Si es mayor que cero, se configura antes de abrir. |
| `SIGNAL_DEDUPE_TTL_S` | Ventana simple anti-duplicados; por defecto `10`. |
| `TELEGRAM_BOT_TOKEN` y `TELEGRAM_CHAT_ID` | Opcionales para avisos básicos. |

## Señales aceptadas

Todas las señales deben enviarse a `/signal` con `Content-Type: application/json` y el header:

```http
X-Signal-Secret: <SIGNAL_SECRET>
```

### Apertura

```json
{
  "action": "open",
  "trade_id": 123,
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "price": 65000.0,
  "quantity": 0.001
}
```

Campos obligatorios:

- `action`: `open`.
- `symbol`: símbolo Binance, por ejemplo `BTCUSDT`.
- `direction`: `LONG` o `SHORT`.
- `quantity` o `qty`: cantidad exacta que debe ejecutarse.

`price` se usa solo para registrar el precio esperado desde `app.py`. La ejecución real es una orden `MARKET`.

### Cierre

```json
{
  "action": "close",
  "trade_id": 123,
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "reason": "TP",
  "close_price": 65500.0
}
```

El executor consulta la posición abierta real del símbolo en Binance y la cierra a mercado con `reduceOnly=True`. Si el cierre incluye `quantity` o `qty`, cierra como máximo esa cantidad; si no la incluye, cierra toda la posición del símbolo.

### Cierre global

```json
{
  "action": "close_all"
}
```

Cierra todas las posiciones abiertas detectadas en Binance Futures.

## Endpoints

- `POST /signal`: recibe aperturas, cierres y cierres globales.
- `GET /api/state`: devuelve estado local JSON para que `app.py` pueda consultar actividad real.
- `GET /`: dashboard HTML operativo con métricas, posiciones abiertas, cierres recientes y botón `Cerrar manual` por posición.
- `POST /api/close`: cierre manual desde el dashboard. Body: `{ "symbol": "BTCUSDT" }`.
- `POST /api/close/{symbol}`: alternativa para cierre manual directo por símbolo.

## Dashboard web y cierre manual

La página `GET /` muestra:

- Contadores de señales recibidas, aperturas, cierres y rechazos.
- Tabla de posiciones abiertas registradas localmente.
- Tabla de cierres recientes con PnL estimado según precio de apertura/cierre conocido.
- Botón `Cerrar manual` en cada posición abierta.

El botón llama a `POST /api/close` y ejecuta un cierre a mercado con `reduceOnly=True` y razón `MANUAL_WEB`. No coloca TP/SL ni recalcula cantidad: consulta la posición real de Binance para ese símbolo y la cierra.

## Nota importante sobre `app.py`

Este executor confía en que `app.py` envía cantidades válidas. Si una apertura llega sin `quantity`/`qty` o llega con cero, el executor rechaza la señal en vez de inventar una cantidad. Esto evita volver al comportamiento anterior, donde el executor usaba sizing propio que no coincidía con la estrategia principal.
