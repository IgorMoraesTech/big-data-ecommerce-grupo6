# Contrato de Eventos

Formato: um JSON por linha (`JSONL`).

## Campos
- `event_id`: UUID/string
- `event_type`: `click`, `add_to_cart`, `purchase`, `shipment_update`
- `event_time`: ISO-8601 UTC, ex. `2026-09-24T21:15:30.123Z`
- `user_id`: string
- `session_id`: string
- `product_id`: string ou null
- `category`: string ou null
- `quantity`: inteiro ou null
- `unit_price`: número ou null
- `order_id`: string ou null
- `shipping_status`: string ou null

## Regras
- `click`: produto e categoria.
- `add_to_cart`: produto, categoria, quantidade e preço.
- `purchase`: produto, categoria, quantidade, preço e pedido.
- `shipment_update`: pedido e status logístico.

Status logísticos sugeridos:
`processing`, `shipped`, `in_transit`, `delivered`, `delayed`.

O gerador pode atrasar uma pequena parcela dos eventos de propósito para permitir a demonstração de eventos fora de ordem e watermarks no Flink.
