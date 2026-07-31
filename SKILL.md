---
name: pubsub
description: Publicar mensajes en Google Pub/Sub, crear suscripciones y consumir mensajes
---

# Google Pub/Sub

Conector para mensajería empresarial en Google Cloud Pub/Sub sobre el
tópico configurado.

## Credenciales

Usa una cuenta de servicio de Google Cloud (JSON completo) autenticada
vía `google.oauth2.service_account.Credentials`. Nunca hardcodees ni
loguees el contenido del JSON. La cuenta de servicio debe tener
asignados, limitados al tópico:

- **"Pub/Sub Publisher"** — requerido para `publish_message`.
- **"Pub/Sub Editor"** o **"Pub/Sub Subscriber"** con permiso de
  creación de suscripciones — requerido para `create_subscription` y
  `consume_messages`.

No uses el rol "Pub/Sub Admin" a nivel de proyecto si podés
limitarlo al tópico.

## Capacidades

### `publish_message(data: str, attributes: dict | None = None) -> str`

Publica un mensaje real en el tópico configurado. Retorna el
message_id.

```python
message_id = client.publish_message("pedido #1234 procesado")
```

### `create_subscription(subscription_name: str) -> dict`

Crea una suscripción pull real sobre el tópico configurado.

```python
client.create_subscription("mi-suscripcion")
```

### `consume_messages(subscription_name: str, max_messages: int = 10) -> list[dict]`

Consume y confirma (ack) mensajes de una suscripción. Los mensajes
confirmados no se vuelven a entregar.

```python
mensajes = client.consume_messages("mi-suscripcion", max_messages=5)
```

## Manejo de errores

Todas las excepciones heredan de `PubSubConnectorError`:

- `PubSubAuthError`: credenciales inválidas o rol IAM insuficiente.
- `PubSubNetworkError`: Pub/Sub inalcanzable o límite de cuota
  temporal.
- `PubSubDataError`: tópico o suscripción inexistente, u otro error
  de datos.

Nunca dejes propagar excepciones crudas del SDK
(`google.api_core.exceptions.*`) — siempre se traducen a estas.

## Buenas prácticas

- Tratá `publish_message`, `create_subscription` y `consume_messages`
  como operaciones con efectos secundarios reales (escritura,
  creación de recursos y confirmación/eliminación respectivamente);
  no las ejecutes contra un tópico de producción sin confirmar con
  el usuario.
- Si `consume_messages` retorna una lista vacía repetidamente,
  verificá que otro consumidor no esté vaciando la suscripción antes
  de asumir un fallo de conexión.
