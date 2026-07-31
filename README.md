# Conector Google Pub/Sub (CON-075)

Conector Ruvic de mensajería empresarial en Google Cloud Pub/Sub.
Permite publicar un mensaje en el tópico configurado, crear
suscripciones pull sobre ese tópico, y consumir mensajes de una
suscripción.

## Instalación

```bash
pip install git+https://github.com/Dgirto/Google-Pub-Sub.git#subdirectory=lib
```

Python 3.10+. Dependencia: `google-cloud-pubsub`.

## Permisos requeridos en Google Cloud

Creá una **cuenta de servicio dedicada** y otorgale, limitado al
tópico configurado (o al proyecto si el tópico no admite condiciones
IAM finas):

- **"Pub/Sub Publisher"**: necesario para `publish_message`.
- **"Pub/Sub Editor"** (o "Pub/Sub Subscriber" + permiso de creación
  de suscripciones): necesario para `create_subscription` y
  `consume_messages`.

No otorgues el rol **"Pub/Sub Admin"** a nivel de proyecto completo
si podés limitarlo al tópico específico.

## Variables de entorno (`RUVIC_PUBSUB_*`)

| Variable | Obligatoria | Descripción |
|----------|-------------|-------------|
| `RUVIC_PUBSUB_SERVICE_ACCOUNT_JSON` | Sí | Contenido completo del JSON de la cuenta de servicio |
| `RUVIC_PUBSUB_TOPIC_NAME` | Sí | Nombre del tópico |
| `RUVIC_PUBSUB_CONNECT_TIMEOUT` | No (default `10`) | Timeout de conexión en segundos |

## Pruebas locales

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ./lib

export RUVIC_PUBSUB_SERVICE_ACCOUNT_JSON="$(cat service-account.json)"
export RUVIC_PUBSUB_TOPIC_NAME=tu-topico-de-prueba

python test_connection.py
python validate_local.py
```

## Notas de integración

- `publish_message` **SÍ escribe** un mensaje real en el tópico.
  `create_subscription` **SÍ crea** un recurso real en el proyecto.
  `consume_messages` **SÍ confirma (ack)** los mensajes recibidos —
  no vuelven a entregarse.
- `consume_messages` retorna una lista vacía si no había mensajes
  disponibles — no es un error.
- Los mensajes se publican y consumen como texto plano (UTF-8); si
  necesitás enviar JSON estructurado, serializalo vos antes de llamar
  a `publish_message`.
- La cuenta de servicio se pasa como JSON completo (no como archivo en
  disco), siguiendo el mismo patrón que el conector de Google Cloud
  Storage.
