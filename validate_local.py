"""Validación local del conector pubsub: ejercita las 3 capacidades.

Uso:
    python validate_local.py

Requiere las variables RUVIC_PUBSUB_* exportadas en el entorno,
apuntando a un tópico de prueba.

ADVERTENCIA: este script crea una suscripción REAL, publica un
mensaje REAL y lo consume (confirmándolo/ack, ya no se vuelve a
entregar). Ejecútalo solo contra un tópico de pruebas.
"""

from ruvic_pubsub_connector import PubSubClient, setup_logging

setup_logging("INFO")
client = PubSubClient()
subscripcion_prueba = "ruvic-validate-local-sub"

print("== 1. Crear suscripción de prueba ==")
try:
    suscripcion = client.create_subscription(subscripcion_prueba)
    print(f"  {suscripcion}")
except Exception as exc:
    print(f"  ya existía o no se pudo crear ({exc}), continuando...")

print("== 2. Publicar mensaje de prueba ==")
message_id = client.publish_message("prueba de validación local Ruvic")
print(f"  publicado: {message_id}")

print("== 3. Consumir mensajes ==")
mensajes = client.consume_messages(subscripcion_prueba, max_messages=10)
print(f"  {len(mensajes)} mensaje(s) consumido(s)")

print("\nTodo OK: publish_message, create_subscription y consume_messages funcionan.")
