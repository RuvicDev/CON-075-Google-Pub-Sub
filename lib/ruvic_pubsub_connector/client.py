"""Cliente de mensajería en Google Cloud Pub/Sub.

Capacidades:
- publish_message():    publica un mensaje en el tópico configurado.
- create_subscription(): crea una suscripción pull sobre el tópico configurado.
- consume_messages():   consume (pull + ack) mensajes de una suscripción.

Las credenciales SIEMPRE provienen de variables de entorno
RUVIC_PUBSUB_* (ver config.PubSubConfig.from_env). Prohibido
hardcodearlas.

El conector opera sobre un único tópico configurado (principio de
mínimo privilegio: el rol IAM de la cuenta de servicio debe limitarse
a ese tópico y a sus suscripciones).
"""

from __future__ import annotations

from typing import Any

from google.api_core.exceptions import (
    AlreadyExists,
    Forbidden,
    GoogleAPICallError,
    NotFound,
    ServiceUnavailable,
    Unauthorized,
)
from google.auth.exceptions import GoogleAuthError, RefreshError
from google.oauth2 import service_account
from google.cloud import pubsub_v1

from .config import PubSubConfig
from .exceptions import (
    PubSubAuthError,
    PubSubConnectorError,
    PubSubDataError,
    PubSubNetworkError,
)
from .logging_utils import get_logger


def _wrap_error(exc: GoogleAPICallError, not_found_message: str) -> PubSubConnectorError:
    """Traduce un error de la API de Pub/Sub a una excepción propia, sin
    dejar escapar nunca el tipo crudo del SDK."""
    if isinstance(exc, (Forbidden, Unauthorized)):
        return PubSubAuthError(
            "Credenciales inválidas o sin permiso IAM suficiente sobre este "
            "tópico/suscripción. Revisa el rol asignado a la cuenta de servicio."
        )
    if isinstance(exc, NotFound):
        return PubSubDataError(not_found_message)
    if isinstance(exc, ServiceUnavailable):
        return PubSubNetworkError(
            "Google Cloud Pub/Sub no respondió correctamente. Puede ser un "
            "límite de cuota temporal; reintenta en unos segundos."
        )
    return PubSubDataError(f"Error de datos: {exc}")


class PubSubClient:
    """Cliente de mensajería sobre el tópico configurado de Google Cloud Pub/Sub.

    Args:
        config: configuración de conexión. Si se omite, se lee de las
            variables de entorno RUVIC_PUBSUB_* (comportamiento estándar
            en el runtime de la plataforma).

    Ejemplo:
        >>> client = PubSubClient()  # lee RUVIC_PUBSUB_* del entorno
        >>> client.publish_message("hola mundo")
    """

    def __init__(self, config: PubSubConfig | None = None) -> None:
        self.config = config or PubSubConfig.from_env()
        self._logger = get_logger()
        self._publisher: pubsub_v1.PublisherClient | None = None
        self._subscriber: pubsub_v1.SubscriberClient | None = None

    # ------------------------------------------------------------------ #
    # Conexión
    # ------------------------------------------------------------------ #

    def _get_credentials(self) -> service_account.Credentials:
        try:
            return service_account.Credentials.from_service_account_info(
                self.config.service_account_info
            )
        except (ValueError, KeyError) as exc:
            raise PubSubAuthError(
                f"El JSON de la cuenta de servicio es inválido o le faltan campos: {exc}"
            ) from exc

    def _get_publisher(self) -> pubsub_v1.PublisherClient:
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient(credentials=self._get_credentials())
        return self._publisher

    def _get_subscriber(self) -> pubsub_v1.SubscriberClient:
        if self._subscriber is None:
            self._subscriber = pubsub_v1.SubscriberClient(credentials=self._get_credentials())
        return self._subscriber

    def _project_id(self) -> str:
        return self.config.service_account_info["project_id"]

    def _topic_path(self) -> str:
        return self._get_publisher().topic_path(self._project_id(), self.config.topic_name)

    def ping(self) -> bool:
        """Verifica la conexión comprobando acceso al tópico configurado.

        Returns:
            True si la conexión funciona.

        Raises:
            PubSubAuthError / PubSubNetworkError / PubSubDataError según el fallo.
        """
        try:
            self._get_publisher().get_topic(request={"topic": self._topic_path()})
        except (RefreshError, GoogleAuthError) as exc:
            raise PubSubAuthError(
                "No se pudo autenticar con la cuenta de servicio. Verifica que "
                "el JSON de credenciales sea correcto y no esté revocado."
            ) from exc
        except GoogleAPICallError as exc:
            raise _wrap_error(
                exc, f"El tópico {self.config.topic_name!r} no existe o no es accesible."
            ) from exc
        self._logger.info("Ping exitoso al tópico %s", self.config.topic_name)
        return True

    # ------------------------------------------------------------------ #
    # Capacidad 1: publicar un mensaje
    # ------------------------------------------------------------------ #

    def publish_message(self, data: str, attributes: dict[str, str] | None = None) -> str:
        """Publica un mensaje en el tópico configurado.

        Args:
            data: contenido del mensaje (texto).
            attributes: atributos opcionales clave-valor del mensaje.

        Returns:
            El message_id asignado por Pub/Sub.

        Ejemplo:
            >>> client.publish_message("pedido #1234 procesado")
        """
        if not data:
            raise PubSubDataError("data no puede estar vacío.")
        try:
            future = self._get_publisher().publish(
                self._topic_path(), data.encode("utf-8"), **(attributes or {})
            )
            message_id = future.result(timeout=self.config.connect_timeout)
        except (RefreshError, GoogleAuthError) as exc:
            raise PubSubAuthError(f"No se pudo autenticar: {exc}") from exc
        except GoogleAPICallError as exc:
            raise _wrap_error(
                exc, f"El tópico {self.config.topic_name!r} no existe o no es accesible."
            ) from exc
        self._logger.info("Mensaje publicado en %s: %s", self.config.topic_name, message_id)
        return message_id

    # ------------------------------------------------------------------ #
    # Capacidad 2: crear una suscripción
    # ------------------------------------------------------------------ #

    def create_subscription(self, subscription_name: str) -> dict[str, Any]:
        """Crea una suscripción pull sobre el tópico configurado.

        Args:
            subscription_name: nombre de la nueva suscripción.

        Returns:
            Dict con: name, topic.

        Ejemplo:
            >>> client.create_subscription("mi-suscripcion")
        """
        subscriber = self._get_subscriber()
        subscription_path = subscriber.subscription_path(self._project_id(), subscription_name)
        try:
            subscription = subscriber.create_subscription(
                request={"name": subscription_path, "topic": self._topic_path()}
            )
        except AlreadyExists as exc:
            raise PubSubDataError(
                f"La suscripción {subscription_name!r} ya existe."
            ) from exc
        except (RefreshError, GoogleAuthError) as exc:
            raise PubSubAuthError(f"No se pudo autenticar: {exc}") from exc
        except GoogleAPICallError as exc:
            raise _wrap_error(
                exc, f"El tópico {self.config.topic_name!r} no existe o no es accesible."
            ) from exc
        self._logger.info("Suscripción creada: %s", subscription_name)
        return {"name": subscription.name, "topic": subscription.topic}

    # ------------------------------------------------------------------ #
    # Capacidad 3: consumir mensajes
    # ------------------------------------------------------------------ #

    def consume_messages(self, subscription_name: str, max_messages: int = 10) -> list[dict[str, Any]]:
        """Consume (pull + confirma) mensajes disponibles en una suscripción.

        Los mensajes se confirman (ack) inmediatamente al recibirse y no
        vuelven a entregarse.

        Args:
            subscription_name: nombre de la suscripción a consumir.
            max_messages: máximo de mensajes a consumir (default 10).

        Returns:
            Lista de dicts: {"message_id", "data", "attributes"}.

        Ejemplo:
            >>> client.consume_messages("mi-suscripcion", max_messages=5)
        """
        subscriber = self._get_subscriber()
        subscription_path = subscriber.subscription_path(self._project_id(), subscription_name)
        try:
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": max_messages},
                timeout=self.config.connect_timeout,
            )
        except (RefreshError, GoogleAuthError) as exc:
            raise PubSubAuthError(f"No se pudo autenticar: {exc}") from exc
        except GoogleAPICallError as exc:
            raise _wrap_error(
                exc, f"La suscripción {subscription_name!r} no existe."
            ) from exc

        if not response.received_messages:
            self._logger.info("No había mensajes disponibles en %s", subscription_name)
            return []

        ack_ids = [msg.ack_id for msg in response.received_messages]
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
        result = [
            {
                "message_id": msg.message.message_id,
                "data": msg.message.data.decode("utf-8"),
                "attributes": dict(msg.message.attributes),
            }
            for msg in response.received_messages
        ]
        self._logger.info(
            "Consumidos y confirmados %d mensajes de %s", len(result), subscription_name
        )
        return result
