"""Jerarquía de excepciones del conector Google Pub/Sub."""

from __future__ import annotations


class PubSubConnectorError(Exception):
    """Error base del conector Google Pub/Sub."""


class PubSubAuthError(PubSubConnectorError):
    """Credenciales inválidas o cuenta de servicio sin permiso suficiente."""


class PubSubNetworkError(PubSubConnectorError):
    """Error de red o timeout al conectar con Google Cloud Pub/Sub."""


class PubSubDataError(PubSubConnectorError):
    """Tópico/suscripción inexistente u otro error de datos."""
