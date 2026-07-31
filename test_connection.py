"""Prueba de conexión estándar del conector pubsub.

Firma estándar Ruvic: def test_connection() -> tuple[bool, str]
- Lee la configuración EXCLUSIVAMENTE de las env vars RUVIC_PUBSUB_*.
- Nunca lanza excepciones; retorna (ok, mensaje).

Ejecutable también como script para pruebas locales:
    python test_connection.py
"""

from __future__ import annotations


def test_connection() -> tuple[bool, str]:
    """Conecta a Google Cloud Pub/Sub y verifica acceso al tópico usando
    las env vars RUVIC_PUBSUB_*."""
    try:
        from ruvic_pubsub_connector import (
            PubSubAuthError,
            PubSubClient,
            PubSubDataError,
            PubSubNetworkError,
        )
    except ImportError:
        return (
            False,
            "La librería ruvic-pubsub-connector no está instalada. "
            "Instala con: pip install git+https://github.com/Dgirto/"
            "Google-Pub-Sub.git#subdirectory=lib",
        )

    try:
        client = PubSubClient()  # valida que existan las env vars
    except ValueError as exc:
        return False, str(exc)

    try:
        client.ping()
    except PubSubAuthError as exc:
        return False, f"Autenticación fallida: {exc}"
    except PubSubNetworkError as exc:
        return False, f"Error de red: {exc}"
    except PubSubDataError as exc:
        return False, f"Error de datos: {exc}"
    except Exception as exc:  # red de seguridad: jamás propagar
        return False, f"Error inesperado: {exc}"

    return (
        True,
        f"Conexión exitosa a Google Pub/Sub (tópico={client.config.topic_name!r})",
    )


if __name__ == "__main__":
    ok, message = test_connection()
    print(f"{'OK' if ok else 'FALLO'}: {message}")
    raise SystemExit(0 if ok else 1)
