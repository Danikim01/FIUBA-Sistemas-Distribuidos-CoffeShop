#!/usr/bin/env python3

import os
import sys
import logging
import signal
from typing import Any, Dict, List
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_eof_message(message: Any) -> bool:
    """Return True when message carries an EOF control marker."""
    return isinstance(message, dict) and str(message.get("type", "")).upper() == "EOF"


class ResultsWorker:
    """Worker final que reenvía los resultados procesados hacia el gateway."""

    def __init__(self):
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        self.rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
        self.shutdown_requested = False
        
        # Configurar manejo de SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        # Colas de entrada y salida configurables por entorno
        self.input_queue = os.getenv("INPUT_QUEUE", "transactions_final_results")
        self.output_queue = os.getenv("OUTPUT_QUEUE", "gateway_results")

        # Configuración de prefetch para load balancing
        self.prefetch_count = int(os.getenv("PREFETCH_COUNT", 10))

        # Número de EOF esperados desde upstream
        self.expected_eof_count = int(os.getenv("EXPECTED_EOF_COUNT", 1))
        self._eof_seen = 0

        # Middleware para recibir datos
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
        )

        # Middleware para reenviar resultados al gateway
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port,
        )

        self.result_count = 0

        logger.info(
            "ResultsWorker inicializado - Input: %s, Output: %s",
            self.input_queue,
            self.output_queue,
        )

    def _handle_sigterm(self, signum, frame):
        """Maneja la señal SIGTERM para terminar ordenadamente"""
        logger.info("SIGTERM recibido, iniciando shutdown ordenado...")
        self.shutdown_requested = True
        self.input_middleware.stop_consuming()

    def process_result(self, result: Dict[str, Any]) -> None:
        """Reenvía un resultado individual al gateway."""
        try:
            self.output_middleware.send(result)
            self.result_count += 1

            transaction_id = result.get("transaction_id", "unknown")
            final_amount = result.get("final_amount", 0)
            logger.info("Resultado #%s reenviado: %s - $%s", self.result_count, transaction_id, final_amount)
        except Exception as exc:
            logger.error("Error procesando resultado: %s", exc)

    def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Procesa un lote de resultados (chunk) y los reenvía al gateway como chunk."""
        try:
            # Reenviar chunk completo al gateway
            self.output_middleware.send(batch)
            logger.info("Procesado chunk de %s resultados", len(batch))
        except Exception as exc:
            logger.error("Error procesando chunk de resultados: %s", exc)

    def _handle_message(self, message: Any) -> None:
        """Procesa cualquier mensaje recibido desde la cola."""
        if is_eof_message(message):
            self._eof_seen += 1
            logger.info(
                "Recibido EOF en ResultsWorker (%s/%s)",
                self._eof_seen,
                self.expected_eof_count,
            )

            if self._eof_seen >= self.expected_eof_count:
                logger.info("Todos los EOF recibidos; reenviando EOF al gateway")
                try:
                    self.output_middleware.send({"type": "EOF"})
                finally:
                    self.input_middleware.stop_consuming()
            return

        if isinstance(message, list):
            self.process_batch(message)
        else:
            self.process_result(message)

    def start_consuming(self) -> None:
        """Inicia el consumo de mensajes de la cola de entrada."""
        try:
            logger.info("Iniciando ResultsWorker...")

            def on_message(message: Any) -> None:
                try:
                    if self.shutdown_requested:
                        logger.info("Shutdown requested, stopping message processing")
                        return
                        
                    self._handle_message(message)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Error en callback de mensaje: %s", exc)

            self.input_middleware.start_consuming(on_message)

        except KeyboardInterrupt:
            logger.info("ResultsWorker interrumpido por el usuario")
        except Exception as exc:
            logger.error("Error iniciando consumo: %s", exc)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Limpia recursos al finalizar."""
        try:
            if hasattr(self, 'input_middleware') and self.input_middleware:
                self.input_middleware.close()
            if hasattr(self, 'output_middleware') and self.output_middleware:
                self.output_middleware.close()
            logger.info("ResultsWorker finalizado - total reenviado: %s", self.result_count)
        except Exception as exc:
            logger.warning("Error limpiando recursos: %s", exc)


def main() -> None:
    """Punto de entrada principal."""
    try:
        worker = ResultsWorker()
        worker.start_consuming()
    except Exception as exc:  # noqa: BLE001
        logger.error("Error en main: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
