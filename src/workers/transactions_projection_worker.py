#!/usr/bin/env python3

import os
import sys
import logging
import signal
from typing import Any, Dict, List, Optional

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_eof(message: Any) -> bool:
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


class TransactionsProjectionWorker:
    """Reduce cada transaccion a las columnas necesarias para el analisis."""

    def __init__(self) -> None:
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.shutdown_requested = False

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.input_queue = os.getenv('INPUT_QUEUE', 'transactions_year_for_top_clients')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_compact')
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))

        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port,
        )

        logger.info(
            "TransactionsProjectionWorker inicializado - Input: %s, Output: %s, Prefetch: %s",
            self.input_queue,
            self.output_queue,
            self.prefetch_count,
        )

    def _handle_sigterm(self, signum, frame):
        logger.info("SIGTERM recibido, deteniendo consumo de transacciones proyectadas")
        self.shutdown_requested = True
        self.input_middleware.stop_consuming()

    def _project_transaction(self, transaction: Dict[str, Any]) -> Optional[Dict[str, int]]:
        try:
            store_id_raw = transaction.get('store_id')
            user_id_raw = transaction.get('user_id')
            if store_id_raw is None or user_id_raw is None:
                return None

            store_id = int(float(store_id_raw))
            user_id = int(float(user_id_raw))
            if user_id <= 0:
                return None
            return {
                'store_id': store_id,
                'user_id': user_id,
            }
        except (TypeError, ValueError):
            logger.debug("Transaccion con IDs invalidos omitida: %s", transaction)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error("Error inesperado proyectando transaccion: %s", exc)
            return None

    def process_transaction(self, transaction: Dict[str, Any]) -> None:
        projected = self._project_transaction(transaction)
        if projected is not None:
            self.output_middleware.send(projected)

    def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        projected_batch: List[Dict[str, int]] = []
        for transaction in batch:
            projected = self._project_transaction(transaction)
            if projected is not None:
                projected_batch.append(projected)

        if projected_batch:
            self.output_middleware.send(projected_batch)

    def start_consuming(self) -> None:
        logger.info("TransactionsProjectionWorker iniciando consumo")

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown solicitado, deteniendo procesamiento de proyeccion")
                return

            if _is_eof(message):
                self.output_middleware.send({'type': 'EOF'})
                self.input_middleware.stop_consuming()
                return

            if isinstance(message, list):
                self.process_batch(message)
            else:
                self.process_transaction(message)

        try:
            self.input_middleware.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info("TransactionsProjectionWorker interrumpido por el usuario")
        except Exception as exc:  # noqa: BLE001
            logger.error("Error en TransactionsProjectionWorker: %s", exc)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        try:
            if hasattr(self, 'input_middleware') and self.input_middleware:
                self.input_middleware.close()
        finally:
            if hasattr(self, 'output_middleware') and self.output_middleware:
                self.output_middleware.close()


def main() -> None:
    try:
        worker = TransactionsProjectionWorker()
        worker.start_consuming()
    except Exception as exc:  # noqa: BLE001
        logger.error("Error fatal en TransactionsProjectionWorker: %s", exc)
        sys.exit(1)


if __name__ == '__main__':
    main()
