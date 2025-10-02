#!/usr/bin/env python3

import os
import sys
import logging
import signal
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_eof(message: Any) -> bool:
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


class TopClientsWorker:
    """Calcula los clientes mas frecuentes por sucursal para 2024/2025."""

    def __init__(self) -> None:
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.shutdown_requested = False

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.transactions_queue_name = os.getenv('TRANSACTIONS_QUEUE', 'transactions_compact')
        self.users_queue_name = os.getenv('USERS_QUEUE', 'users_raw')
        self.stores_queue_name = os.getenv('STORES_QUEUE', 'stores_for_top_clients')
        self.output_queue_name = os.getenv('OUTPUT_QUEUE', 'transactions_final_results')
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))
        self.top_levels = int(os.getenv('TOP_LEVELS', 3))

        self.transactions_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.transactions_queue_name,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )
        self.users_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.users_queue_name,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )
        self.stores_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.stores_queue_name,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue_name,
            port=self.rabbitmq_port,
        )

        # Lookup tables y contadores acumulados
        self.store_lookup: Dict[int, str] = {}
        self.user_lookup: Dict[int, Dict[str, Any]] = {}
        self.counts_by_store: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))

        self.stores_loaded = False
        self.users_loaded = False
        self.results_emitted = False

        logger.info(
            "TopClientsWorker inicializado - Transactions: %s, Users: %s, Stores: %s, Output: %s",
            self.transactions_queue_name,
            self.users_queue_name,
            self.stores_queue_name,
            self.output_queue_name,
        )

    def _handle_sigterm(self, signum, frame):
        logger.info("SIGTERM recibido, iniciando shutdown ordenado en TopClientsWorker")
        self.shutdown_requested = True
        self.transactions_middleware.stop_consuming()
        self.users_middleware.stop_consuming()
        self.stores_middleware.stop_consuming()

    def process_stores_batch(self, batch: List[Dict[str, Any]]) -> None:
        for store in batch:
            try:
                store_id = int(float(store.get('store_id')))
                store_name = store.get('store_name', '')
                self.store_lookup[store_id] = store_name
            except (TypeError, ValueError):
                logger.debug("Store invalida omitida: %s", store)

    def _consume_stores(self) -> None:
        logger.info("TopClientsWorker cargando metadata de stores")

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown solicitado, deteniendo carga de stores")
                self.stores_middleware.stop_consuming()
                return

            if _is_eof(message):
                self.stores_loaded = True
                self.stores_middleware.stop_consuming()
                return

            if isinstance(message, list):
                self.process_stores_batch(message)
            else:
                self.process_stores_batch([message])

        try:
            self.stores_middleware.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info("Carga de stores interrumpida" )
        except Exception as exc:  # noqa: BLE001
            logger.error("Error cargando stores: %s", exc)
        finally:
            logger.info("Stores procesados: %s", len(self.store_lookup))

    def process_users_batch(self, batch: List[Dict[str, Any]]) -> None:
        for user in batch:
            try:
                user_id = int(float(user.get('user_id')))
            except (TypeError, ValueError):
                logger.debug("Usuario invalido omitido: %s", user)
                continue

            birthdate = user.get('birthdate', '')
            self.user_lookup[user_id] = {
                'birthdate': birthdate,
            }

    def _consume_users(self) -> None:
        logger.info("TopClientsWorker cargando metadata de usuarios")

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown solicitado, deteniendo carga de usuarios")
                self.users_middleware.stop_consuming()
                return

            if _is_eof(message):
                self.users_loaded = True
                self.users_middleware.stop_consuming()
                return

            if isinstance(message, list):
                self.process_users_batch(message)
            else:
                self.process_users_batch([message])

        try:
            self.users_middleware.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info("Carga de usuarios interrumpida")
        except Exception as exc:  # noqa: BLE001
            logger.error("Error cargando usuarios: %s", exc)
        finally:
            logger.info("Usuarios procesados: %s", len(self.user_lookup))

    def _update_counts(self, transaction: Dict[str, Any]) -> None:
        try:
            store_id = int(transaction['store_id'])
            user_id = int(transaction['user_id'])
        except (KeyError, TypeError, ValueError):
            logger.debug("Transaccion compacta invalida: %s", transaction)
            return

        if user_id <= 0:
            return

        self.counts_by_store[store_id][user_id] += 1

    def process_transactions_batch(self, batch: List[Dict[str, Any]]) -> None:
        for transaction in batch:
            self._update_counts(transaction)

    def process_transaction(self, transaction: Dict[str, Any]) -> None:
        self._update_counts(transaction)

    def _compute_results(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for store_id, user_counts in self.counts_by_store.items():
            if not user_counts:
                continue

            sorted_counts: List[Tuple[int, int]] = sorted(
                user_counts.items(),
                key=lambda item: (item[1], -item[0]),
                reverse=True,
            )

            unique_levels: List[int] = []
            for _, count in sorted_counts:
                if count not in unique_levels:
                    unique_levels.append(count)
                if len(unique_levels) >= self.top_levels:
                    break

            if not unique_levels:
                continue

            threshold = unique_levels[-1]
            store_name = self.store_lookup.get(store_id)
            if not store_name:
                continue

            limited_results: List[Dict[str, Any]] = []

            for user_id, count in sorted_counts:
                if count < threshold:
                    break

                birthdate = (self.user_lookup.get(user_id, {}) or {}).get('birthdate', '')
                if not birthdate or not str(birthdate).strip():
                    continue

                limited_results.append(
                    {
                        'store_id': store_id,
                        'store_name': store_name,
                        'user_id': user_id,
                        'birthdate': birthdate,
                        'purchases_qty': count,
                    }
                )

                if len(limited_results) >= 35:
                    break

            results.extend(limited_results)

        results.sort(
            key=lambda item: (
                item['store_name'],
                -item['purchases_qty'],
                item['birthdate'],
                item['user_id'],
            )
        )

        return results

    def _emit_results(self) -> None:
        if self.results_emitted:
            return

        results = self._compute_results()
        payload = {
            'type': 'top_clients_birthdays',
            'results': results,
        }

        self.output_middleware.send(payload)
        self.results_emitted = True
        logger.info("TopClientsWorker envio %s resultados", len(results))

    def _consume_transactions(self) -> None:
        logger.info("TopClientsWorker procesando transacciones compactas")

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                logger.info("Shutdown solicitado, deteniendo procesamiento de transacciones")
                self.transactions_middleware.stop_consuming()
                return

            if _is_eof(message):
                self._emit_results()
                self.output_middleware.send({'type': 'EOF', 'source': 'top_clients_birthdays'})
                self.transactions_middleware.stop_consuming()
                return

            if isinstance(message, list):
                self.process_transactions_batch(message)
            else:
                self.process_transaction(message)

        try:
            self.transactions_middleware.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info("Procesamiento de transacciones interrumpido")
        except Exception as exc:  # noqa: BLE001
            logger.error("Error procesando transacciones: %s", exc)
        finally:
            logger.info("TopClientsWorker finalizo el procesamiento de transacciones")

    def start_consuming(self) -> None:
        try:
            self._consume_stores()
            self._consume_users()
            self._consume_transactions()
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        try:
            if hasattr(self, 'stores_middleware') and self.stores_middleware:
                self.stores_middleware.close()
        finally:
            try:
                if hasattr(self, 'users_middleware') and self.users_middleware:
                    self.users_middleware.close()
            finally:
                try:
                    if hasattr(self, 'transactions_middleware') and self.transactions_middleware:
                        self.transactions_middleware.close()
                finally:
                    if hasattr(self, 'output_middleware') and self.output_middleware:
                        self.output_middleware.close()


def main() -> None:
    try:
        worker = TopClientsWorker()
        worker.start_consuming()
    except Exception as exc:  # noqa: BLE001
        logger.error("Error fatal en TopClientsWorker: %s", exc)
        sys.exit(1)


if __name__ == '__main__':
    main()
