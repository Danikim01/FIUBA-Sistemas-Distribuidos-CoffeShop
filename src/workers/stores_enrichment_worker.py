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


def _is_eof(message: Any) -> bool:
    """Detecta mensajes EOF"""
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


class StoresEnrichmentWorker:
    """
    Worker que enriquece resultados de TPV con nombres de stores.
    Consume resultados del TPV worker y los enriquece con store names.
    """

    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.shutdown_requested = False
        # Configurar manejo de SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        # Colas de entrada y salida
        self.stores_input_queue = os.getenv('STORES_INPUT_QUEUE', 'stores_raw')
        self.tpv_input_queue = os.getenv('TPV_INPUT_QUEUE', 'tpv_results')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_final_results')

        # Configuración de prefetch
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))

        # Middleware para stores
        self.stores_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.stores_input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        # Middleware para TPV results
        self.tpv_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.tpv_input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count,
        )

        # Middleware de salida
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port,
        )

        # Almacenamiento de stores (patrón joiner) - solo metadata pequeña
        self.stores_lookup = {}  # {store_id: store_name} - solo metadata esencial
        self.stores_loaded = False
        self.stores_eof_received = False
        self.tpv_eof_received = False

        logger.info(
            "StoresEnrichmentWorker inicializado - Stores: %s, TPV: %s, Output: %s",
            self.stores_input_queue,
            self.tpv_input_queue,
            self.output_queue,
        )

    def _handle_sigterm(self, signum, frame):
        """Maneja la señal SIGTERM para terminar ordenadamente"""
        logger.info("SIGTERM recibido, iniciando shutdown ordenado...")
        self.stores_middleware.stop_consuming()
        self.tpv_middleware.stop_consuming()
        self.shutdown_requested = True
        

    def process_stores_batch(self, stores_batch: List[Dict[str, Any]]) -> None:
        """Procesa un lote de stores y almacena solo metadata esencial"""
        logger.info(f"Procesando lote de stores: {len(stores_batch)} stores")
        for store in stores_batch:
            store_id = store.get('store_id')
            store_name = store.get('store_name', '')
            if store_id is not None:
                # Solo almacenar metadata esencial
                self.stores_lookup[int(store_id)] = store_name
                logger.info("Store metadata almacenada: ID=%s, Name=%s", store_id, store_name)
        
        logger.info("Total stores en lookup: %s", len(self.stores_lookup))

    def process_stores_message(self, message: Any) -> None:
        """Procesa mensajes de stores"""
        if _is_eof(message):
            logger.info("EOF recibido para stores")
            self.stores_eof_received = True
            self.stores_loaded = True
            self.stores_middleware.stop_consuming()  # Cerrar stores después del EOF
            self._check_completion()
            return

        if isinstance(message, list):
            self.process_stores_batch(message)
        

    def enrich_tpv_result(self, tpv_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enriquece un resultado de TPV con el nombre de store desde lookup"""
        try:
            store_id = tpv_result.get('store_id')
            if store_id is None:
                return tpv_result

            # Lookup en metadata almacenada
            store_name = self.stores_lookup.get(int(store_id), '')
            
            # Actualizar el resultado de TPV con el nombre correcto de la store
            enriched_result = {
                **tpv_result,
                'store_name': store_name,
            }
            
            return enriched_result
            
        except Exception as exc:
            logger.error("Error enriqueciendo resultado TPV: %s", exc)
            return tpv_result

    def process_tpv_summary(self, tpv_summary: Dict[str, Any]) -> None:
        """Procesa y enriquece un resumen de TPV"""
        try:
            results = tpv_summary.get('results', [])
            enriched_results = []
            
            for result in results:
                enriched_result = self.enrich_tpv_result(result)
                enriched_results.append(enriched_result)

            # Crear el resumen enriquecido
            enriched_summary = {
                **tpv_summary,
                'results': enriched_results,
            }

            self.output_middleware.send(enriched_summary)
            logger.info("TPV summary enriquecido y enviado con %s resultados", len(enriched_results))
            
        except Exception as exc:
            logger.error("Error procesando TPV summary: %s", exc)

    def process_tpv_message(self, message: Any) -> None:
        """Procesa mensajes del TPV worker"""
        if _is_eof(message):
            logger.info("EOF recibido para TPV")
            self.tpv_eof_received = True
            self._check_completion()
            return

        # Procesar resultado de TPV
        if isinstance(message, dict) and message.get('type') == 'tpv_summary':
            self.process_tpv_summary(message)
        else:
            # Fallback para otros tipos de mensaje
            enriched_result = self.enrich_tpv_result(message)
            self.output_middleware.send(enriched_result)

    def _check_completion(self) -> None:
        """Verifica si se han recibido EOF de ambos tipos de datos"""
        if self.stores_eof_received and self.tpv_eof_received:
            logger.info("Todos los EOF recibidos, enviando EOF de salida")
            self.output_middleware.send({'type': 'EOF'})
            self.tpv_middleware.stop_consuming()

    def start_consuming(self) -> None:
        """Inicia el consumo de mensajes de ambas colas en paralelo"""
        logger.info("Iniciando consumo de stores y TPV results")

        def on_stores_message(message: Any) -> None:
            try:
                if self.shutdown_requested:
                    logger.info("Shutdown requested, stopping stores processing")
                    return
                self.process_stores_message(message)
            except Exception as exc:
                logger.error("Error en callback de stores: %s", exc)

        def on_tpv_message(message: Any) -> None:
            try:
                if self.shutdown_requested:
                    logger.info("Shutdown requested, stopping TPV processing")
                    return
                self.process_tpv_message(message)
            except Exception as exc:
                logger.error("Error en callback de TPV: %s", exc)

        try:
            # Procesar stores para construir lookup table
            self.stores_middleware.start_consuming(on_stores_message)
            logger.info("Iniciando consumo de TPV results")
            # Procesar resultados de TPV
            self.tpv_middleware.start_consuming(on_tpv_message)
            
        except KeyboardInterrupt:
            logger.info("StoresEnrichmentWorker interrumpido por el usuario")
        except Exception as exc:
            logger.error("Error iniciando StoresEnrichmentWorker: %s", exc)
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Limpia recursos"""
        try:
            if hasattr(self, 'stores_middleware') and self.stores_middleware:
                self.stores_middleware.close()
        finally:
            try:
                if hasattr(self, 'tpv_middleware') and self.tpv_middleware:
                    self.tpv_middleware.close()
            finally:
                if hasattr(self, 'output_middleware') and self.output_middleware:
                    self.output_middleware.close()


def main() -> None:
    try:
        worker = StoresEnrichmentWorker()
        worker.start_consuming()
    except Exception as exc:
        logger.error("Error fatal en StoresEnrichmentWorker: %s", exc)
        sys.exit(1)


if __name__ == '__main__':
    main()
