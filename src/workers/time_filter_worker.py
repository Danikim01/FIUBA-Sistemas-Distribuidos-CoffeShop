#!/usr/bin/env python3

import os
import sys
import logging
import signal
from typing import Any
from datetime import datetime, time
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimeFilterWorker:
    """
    Worker que filtra transacciones por hora (06:00 AM - 11:00 PM).
    Recibe transacciones filtradas por año y las filtra por hora.
    """
    
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.shutdown_requested = False
        
        # Configurar manejo de SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        # Colas de entrada y salida configurables por entorno
        self.input_queue = os.getenv('INPUT_QUEUE', 'transactions_enriched')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_time_filtered')

        # Permitir fan-out a múltiples colas configuradas por coma
        raw_output_queues = os.getenv('OUTPUT_QUEUES')
        if raw_output_queues:
            queue_names = [name.strip() for name in raw_output_queues.split(',') if name.strip()]
        else:
            queue_names = []

        if not queue_names:
            queue_names = [self.output_queue]
        elif self.output_queue and self.output_queue not in queue_names:
            # Mantener compatibilidad si OUTPUT_QUEUE también está configurado
            queue_names.insert(0, self.output_queue)
        self.output_queue_names = queue_names
        
        # Configuración de prefetch para load balancing
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))
        
        # Middleware para recibir datos con prefetch optimizado
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count
        )
        
        # Middleware para enviar datos filtrados a cada destino configurado
        self.output_middlewares = [
            RabbitMQMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=queue_name,
                port=self.rabbitmq_port
            )
            for queue_name in self.output_queue_names
        ]
        
        # Definir rango de horas (06:00 AM - 11:00 PM)
        self.start_time = time(6, 0)   # 06:00 AM
        self.end_time = time(23, 0)    # 11:00 PM (23:00)
        
        logger.info(
            "TimeFilterWorker inicializado - Input: %s, Output: %s, Prefetch: %s",
            self.input_queue,
            self.output_queue_names,
            self.prefetch_count
        )

    def _handle_sigterm(self, signum, frame):
        """Maneja la señal SIGTERM para terminar ordenadamente"""
        logger.info("SIGTERM recibido, iniciando shutdown ordenado...")
        self.input_middleware.stop_consuming()
        self.shutdown_requested = True
        # Filtro de hora configurado
    
    def filter_by_time(self, transaction):
        """
        Filtra una transacción por hora (06:00 AM - 11:00 PM).
        
        Args:
            transaction: Diccionario con los datos de la transacción
            
        Returns:
            bool: True si la transacción cumple el filtro de hora
        """
        try:
            # Extraer la fecha y hora de created_at
            created_at = transaction.get('created_at', '')
            if not created_at:
                return False
            
            # Parsear la fecha y hora (formato esperado: YYYY-MM-DD HH:MM:SS)
            datetime_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            transaction_time = datetime_obj.time()
            
            # Verificar si está en el rango de horas
            # Considerar que 11:00 PM es 23:00, así que el rango es 06:00-23:00
            return self.start_time <= transaction_time <= self.end_time
            
        except ValueError:
            return False
        except Exception:
            return False
    
    def _is_eof(self, message: Any) -> bool:
        return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'

    def process_transaction(self, transaction):
        """
        Procesa una transacción individual.
        
        Args:
            transaction: Diccionario con los datos de la transacción
        """
        try:
            # Aplicar filtro de hora
            if self.filter_by_time(transaction):
                # Enviar transacción filtrada a todos los destinos configurados
                for middleware in self.output_middlewares:
                    middleware.send(transaction)
                pass
            else:
                pass
                
        except Exception:
            pass
    
    def process_batch(self, batch):
        """
        Procesa un lote de transacciones (chunk) y envía como chunk filtrado.
        Optimizado para procesar chunks completos en lugar de transacciones individuales.
        
        Args:
            batch: Lista de transacciones (chunk)
        """
        try:
            # Filtrar transacciones del chunk
            filtered_transactions = []
            for transaction in batch:
                if self.filter_by_time(transaction):
                    filtered_transactions.append(transaction)
            
            # Enviar chunk filtrado si tiene transacciones
            if filtered_transactions:
                for middleware in self.output_middlewares:
                    middleware.send(filtered_transactions)
            
        except Exception:
            pass
    
    def start_consuming(self):
        """Inicia el consumo de mensajes de la cola de entrada."""
        try:
            # Iniciando worker sin logs para optimización
            
            def on_message(message):
                """Callback para procesar mensajes recibidos."""
                try:
                    if self.shutdown_requested:
                        logger.info("Shutdown requested, stopping message processing")
                        return
                        
                    if self._is_eof(message):
                        for middleware in self.output_middlewares:
                            middleware.send({'type': 'EOF'})
                        self.input_middleware.stop_consuming()
                        return

                    if isinstance(message, list):
                        # Es un lote de transacciones
                        self.process_batch(message)
                    else:
                        # Es una transacción individual
                        self.process_transaction(message)
                        
                except Exception:
                    pass
            
            # Iniciar consumo
            self.input_middleware.start_consuming(on_message)
            
        except KeyboardInterrupt:
            pass
        except Exception:
            pass
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpia recursos."""
        try:
            if hasattr(self, 'input_middleware') and self.input_middleware:
                self.input_middleware.close()
            if hasattr(self, 'output_middlewares') and self.output_middlewares:
                for middleware in self.output_middlewares:
                    middleware.close()
            logger.info("Recursos limpiados")
        except Exception as e:
            logger.warning(f"Error limpiando recursos: {e}")

def main():
    """Punto de entrada principal."""
    try:
        worker = TimeFilterWorker()
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error en main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
