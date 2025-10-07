"""
worker_sqs.py - Worker que consume mensajes de AWS SQS y aplica cifrado
"""
import boto3
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Optional, Dict

class SQSCipherWorker:
    def __init__(self, queue_name='message-queue', region_name='us-east-1', shift=3):
        """
        Inicializa el worker SQS
        
        Args:
            queue_name: Nombre de la cola SQS
            region_name: Región de AWS
            shift: Desplazamiento para cifrado César
        """
        self.queue_name = queue_name
        self.region_name = region_name
        self.shift = shift
        self.processed_count = 0
        
        # Crear cliente SQS
        self.sqs = boto3.client('sqs', region_name=region_name)
        
        # Obtener URL de la cola
        self.queue_url = self._get_queue_url()
        
    def _get_queue_url(self) -> str:
        """Obtiene la URL de la cola SQS"""
        try:
            response = self.sqs.get_queue_url(QueueName=self.queue_name)
            return response['QueueUrl']
        except ClientError as e:
            print(f"✗ Error: Cola '{self.queue_name}' no existe")
            print(f"  Ejecuta primero el producer para crear la cola")
            raise
    
    def caesar_cipher(self, text: str) -> str:
        """
        Aplica cifrado César al texto
        
        Args:
            text: Texto a cifrar
            
        Returns:
            str: Texto cifrado
        """
        result = []
        
        for char in text:
            if char.isalpha():
                base = ord('A') if char.isupper() else ord('a')
                shifted = (ord(char) - base + self.shift) % 26
                result.append(chr(base + shifted))
            else:
                result.append(char)
        
        return ''.join(result)
    
    def process_message(self, payload: dict) -> dict:
        """
        Procesa un mensaje aplicando cifrado
        
        Args:
            payload: Diccionario con el mensaje
            
        Returns:
            dict: Payload procesado con mensaje cifrado
        """
        original = payload.get('message', '')
        encrypted = self.caesar_cipher(original)
        
        payload['original_message'] = original
        payload['encrypted_message'] = encrypted
        payload['status'] = 'processed'
        payload['processed_at'] = datetime.now().isoformat()
        payload['cipher_shift'] = self.shift
        
        return payload
    
    def consume_message(self) -> Optional[Dict]:
        """
        Consume un mensaje de la cola SQS
        
        Returns:
            dict o None: Mensaje procesado o None si no hay mensajes
        """
        try:
            # Recibir mensaje con long polling (WaitTimeSeconds=20)
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long polling: espera hasta 20 segundos
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            
            if not messages:
                return None
            
            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            
            # Parsear el body del mensaje
            payload = json.loads(message['Body'])
            
            # Procesar el mensaje
            processed = self.process_message(payload)
            
            # IMPORTANTE: Eliminar el mensaje de la cola después de procesarlo
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            
            self.processed_count += 1
            return processed
            
        except ClientError as e:
            print(f"✗ Error al procesar mensaje: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"✗ Error al parsear JSON: {e}")
            # Aún así eliminar el mensaje corrupto
            try:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
            except:
                pass
            return None
    
    def get_queue_stats(self) -> dict:
        """Obtiene estadísticas de la cola"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['All']
            )
            return response['Attributes']
        except ClientError:
            return {}
    
    def start(self, continuous: bool = True, max_messages: Optional[int] = None):
        """
        Inicia el worker
        
        Args:
            continuous: Si es True, ejecuta continuamente
            max_messages: Número máximo de mensajes a procesar (None = ilimitado)
        """
        print("=== Worker de Cifrado AWS SQS ===")
        print(f"Cola: {self.queue_name}")
        print(f"Region: {self.region_name}")
        print(f"Algoritmo: Cifrado César (shift={self.shift})")
        print(f"Modo: {'Continuo' if continuous else 'Single run'}")
        
        # Estadísticas iniciales
        stats = self.get_queue_stats()
        print(f"Mensajes en cola: {stats.get('ApproximateNumberOfMessages', 'N/A')}")
        print("\nEsperando mensajes...\n")
        
        try:
            while True:
                # Verificar límite de mensajes
                if max_messages and self.processed_count >= max_messages:
                    print(f"\n✓ Límite alcanzado: {max_messages} mensajes procesados")
                    break
                
                result = self.consume_message()
                
                if result:
                    print(f"[{self.processed_count}] Procesado:")
                    print(f"  Original:  {result['original_message']}")
                    print(f"  Cifrado:   {result['encrypted_message']}")
                    print(f"  Timestamp: {result['processed_at']}\n")
                else:
                    if not continuous:
                        print("⏳ No hay mensajes disponibles")
                        break
                    # Long polling maneja la espera automáticamente
                    # No necesitamos sleep adicional
                    
        except KeyboardInterrupt:
            print(f"\n✓ Worker detenido por usuario")
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """Cierre limpio del worker"""
        print(f"✓ Mensajes procesados: {self.processed_count}")
        
        # Estadísticas finales
        stats = self.get_queue_stats()
        remaining = stats.get('ApproximateNumberOfMessages', 'N/A')
        print(f"✓ Mensajes restantes en cola: {remaining}")


def main():
    """Función principal para ejecutar el worker"""
    worker = SQSCipherWorker(
        queue_name='message-queue',
        region_name='us-east-1',
        shift=3
    )
    
    # Ejecutar continuamente
    worker.start(continuous=True)
    
    # O procesar solo N mensajes:
    # worker.start(continuous=False, max_messages=10)


if __name__ == "__main__":
    main()
