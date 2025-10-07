"""
producer_sqs.py - Productor que envía mensajes a AWS SQS
"""
import boto3
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError

class SQSMessageProducer:
    def __init__(self, queue_name='message-queue', region_name='us-east-1'):
        """
        Inicializa la conexión a AWS SQS
        
        Args:
            queue_name: Nombre de la cola SQS
            region_name: Región de AWS
        """
        self.queue_name = queue_name
        self.region_name = region_name
        
        # Crear cliente SQS
        self.sqs = boto3.client('sqs', region_name=region_name)
        
        # Obtener URL de la cola (o crearla si no existe)
        self.queue_url = self._get_or_create_queue()
        
    def _get_or_create_queue(self) -> str:
        """
        Obtiene la URL de la cola o la crea si no existe
        
        Returns:
            str: URL de la cola SQS
        """
        try:
            # Intentar obtener la cola existente
            response = self.sqs.get_queue_url(QueueName=self.queue_name)
            queue_url = response['QueueUrl']
            print(f"✓ Cola encontrada: {self.queue_name}")
            return queue_url
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                # Crear la cola si no existe
                print(f"⚠ Cola no existe, creando: {self.queue_name}")
                response = self.sqs.create_queue(
                    QueueName=self.queue_name,
                    Attributes={
                        'MessageRetentionPeriod': '345600',  # 4 días
                        'VisibilityTimeout': '60',  # 60 segundos
                        'ReceiveMessageWaitTimeSeconds': '20'  # Long polling
                    }
                )
                queue_url = response['QueueUrl']
                print(f"✓ Cola creada exitosamente")
                return queue_url
            else:
                raise
    
    def send_message(self, message: str) -> bool:
        """
        Envía un mensaje a la cola de SQS
        
        Args:
            message: Mensaje a enviar
            
        Returns:
            bool: True si se envió correctamente
        """
        try:
            payload = {
                'message': message,
                'timestamp': datetime.now().isoformat(),
                'status': 'pending'
            }
            
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(payload),
                MessageAttributes={
                    'Timestamp': {
                        'StringValue': datetime.now().isoformat(),
                        'DataType': 'String'
                    },
                    'Source': {
                        'StringValue': 'producer-python',
                        'DataType': 'String'
                    }
                }
            )
            
            message_id = response['MessageId']
            print(f"✓ Mensaje enviado: {message}")
            print(f"  MessageId: {message_id}")
            return True
            
        except ClientError as e:
            print(f"✗ Error al enviar mensaje: {e}")
            return False
    
    def send_batch(self, messages: list) -> dict:
        """
        Envía múltiples mensajes en batch (más eficiente)
        
        Args:
            messages: Lista de mensajes a enviar
            
        Returns:
            dict: Resultado del envío batch
        """
        entries = []
        for idx, msg in enumerate(messages):
            payload = {
                'message': msg,
                'timestamp': datetime.now().isoformat(),
                'status': 'pending'
            }
            entries.append({
                'Id': str(idx),
                'MessageBody': json.dumps(payload)
            })
        
        try:
            response = self.sqs.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries
            )
            
            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))
            
            print(f"✓ Batch enviado: {successful} exitosos, {failed} fallidos")
            return response
            
        except ClientError as e:
            print(f"✗ Error al enviar batch: {e}")
            return {}
    
    def get_queue_attributes(self) -> dict:
        """Obtiene atributos de la cola (mensajes disponibles, etc.)"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['All']
            )
            return response['Attributes']
        except ClientError as e:
            print(f"✗ Error al obtener atributos: {e}")
            return {}


def main():
    """Función principal para demostrar el uso del productor"""
    print("=== Productor de Mensajes AWS SQS ===\n")
    
    # Crear productor
    producer = SQSMessageProducer(queue_name='message-queue')
    
    # Obtener información de la cola
    attrs = producer.get_queue_attributes()
    print(f"Cola: {producer.queue_name}")
    print(f"Region: {producer.region_name}")
    print(f"Mensajes disponibles: {attrs.get('ApproximateNumberOfMessages', 'N/A')}")
    print(f"Mensajes en proceso: {attrs.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}\n")
    
    # Enviar mensajes individuales
    mensajes = [
        "Hola mundo desde AWS SQS",
        "Este mensaje será cifrado",
        "SQS es serverless y escalable",
        "Arquitectura cloud-native"
    ]
    
    print("--- Enviando mensajes individuales ---")
    for msg in mensajes:
        producer.send_message(msg)
        time.sleep(0.3)
    
    # Enviar batch (más eficiente para múltiples mensajes)
    print("\n--- Enviando mensajes en batch ---")
    batch_messages = [
        "Mensaje batch 1",
        "Mensaje batch 2",
        "Mensaje batch 3"
    ]
    producer.send_batch(batch_messages)
    
    # Estadísticas finales
    print("\n--- Estadísticas ---")
    attrs = producer.get_queue_attributes()
    print(f"✓ Total mensajes en cola: {attrs.get('ApproximateNumberOfMessages', 'N/A')}")
    print(f"✓ URL de la cola: {producer.queue_url}")


if __name__ == "__main__":
    main()
