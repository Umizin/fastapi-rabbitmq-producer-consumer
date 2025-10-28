import pika
import json
import os
from typing import Dict, Any

RABBITMQ_HOST = os.getenv("RABBETMQ_HOST", "rabbitmq")
QUEUE_NAME = "my_stack"

def publish_message(message: Dict[str, Any]) -> bool:
    try:
        #Manda mensagem para a fila
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
        channel = connection.channel()

        #Declarando e mantendo a fila
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        #Passar body da message para json
        message_body = json.dumps(message)

        #publish
        channel.basic_publish('',QUEUE_NAME,message_body.encode('UTF-8'),pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
        print(f"[x] Sent message to {QUEUE_NAME}: {message_body}")
        connection.close()
        return True

    except pika.exceptions.AMQPConnectionError as e:
            # Erro de conexão, 503
            print(f" [!] Failed to connect to RabbitMQ at {RABBITMQ_HOST}: {e}")
            return False
            
    except Exception as e:
            # Outros erros (ex: serialização JSON)
            print(f" [!] An unexpected error occurred during publish: {e}")
            return False