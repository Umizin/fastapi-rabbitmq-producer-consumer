import pika
import json
import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "my_stack"

def callback(ch, method, properties, body):
    #Função chamada quando uma mensagem é recebida.
    try:
        message_data = json.loads(body.decode('utf-8'))
        
        print(f" [x] RECEIVED: {message_data}")
        print(f"     -> Nome: {message_data.get('name')}")
        print(f"     -> Texto: {message_data.get('text')}")
        
        # Reconhece a mensagem para removê-la da fila
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.JSONDecodeError:
        print(f" [!] Error decoding JSON: {body}")
        ch.basic_nack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f" [!] An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag) 

def start_consuming():
    print(f' [*] Trying to connect to RabbitMQ at {RABBITMQ_HOST}')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Manter a fila
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    # Manter o conceito da fila, para q a mensageria mantenha a logica
    channel.basic_qos(prefetch_count=1) 
    
    print(' [*] Waiting for messages. To exit the container press CTRL+C')
    
    # Configurar e iniciar o consumo
    channel.basic_consume(
        queue=QUEUE_NAME, 
        on_message_callback=callback
    )

    # Inicia o loop
    channel.start_consuming()

if __name__ == '__main__':
    try:
        start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        print(f" [!!!] FAILED TO START CONSUMER: Connection error: {e}")
        print(" [!!!] Ensure the 'rabbitmq' service is running and accessible.")
    except KeyboardInterrupt:
        print(' [!] Consumer stopped manually.')