from cfn_tools import load_yaml
import pika


# reads credentials
def read_rabbit_credentials(file):
    with open(file, 'r') as stream:
        credentials = load_yaml(stream)
        return credentials


# publishes message on rabbitMQ
def publish_message(host, msg, queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=msg,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    connection.close()