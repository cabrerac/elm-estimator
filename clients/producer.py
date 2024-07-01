import pika
import json


class Producer:

    def __init__(self):
        rabbit_parameters = pika.ConnectionParameters(host='localhost')
        self.connection = pika.BlockingConnection(rabbit_parameters)
        self.channel = self.connection.channel()

    def publish(self, queue_name, body):
        try:
            self.channel.queue_declare(queue=queue_name, durable=False)
            body_json = json.dumps(body, indent=3)
            self.channel.basic_publish(exchange='', routing_key=queue_name, body=body_json, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
            self.connection.close()
            print("[>] Sent to queue: " + queue_name)
            return True
        except Exception as ex:
            print(str(ex))
            return False
