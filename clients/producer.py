import pika
import ssl
import json


class Producer:

    def __init__(self):
        rabbit_parameters = pika.ConnectionParameters(host='localhost')
        self.connection = pika.BlockingConnection(rabbit_parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='messages', exchange_type='topic', durable=False)
        self.channel.confirm_delivery()

    def publish(self, routing_key, body):
        try:
            body_json = json.dumps(body, indent=4)
            self.channel.basic_publish(exchange='messages', routing_key=routing_key, body=body_json)
            self.connection.close()
            print(" [x] Sent to " + routing_key + " Message: " + str(body_json))
            return True
        except Exception as ex:
            print(str(ex))
            return False
