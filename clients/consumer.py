import pika
import ssl
import threading


class Consumer(threading.Thread):

    def __init__(self, binding_key, callback):
        threading.Thread.__init__(self)
        rabbit_parameters = pika.ConnectionParameters(host='localhost')
        self.connection = pika.BlockingConnection(rabbit_parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='messages', exchange_type='topic', durable=False)
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='messages', queue=queue_name, routing_key=binding_key)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        threading.Thread(target=self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False), daemon=True)
        self.binding_key = binding_key
    
    def run(self):
        print(' [*] waiting for messages at topic: ' + self.binding_key + '...')
        self.channel.start_consuming()
