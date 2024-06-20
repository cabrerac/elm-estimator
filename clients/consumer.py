import pika
import threading


class Consumer(threading.Thread):

    def __init__(self, queue_name, callback):
        threading.Thread.__init__(self)
        rabbit_parameters = pika.ConnectionParameters(host='localhost')
        self.connection = pika.BlockingConnection(rabbit_parameters)
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name, durable=False)
        threading.Thread(target=self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback), daemon=True)
    
    def run(self):
        print(' [*] waiting for messages at queue: ' + self.queue_name + '...')
        self.channel.start_consuming()
