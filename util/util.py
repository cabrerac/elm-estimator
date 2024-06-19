from data_manager import mongo
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


# gets topic for next step in a learning process
def get_next_topic(task_id):
    task = mongo.retrieve_record('xlm', 'tasks', {'_id': task_id})
    print('tasks: ' + str(task))
    steps = task['steps']
    sorted_keys = sorted(steps.keys())
    if len(sorted_keys) > 0:
        step = sorted_keys[0]
        next_topic = steps[sorted_keys[0]].replace('_', '.')
    else:
        step = -1
        next_topic = 'learning.finish'
    return step, next_topic


# updates the steps of a learning process
def update_steps_task(task_id, step):
    task = mongo.retrieve_record('xlm', 'tasks', {'_id': task_id})
    del task['steps'][step]
    mongo.update_record('xlm', 'tasks', {'_id': task_id}, {'$set': {'steps': task['steps']}})