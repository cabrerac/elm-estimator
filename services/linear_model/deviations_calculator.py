import json
from clients import producer
from clients import consumer
from util import util
from data_manager import mongo
from bson import ObjectId


topic = 'calculate_deviations'


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    message = json.loads(body)
    sender = message['from']
    message_id = message['message_id']
    object_id = ObjectId(message_id)
    message = mongo.retrieve_record('xlm', 'messages', {'_id': object_id})
    task = message['task']
    task = _calculate_deviations(task)
    task = util.update_steps_task(task)
    finished = False
    if len(task['steps']) == 0:
        finished = True
    message = {'from': sender, 'to': topic, 'task': task, 'finished': finished}
    message_id = mongo.store('xlm', 'messages', message)
    next_topic = util.get_next_topic(task)
    message = {'message_id': str(message_id), 'task_id': task['task_id'], 'from': topic, 'to': next_topic}
    prod = producer.Producer()
    print("next step: " + next_topic)
    prod.publish(next_topic, message)


consumer_thread = consumer.Consumer(topic, callback)
consumer_thread.start()


def _calculate_deviations(task):
    task_id = task['task_id']
    metadata = task['metadata']
    y = task['data'][metadata['dependent_variable']]['train']
    y_mean = sum(y) / len(y)
    y_test = task['data']['y']['test']
    deviations = []
    i = 0
    while i < len(y_test):
        deviation = y_test[i] - y_mean
        deviations.append(deviation)
        i = i + 1
    task['results']['deviations'] = deviations
    print('[>] Deviations calculated for task: ' + task_id)
    return task
