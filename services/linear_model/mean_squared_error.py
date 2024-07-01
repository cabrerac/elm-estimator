import json
from clients import producer
from clients import consumer
from util import util
from data_manager import mongo
from bson import ObjectId


topic = 'evaluate_mse'


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    message = json.loads(body)
    sender = message['from']
    message_id = message['message_id']
    object_id = ObjectId(message_id)
    message = mongo.retrieve_record('xlm', 'messages', {'_id': object_id})
    task = message['task']
    task = _evaluate(task)
    task = util.update_steps_task(task)
    finished = False
    if len(task['steps']) == 0:
        finished = True
    message = {'from': sender, 'to': topic, 'task': task, 'finished': finished}
    message_id = mongo.store('xlm', 'messages', message)
    next_topic = util.get_next_topic(task)
    message = {'message_id': str(message_id), 'task_id': task['task_id'], 'from': topic, 'to': next_topic}
    prod = producer.Producer()
    print("Next topic slope: " + next_topic)
    prod.publish(next_topic, message)


consumer_thread = consumer.Consumer(topic, callback)
consumer_thread.start()


def _evaluate(task):
    task_id = task['task_id']
    y_test = task['data']['y']['test']
    y_pred = task['results']['y_pred']
    residuals = []
    mse = 0.0
    i = 0
    while i < len(y_test):
        residual = y_test[i] - y_pred[i]
        mse = mse + (residual*residual)
        residuals.append(residual*residual)
        i = i + 1
    mse = mse/len(y_test)
    task['results']['residuals'] = residuals
    task['results']['mse'] = mse
    print('MSE calculated for task: ' + task_id)
    return task
