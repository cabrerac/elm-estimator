from flask import (Blueprint, request, make_response, jsonify)
from data_manager import mongo
import json
from clients import producer
from clients import consumer
from util import util


topic = 'learning_finish'


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    message = json.loads(body)
    task_id = message['task_id']
    print("learning process finished for task: " + task_id)


consumer_thread = consumer.Consumer(topic, callback)
consumer_thread.start()

bp = Blueprint('learner', __name__, url_prefix='')


# Flask interface
@bp.route('/learn', methods=['POST'])
def learn():
    task = request.get_json()
    task['results'] = {}
    message = {'from': 'client', 'to': 'learning_start', 'task': task, 'finished': False}
    message_id = mongo.store('xlm', 'messages', message)
    task_id = task['task_id']
    next_topic = util.get_next_topic(task)
    message = {'message_id': str(message_id), 'task_id': task_id, 'from': topic, 'to': next_topic}
    prod = producer.Producer()
    prod.publish(next_topic, message)
    res = {'message_id': str(message_id)}
    res = make_response(jsonify(res), 200)
    return res
