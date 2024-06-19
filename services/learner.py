from flask import (Blueprint, request, make_response, jsonify)
from data_manager import mongo
import json
from clients import producer
from clients import consumer
from util import util


def callback(ch, method, properties, body):
    message = json.loads(body)
    task_id = message['task_id']
    print("learning process finished for task: " + task_id)


consumer_thread = consumer.Consumer('learning.finish', callback)
consumer_thread.start()

bp = Blueprint('learner', __name__, url_prefix='')


# Flask interface
@bp.route('/learn', methods=['POST'])
def learn():
    data = request.get_json()
    task_id = mongo.store('xlm', 'tasks', data)
    message = {'task_id': str(task_id)}
    model = data['model']
    method = data['method']
    if model == 'linear':
        if method == 'least_squares':
            next_topic = util.get_next_topic(task_id)
            prod = producer.Producer()
            prod.publish(next_topic, message)
    res = {'task_id': str(task_id)}
    res = make_response(jsonify(res), 200)
    return res
