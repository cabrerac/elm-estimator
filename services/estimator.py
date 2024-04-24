from flask import (Blueprint, request, make_response, jsonify)
from data_manager import mongo
import json
from clients import producer
from clients import consumer
from util import util

rabbit_credentials_file = 'rabbit-mq.yaml'


def callback(ch, method, properties, body):
    message = json.loads(body)
    req_id = message['req_id']
    next_topic = message['next_topic']
    message_dict = {
        'req_id': req_id, 'user_topic': 'user.response', 'desc': 'response from home!!!',
        'next_topic': 'user.response'
    }
    credentials = util.read_rabbit_credentials(rabbit_credentials_file)
    prod = producer.Producer(credentials)
    prod.publish(next_topic, message_dict)


credentials = util.read_rabbit_credentials(rabbit_credentials_file)
consumer_thread = consumer.Consumer(credentials, 'service.home', callback)
consumer_thread.start()

bp = Blueprint('estimator', __name__, url_prefix='')


# Flask interface
@bp.route('/estimate', methods=['POST'])
def estimate():
    data = request.get_json()
    task_id = mongo.store('elm', 'tasks', data)
    res = {'task_id': str(task_id)}
    prod = producer.Producer(credentials)
    prod.publish('', {})
    res = make_response(jsonify(res), 200)
    return res
