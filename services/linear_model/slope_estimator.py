from data_manager import mongo
import json
from clients import producer
from clients import consumer
from util import util


def callback(ch, method, properties, body):
    message = json.loads(body)
    task_id = message['task_id']
    step, next_topic = util.get_next_topic(task_id)
    _estimate_slope(task_id)
    util.update_steps_task(task_id, step)
    prod = producer.Producer()
    prod.publish(next_topic, message)


consumer_thread = consumer.Consumer('estimate.slope', callback)
consumer_thread.start()


def _estimate_slope(task_id):
    print('estimating slope for task: ' + task_id)
