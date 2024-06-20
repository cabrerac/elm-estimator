import json
from clients import producer
from clients import consumer
from util import util


def callback(ch, method, properties, body):
    message = json.loads(body)
    task_id = message['task_id']
    step, next_topic = util.get_next_topic(task_id)
    _estimate_intercept(task_id)
    util.update_steps_task(task_id, step)
    prod = producer.Producer()
    prod.publish(next_topic, message)
    ch.basic_ack(delivery_tag=method.delivery_tag)


consumer_thread = consumer.Consumer('estimate_intercept', callback)
consumer_thread.start()


def _estimate_intercept(task_id):
    print('estimating intercept for task: ' + task_id)
