from data_manager import mongo
from cfn_tools import load_yaml
import pika
from bson import ObjectId


# reads credentials
def read_rabbit_credentials(file):
    with open(file, 'r') as stream:
        credentials = load_yaml(stream)
        return credentials


# gets topic for next step in a learning process
def get_next_topic(task_id):
    object_id = ObjectId(task_id)
    task = mongo.retrieve_record('xlm', 'tasks', {'_id': object_id})
    steps = task['steps']
    sorted_keys = sorted(steps.keys())
    if len(sorted_keys) > 0:
        step = sorted_keys[0]
        next_topic = steps[sorted_keys[0]]
    else:
        step = -1
        next_topic = 'learning_finish'
    return step, next_topic


# updates the steps of a learning process
def update_steps_task(task_id, step):
    object_id = ObjectId(task_id)
    task = mongo.retrieve_record('xlm', 'tasks', {'_id': object_id})
    if len(task['steps']) > 0:
        del task['steps'][step]
        mongo.update_record('xlm', 'tasks', {'_id': object_id}, {'$set': {'steps': task['steps']}})
