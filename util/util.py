# gets topic for next step in a learning process
def get_next_topic(task):
    steps = task['steps']
    sorted_keys = sorted(steps.keys())
    if len(sorted_keys) > 0:
        next_topic = steps[sorted_keys[0]]
    else:
        next_topic = 'learning_finish'
    return next_topic


# updates the steps of a learning process
def update_steps_task(task):
    if len(task['steps']) > 0:
        steps = task['steps']
        sorted_keys = sorted(steps.keys())
        del steps[sorted_keys[0]]
        task['steps'] = steps
    return task
