import os
import logging
from datetime import datetime
from flask import Flask

# create and configure logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


# console log
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # set the level for console output
console_formatter = logging.Formatter('%(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
# file log
if not os.path.exists('./logs/'):
    os.makedirs('./logs/')
log_file = './logs/xlm_learner_' + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p") + '.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)  # set the level for file output
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev'
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    from . import learner
    app.register_blueprint(learner.bp)

    from .linear_model import slope_estimator
    from .linear_model import intercept_estimator
    from .linear_model import predictions_estimator
    from .linear_model import residuals_calculator
    from .linear_model import deviations_calculator
    from .linear_model import mse_calculator
    from .linear_model import rs_calculator

    return app
