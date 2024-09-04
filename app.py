from flask import Flask
from flask_cors import CORS
from controller.load_schedule_Controller import load_schedule_controller
from handler.error_handler import register_error_handlers



app = Flask(__name__)
CORS(app)

app.register_blueprint(load_schedule_controller)
register_error_handlers(app)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0', port=8089)










