from flask import Flask
from flask_cors import CORS
from controller.load_schedule_Controller import load_schedule_controller
from handler.error_handler import register_error_handlers
from flask_jwt_extended import JWTManager
from controller.auth_controller import auth_bp


app = Flask(__name__)
CORS(app)

app.config['JWT_SECRET_KEY'] = 'super-secret-key'
jwt = JWTManager(app)

app.register_blueprint(load_schedule_controller)
app.register_blueprint(auth_bp)

register_error_handlers(app)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0', port=8089)










