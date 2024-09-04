from flask import jsonify



class AppException(Exception):
    def __init__(self, message, status_code=500) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code



def register_error_handlers(app):

    @app.errorhandler(AppException)
    def handle_app_exception(error):
        response = jsonify(f"error: {error.message}")
        response.status_code = error.status_code
        return response
