from http import HTTPStatus
from flask import jsonify, Blueprint, request
import io
from service.load_schedule_service import LoadScheduleService
from handler.error_handler import AppException

load_schedule_controller = Blueprint('load_schedule_controller', __name__)

@load_schedule_controller.route('/api/v1/loadSchedule', methods=['POST'])
def upload_schedule_airflow():

    try:

        if not request.data:
            raise AppException(f"No file Data", HTTPStatus.BAD_REQUEST)
        
        file = io.BytesIO(request.data)

        response = LoadScheduleService.upload_schedule_airflow(file)

        response_data = {
            "message": f"Registered successfully: {response}",
            "status_code": HTTPStatus.CREATED
        }

        return jsonify(response_data), HTTPStatus.CREATED
    except AppException as e:       
        raise AppException(f"{str(e)}", e.status_code)
    except Exception as e:       
        raise AppException(f"An error occurred in the controller", HTTPStatus.INTERNAL_SERVER_ERROR)    










