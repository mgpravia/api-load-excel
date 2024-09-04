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
            raise AppException(f"Error: No file Data", HTTPStatus.BAD_REQUEST)
        
        file = io.BytesIO(request.data)

        response = LoadScheduleService.upload_schedule_airflow(file)

        return jsonify({f'{response}':'Registered successfully'}), HTTPStatus.OK
    except AppException as e:       
        raise AppException(f"{str(e)}", e.status_code)
    except Exception as e:       
        raise AppException(f"Error en el controlador", HTTPStatus.INTERNAL_SERVER_ERROR)    










