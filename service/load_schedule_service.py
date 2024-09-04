from qsynthetix.load_schedule_airflow import LoadScheduleAirflow
from handler.error_handler import AppException
from http import HTTPStatus
from openpyxl import load_workbook 

class LoadScheduleService:

    @staticmethod
    def upload_schedule_airflow(file):
        try:

            try:
                workbook = load_workbook(file)
            except Exception as e:
                  raise AppException(f"Uploaded file is not a valid Excel file", HTTPStatus.BAD_REQUEST)   
            
            return LoadScheduleAirflow.load_schedule_airflow(file)
        
        except AppException as e:       
            raise AppException(f"{str(e)}", e.status_code)
        
        except Exception as e:
            print("An error occurred in the service")
            raise AppException(f"An error occurred in the service", HTTPStatus.INTERNAL_SERVER_ERROR)


