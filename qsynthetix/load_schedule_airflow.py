import pandas as pd
from repository.load_schedule_repository import LoadScheduleRepository
from handler.error_handler import AppException
from http import HTTPStatus
from utils.format_log import message_format

class LoadScheduleAirflow:

    @staticmethod
    def load_schedule_airflow(file):
        try:
            df_dag_excel = pd.read_excel(
                file,
                sheet_name='schedule',
                header=None,
                nrows=1,
                skiprows=4,
                usecols='B:L',
                engine='openpyxl'
            )
            
            df_tasks_excel = pd.read_excel(
                file,
                sheet_name='schedule',
                header=None,
                skiprows=11,
                usecols='B:N',
                engine='openpyxl'
            )

            df_dag_excel.columns = [
                'dag_name', 'description', 'frequency', 'freq_interval', 'start_date', 'end_date', 'hour_start',
                'hour_end', 'owner', 'tags', 'catchup'
            ]


            df_tasks_excel.columns = [
                'layout','schedule_type','task_description',  'predecessor', 'retries', 'retry_delay', 'depends_on_past','queue_task',
                'priority_weight', 'task_type', 'connection_id','script_task',  'pool_name'
            ]
            df_tasks_excel['task_name'] = df_tasks_excel['schedule_type'] + '_' + df_tasks_excel['layout']

            dag_row = df_dag_excel.iloc[0]
            dag_name  = dag_row['dag_name']
            dag_id = LoadScheduleRepository.get_dag(dag_name)

            if dag_id is None:
                message_format("Registering Dag")
                dag_id = LoadScheduleRepository.add_dag(dag_row)

                message_format(f"Registering tasks for the dag with id: {dag_id}")
                df_tasks_excel['dag_id'] = dag_id

                LoadScheduleRepository.add_task(df_tasks_excel)

            else:
                
                message_format(f"Recording Dag in historical table")
                LoadScheduleRepository.add_dag_hist(dag_id)
                LoadScheduleRepository.add_task_hist(dag_id)

                message_format("Removing Dag and Task")
                LoadScheduleRepository.delete_dag(dag_id)
                LoadScheduleRepository.delete_task_by_dag_id(dag_id)

                message_format("Registering Dag and Task")
                dag_id = LoadScheduleRepository.add_dag(dag_row)

                df_tasks_excel['dag_id'] = dag_id

                LoadScheduleRepository.add_task(df_tasks_excel)

            return "OK"
        
        except AppException as e:       
            raise AppException(f"{str(e)}", e.status_code)        
        except Exception as e:
            message_format(f"An error occurred in the Dag and Task registration process: {str(e)}")
            raise AppException(f"An error occurred in the Dag and Task registration process", HTTPStatus.INTERNAL_SERVER_ERROR)













