import pandas as pd
from repository.load_schedule_repository import LoadScheduleRepository


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
            # PREPARANDO LA DATA DAG

            df_dag_excel.columns = [
                'dag_name', 'description', 'frequency', 'freq_interval', 'start_date', 'end_date', 'hour_start',
                'hour_end', 'owner', 'tags', 'catchup'
            ]
            #df_dag_excel['freq_interval'] = df_dag_excel['freq_interval'].astype(str)

            # PREPARANDO DATA TASK

            df_tasks_excel.columns = [
                'layout','schedule_type','task_description',  'predecessor', 'retries', 'retry_delay', 'depends_on_past','queue_task',
                'priority_weight', 'task_type', 'connection_id','script_task',  'pool_name'
            ]
            df_tasks_excel['task_name'] = df_tasks_excel['schedule_type'] + '_' + df_tasks_excel['layout']



            print("Data Dag")
            #print(df_dag_excel.iloc[0])
            print(df_dag_excel)

            #print("================================================")
            print("Data Task")
            print(df_tasks_excel)

            print('==============REGISTRANDO O ACTUALIZANDO  DAG===================')
            dag_row = df_dag_excel.iloc[0]
            dag_name  = dag_row['dag_name']
            dag_id = LoadScheduleRepository.get_dag(dag_name)




            if dag_id is None:
                print(f"Registrando un nuevo DAG")
                dag_id = LoadScheduleRepository.add_dag(dag_row)
                print(f"Dag Registrado con id: {dag_id}")
                df_tasks_excel['dag_id'] = dag_id

                LoadScheduleRepository.add_task(df_tasks_excel)

            else:
                print(f"Actualizando DAG")

                #Agrgando Dag y Task ha historica
                LoadScheduleRepository.add_dag_hist(dag_id)
                LoadScheduleRepository.add_task_hist(dag_id)

                LoadScheduleRepository.delete_dag(dag_id)
                LoadScheduleRepository.delete_task_by_dag_id(dag_id)

                dag_id = LoadScheduleRepository.add_dag(dag_row)

                df_tasks_excel['dag_id'] = dag_id

                LoadScheduleRepository.add_task(df_tasks_excel)

            return "OK"
        
        except Exception as e:
            print(f"Ocurio un error al cargar los dag y task: {str(e)}")
            raise













