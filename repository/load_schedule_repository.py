import pytz
import pandas as pd
from datetime import datetime
import config.configuration as config
from utils.postgres_conn import PostgresDatabase


timezone = pytz.timezone(config.TIMEZONE)

class LoadScheduleRepository:

    
    @staticmethod
    def get_dag(dag_name):
        print(f"Consultando si el dag {dag_name} existe en base de datos")
        conn = None
        cur = None

        try:
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()
            query = "SELECT dag_id FROM AF_DAG_DESA WHERE dag_name = %s;"
            cur.execute(query, (dag_name,))
            result = cur.fetchone()
            dag_id = result[0] if result else None
            cur.close()
            conn.close()
            return dag_id
        except Exception as e:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error al Consultar Dag: {str(e)}")     

    @staticmethod
    def add_dag(row):
        conn = None
        cur = None

        try: 
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()

            dag_insert_query = """
                INSERT INTO AF_DAG_DESA (
                    dag_name, description, frequency, freq_interval, start_date,
                    hour_start, hour_end, end_date, owner, tags, catchup, create_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING dag_id;
            """
            dag_values = (
                row['dag_name'] if pd.notna(row['dag_name']) else None,
                row['description'] if pd.notna(row['description']) else None,
                row['frequency'] if pd.notna(row['frequency']) else None,
                str(row['freq_interval']) if pd.notna(row['freq_interval']) else None,
                row['start_date'] if pd.notna(row['start_date']) else None,
                row['hour_start'] if pd.notna(row['hour_start']) else None,
                row['hour_end'] if pd.notna(row['hour_end']) else None,
                row['end_date'] if pd.notna(row['end_date']) else None,
                row['owner'] if pd.notna(row['owner']) else None,
                row['tags'] if pd.notna(row['tags']) else None,
                int(row['catchup']) if pd.notna(row['catchup']) else None,
                datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
            )
            cur.execute(dag_insert_query, dag_values)
            dag_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            print(f"DAG Registrado con dag_id: {dag_id}")

            return dag_id

        except Exception as e:
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error al registrar el Dag: {str(e)}")

    @staticmethod
    def delete_dag(dag_id):

        conn = None
        cur = None

        try:
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()
            query_get_task = "DELETE FROM AF_DAG_DESA WHERE dag_id = %s;"
            cur.execute(query_get_task, (dag_id,))
            conn.commit()
            cur.close()
            conn.close()
            print(f"DAG Eliminado con dag_id: {dag_id}")
        except Exception as e:
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error al Eliminar el DAG: {str(e)}") 

    @staticmethod
    def delete_task_by_dag_id(dag_id):

        conn = None
        cur = None

        try:
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()
            query_get_task = "DELETE FROM AF_TASK_DESA WHERE dag_id = %s;"
            cur.execute(query_get_task, (dag_id,))
            conn.commit()
            cur.close()
            conn.close()
            print(f"TASKs Eliminados con dag_id: {dag_id}")
        except Exception as e:
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error al Eliminar los TASK con el dag id: {str(e)}") 


    @staticmethod
    def add_task(df_tasks_excel):

        conn = None
        cur = None

        try:
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()

            task_insert_query = """
                INSERT INTO AF_TASK_DESA (
                    dag_id, task_name, layout, schedule_type, task_description, retries, retry_delay, depends_on_past, queue_task,
                    task_type, script_task, connection_id, pool_name, priority_weight, predecessor, create_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING task_id
            """

            for index, row in df_tasks_excel.iterrows():

                task_values = (
                    row['dag_id'] if pd.notna(row['dag_id']) else None,
                    row['task_name'] if pd.notna(row['task_name']) else None,
                    row['layout'] if pd.notna(row['layout']) else None,
                    row['schedule_type'] if pd.notna(row['schedule_type']) else None,
                    row['task_description'] if pd.notna(row['task_description']) else None,
                    int(row['retries']) if pd.notna(row['retries']) else None,
                    int(row['retry_delay']) if pd.notna(row['retry_delay']) else None,
                    int(row['depends_on_past']) if pd.notna(row['depends_on_past']) else None,
                    row['queue_task'] if pd.notna(row['queue_task']) else None,
                    row['task_type'] if pd.notna(row['task_type']) else None,
                    row['script_task'] if pd.notna(row['script_task']) else None,
                    row['connection_id'] if pd.notna(row['connection_id']) else None,
                    row['pool_name'] if pd.notna(row['pool_name']) else None,
                    int(row['priority_weight']) if pd.notna(row['priority_weight']) else None,
                    row['predecessor'] if pd.notna(row['predecessor']) else None,
                    datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")       
                    )
                cur.execute(task_insert_query, task_values)
                task_id = cur.fetchone()[0]
                print(f"TASK {row['task_name']} Registrado con task_id: {task_id}")
            conn.commit()
            cur.close()
            conn.close()    
        except Exception as e:
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error al Registrar el Dag: {str(e)}")


    @staticmethod
    def add_dag_hist(dag_id):

        conn = None
        cur = None

        try:
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()
            task_insert_query = """
                INSERT INTO af_dag_hist_desa (dag_id, dag_name, description, frequency, start_date, hour_start, hour_end,freq_interval, end_date, owner,tags,catchup, create_date)
                SELECT dag_id, dag_name, description, frequency, start_date, hour_start, hour_end,freq_interval, end_date, owner,tags,catchup, create_date
                FROM af_dag_desa WHERE dag_id = %s;
            """
            cur.execute(task_insert_query, (dag_id,))
            conn.commit()
            cur.close()
            conn.close()
            print(f"Se registro el DAG en la tabla historica AF_DAG_HIST")
        except Exception as e:
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error registrar el dag en la tabla AF_DAG_HIST: {str(e)}") 

    @staticmethod
    def add_task_hist(dag_id):

        conn = None
        cur = None

        try:
            conn = PostgresDatabase.get_connection()
            cur = conn.cursor()
            task_insert_query = """
                INSERT INTO af_task_hist_desa (task_id, dag_id, task_name, task_type, task_description, retries, retry_delay,depends_on_past, queue_task, script_task,layout,schedule_type, predecessor, connection_id,pool_name,priority_weight, create_date)
                SELECT task_id, dag_id, task_name, task_type, task_description, retries, retry_delay,depends_on_past, queue_task, script_task,layout,schedule_type, predecessor, connection_id,pool_name,priority_weight, create_date
                FROM af_task_desa WHERE dag_id = %s;
            """
            cur.execute(task_insert_query, (dag_id,))
            conn.commit()
            cur.close()
            conn.close()
            print(f"Se registro correctamente los task en la tabla historica: AF_TASK_HIST")
        except Exception as e:
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()        
            print(f"Error registrar los task en la tabla historica AF_TASK_HIST: {str(e)}") 
