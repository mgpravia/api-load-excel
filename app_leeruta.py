from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine
from werkzeug.utils  import secure_filename
import utils
import os

app = Flask(__name__)

# Configuración de la conexión a la base de datos PostgreSQL
DATABASE_URI = 'postgresql+psycopg2://usuario:password@localhost:5432/nombre_base_datos'
engine = create_engine(DATABASE_URI)

# Ruta para subir y procesar el archivo Excel
@app.route('/upload-excel', methods=['POST'])
def upload_excel():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Asegúrate de que el archivo es seguro para guardar
    filename = secure_filename(file.filename)

    if filename.endswith('.xlsx'):
        # Leer el archivo Excel en un DataFrame de pandas
        #df = pd.read_excel(file)

        df_dag_excel = pd.read_excel(file, sheet_name='schedule', header=None, nrows= 1, skiprows=4, usecols='B:L')

        # Cargar los datos de los Tasks 
        df_tasks_excel = pd.read_excel(file, sheet_name='schedule', header=None, skiprows=11, usecols='B:N')

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
        dag_id = utils.get_dag(dag_name)




        if dag_id == None:
            print(f"Registrando un nuevo DAG")
            dag_id = utils.add_dag(dag_row)
            print(f"Dag Registrado con id: {dag_id}")
            df_tasks_excel['dag_id'] = dag_id

            utils.add_task(df_tasks_excel)

        else:
            print(f"Actualizando DAG")

            utils.delete_dag(dag_id)
            utils.delete_task_by_dag_id(dag_id)

            dag_id = utils.add_dag(dag_row)

            df_tasks_excel['dag_id'] = dag_id

            utils.add_task(df_tasks_excel)

        # Guarda el DataFrame en la base de datos PostgreSQL
        #try:
        #    df.to_sql('nombre_tabla', engine, if_exists='append', index=False)
        #except Exception as e:
        #    return jsonify({'error': str(e)}), 500

        return jsonify({'message': 'File successfully uploaded and data stored'}), 200
    else:
        return jsonify({'error': 'Invalid file format. Please upload an .xlsx file'}), 400

# Ruta para probar si la API está funcionando
@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({'message': 'La api esta ejecutandose...'}), 200

if __name__ == '__main__':
    app.run(debug=True)
