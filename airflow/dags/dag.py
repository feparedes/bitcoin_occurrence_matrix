import os
import csv
import psycopg2
import pandas as pd
from datetime import datetime
from scipy.sparse import diags
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from utils.TransactionMatrix import TransactionMatrix
from utils.OccurenceMatrix import OccurenceMatrix


POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DBNAME = os.getenv('POSTGRES_DBNAME')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
SCHEMA = os.getenv('SCHEMA')

INPUT_QUERY = "SELECT codified_hash, codified_address FROM bitcoin_transactions_input order by 1, 2"
OUTPUT_QUERY = "SELECT codified_hash, codified_address FROM bitcoin_transactions_output order by 1, 2"
INPUT_SET_QUERY = "SELECT * FROM bitcoin_transactions_input_set order by 1, 2"

INPUT_CSV = "/tmp/p_bitcoin_transaction_matrix.csv"
OUTPUT_CSV = "/tmp/q_bitcoin_transaction_matrix.csv"
INPUT_SET_CSV = "/tmp/h_bitcoin_transaction_matrix.csv"


dag = DAG('occurrence_matrix', description='Generate Occurrence matrix',
          schedule_interval=None,
          start_date=datetime(2023, 1, 10), catchup=False)

#################################################### AUX FUNCTIONS ####################################################

def extract_data_from_database(query, csv_file):
    connection = psycopg2.connect(
        database=POSTGRES_DBNAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

    print(f"Connected to DB {POSTGRES_HOST}:{POSTGRES_PORT}")
    cursor = connection.cursor()

    print(F'Query to execute: {query}')

    cursor.execute(query)
    print('Query executed')

    result = cursor.fetchall()
    print('Result fetched')

    cursor.close()
    connection.close()

    with open(csv_file, 'w', newline='') as temp_csv_file:
        csv_writer = csv.writer(temp_csv_file)
        csv_writer.writerow([desc[0] for desc in cursor.description])
        csv_writer.writerows(result)

    print(f"Se ha exportado la consulta a '{csv_file}' en formato CSV.")

def generate_m_matrix(q_matrix, p_matrix, h_matrix, k=1):
    n = 1000000
    diagonal_principal = [1] * n
    m_matrix = diags([diagonal_principal], [0], shape=(n, n), format="csr")


    for i in range(0,k-1):
        if i == 0:
            m_matrix = q_matrix.get_matrix().dot(p_matrix.get_matrix())
        else: 
            m_matrix = m_matrix.dot(p_matrix.get_matrix())

    m_matrix = h_matrix.get_matrix().dot(m_matrix)
    m_matrix = m_matrix.dot(q_matrix.get_matrix()) 
    
    return m_matrix

#################################################### TASK ####################################################

start = DummyOperator(task_id='start', dag=dag)

@task(task_id="load_data_into_database")
def load_data_into_database(path, table):
    df = pd.read_csv(path)
    df = df[["block_timestamp", "hash", "inputs", "outputs"]]
    print(df.head())

    print(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}")

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"
    )

    print('Engine created')
    
    df.to_sql(
        table,
        engine,
        schema=SCHEMA,
        index=False,
        if_exists='replace',
        chunksize=200000
    )

    print('Dataframe uploaded to Postgres DB')

run_dbt_model = BashOperator(
    task_id="run_dbt_model",
    bash_command="cd /project/dbt && ls && dbt debug && dbt compile --vars '{initial_date: 201001, end_date: 202312}' && dbt run -m +bitcoin_transactions_output --vars '{initial_date: 201001, end_date: 202312}' && dbt run -m +bitcoin_transactions_input_set --vars '{initial_date: 201001, end_date: 202312}'",
)

@task(task_id="extract_all_data_from_database")
def extract_all_data_from_database():
    extract_data_from_database(INPUT_QUERY, INPUT_CSV)
    extract_data_from_database(OUTPUT_QUERY, OUTPUT_CSV)
    extract_data_from_database(INPUT_SET_QUERY, INPUT_SET_CSV)
  
@task(task_id="generate_occurrence_matrix")
def generate_occurrence_matrix(k):
    q_matrix = TransactionMatrix(path='/tmp/q_bitcoin_transaction_matrix.csv')
    p_matrix = TransactionMatrix(path='/tmp/p_bitcoin_transaction_matrix.csv')
    h_matrix = TransactionMatrix(path='/tmp/h_bitcoin_transaction_matrix.csv', is_h_matrix=True)

    m_matrix = generate_m_matrix(q_matrix, p_matrix, h_matrix, k=k)

    occurence_matrix = OccurenceMatrix(m_matrix, h_matrix.get_number_addresses())

    occurence_matrix.print_occurrence_matrix()
    occurence_matrix.save_occurrence_matrix(path='/tmp/occurrence_matrix.out')


display_occurrence_matrix = BashOperator(
    task_id="display_occurrence_matrix",
    bash_command="cat /tmp/occurrence_matrix.out",
)

end = DummyOperator(task_id='end', dag=dag)


start >> load_data_into_database(path='/raw_data/transactions20231106.csv', table='bitcoin_raw_data') >> run_dbt_model >> extract_all_data_from_database() >> generate_occurrence_matrix(k=1) >> display_occurrence_matrix >> end

# start >> run_dbt_model >> extract_all_data_from_database() >> generate_occurrence_matrix(k=1) >> display_occurrence_matrix >> end