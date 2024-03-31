import io
import pendulum
import vertica_python
import logging
import psycopg2

from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator

pg_conn_afw = BaseHook.get_connection('PG_connection')
vertica_conn_afw = BaseHook.get_connection('Vertica_connection')

postgres_conn = {
    "host": pg_conn_afw.host,
    "port": pg_conn_afw.port,
    "user": pg_conn_afw.login,
    "password": pg_conn_afw.password,
    "dbname": pg_conn_afw.schema
}

vertica_conn = {
    'host': vertica_conn_afw.host,
    'port': vertica_conn_afw.port,
    'user': vertica_conn_afw.login,
    'password': vertica_conn_afw.password,
    'database': '',
    'autocommit': True
}

def load_stg_currencies(calc_date) -> None:
    with psycopg2.connect(**postgres_conn) as postgre_connect:
        cur_postrgres = postgre_connect.cursor()
        input = io.StringIO()
        cur_postrgres.copy_expert(
            f'''copy (select distinct * 
                from public.currencies 
                where date_update::date='{calc_date}'::date-1 
                order by date_update) to stdout
                ;''', input)
        cur_postrgres.close()

    with vertica_python.connect(**vertica_conn) as vertica_connection:
        cur_vertica = vertica_connection.cursor()
        cur_vertica.execute(
            f"""select drop_partitions('stv202312114__staging.currencies', 
                '{calc_date}'::date-1, 
                '{calc_date}'::date-1);
                ;""")
        vertica_connection.commit()
        cur_vertica.copy(
            f'''copy stv202312114__staging.currencies 
                from stdin delimiter e'\t' null as 'null' 
                rejected data as table copy_rejected
                ;''', input.getvalue())
        cur_vertica.connection.commit()


def load_stg_transactions(calc_date) -> None:
    with psycopg2.connect(**postgres_conn) as postgre_connect:
        cur_postrgres = postgre_connect.cursor()
        input = io.StringIO()
        cur_postrgres.copy_expert(
            f'''copy (select distinct * 
                from public.transactions 
                where transaction_dt::date='{calc_date}'::date-1 
                order by transaction_dt) to stdout
                ;''', input)
        cur_postrgres.close()

    with vertica_python.connect(**vertica_conn) as vertica_connection:
        cur_vertica = vertica_connection.cursor()
        cur_vertica.execute(
            f"""select drop_partitions('stv202312114__staging.transactions', 
            '{calc_date}'::date-1, 
            '{calc_date}'::date-1);
            ;""")
        vertica_connection.commit()
        cur_vertica.copy(
            f'''copy stv202312114__staging.transactions 
                from stdin delimiter e'\t' null as 'null' 
                rejected data as table copy_rejected
                ;''', input.getvalue())
        cur_vertica.connection.commit()


def load_dm(calc_date):
    sql_query = open("/lessons/sql/load_dm.sql").read().format(calc_date=calc_date)
    with vertica_python.connect(**vertica_conn) as vertica_connection:
        cur_vertica = vertica_connection.cursor()
        cur_vertica.execute(sql_query)
        cur_vertica.connection.commit()
        cur_vertica.close()


with DAG(
        'final_DAG',
        schedule_interval="@daily",
        start_date=pendulum.parse('2022-10-01'),
        end_date=pendulum.parse('2022-10-31'),
        catchup=True,
        max_active_runs=1,
        tags=['final_project']
) as dag:
    load_stg_currencies_task = PythonOperator(
        task_id='load_stg_currencies',
        python_callable=load_stg_currencies,
        op_kwargs={'calc_date': '{{ ds }}'},
    )
    load_stg_transactions_task = PythonOperator(
        task_id='load_stg_transactions',
        python_callable=load_stg_transactions,
        op_kwargs={'calc_date': '{{ ds }}'},
    )
    load_dm_task = PythonOperator(
        task_id='load_dm',
        python_callable=load_dm,
        op_kwargs={'calc_date': '{{ ds }}'},
    )

load_stg_currencies_task >> load_stg_transactions_task >> load_dm_task