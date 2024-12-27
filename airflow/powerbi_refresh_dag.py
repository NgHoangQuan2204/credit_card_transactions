from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'power_bi_refresh',
    default_args=default_args,
    description='DAG to refresh Power BI data every day using PowerShell',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:
    
    # Bước để chạy PowerShell script làm mới Power BI
    refresh_power_bi = BashOperator(
        task_id='refresh_power_bi',
        bash_command= 'powershell.exe -ExecutionPolicy Bypass -File "C:\\Users\\kdat1\\OneDrive\\Documents\\VS Project\\Credit Card Transactions\\credit_card_transactions\\airflow\\refresh_power_bi.ps1"'
    )