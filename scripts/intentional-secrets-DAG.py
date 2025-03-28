from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# 🚨 Intentional hardcoded secrets (for SonarLint testing)
aws_access_key = "AKIAIOSFODNN7EXAMPLE"
aws_secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
google_api_key = "AIzaSyA-abcdef1234567890-examplekey123"
stripe_secret_key = "sk_live_4eC39HqLyjWDarjtT1zdp7dc"
db_password = "P@ssword123"
generic_token = "1234567890123456789012345678901234567890"  # 40-char token
auth_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
github_pat = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
slack_token = "xoxb-123456789012-0987654321-AbCDefGhIjKLmnOpQrSTUVwx"

# Dummy function to simulate usage
def leak_secrets():
    print("AWS Access:", aws_access_key)
    print("AWS Secret:", aws_secret_key)
    print("Google API:", google_api_key)
    print("Stripe Secret:", stripe_secret_key)
    print("DB Password:", db_password)
    print("Generic Token:", generic_token)
    print("Auth Header:", auth_token)
    print("GitHub PAT:", github_pat)
    print("Slack Token:", slack_token)
    return "Secrets printed – SonarLint should catch these!"

default_args = {
    'owner': 'bala',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'exposed_secrets_dag',
    default_args=default_args,
    description='A test DAG with hardcoded secrets to trigger SonarLint',
    schedule_interval=timedelta(days=1),
)

trigger_leaks = PythonOperator(
    task_id='leak_secrets',
    python_callable=leak_secrets,
    dag=dag,
)
