import requests
from airflow.models import Variable


def send_slack_notification(context):
    slack_webhook_url = Variable.get("SLACK_WEBHOOK_URL")
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date').isoformat()
    log_url = task_instance.log_url

    message = f"Task {task_id} in DAG {dag_id} failed."

    slack_msg = {
        "text": f"Airflow alert: {message}",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {
                        "title": "Task",
                        "value": task_id,
                        "short": True
                    },
                    {
                        "title": "Dag",
                        "value": dag_id,
                        "short": True
                    },
                    {
                        "title": "Execution Time",
                        "value": execution_date,
                        "short": False
                    },
                    {
                        "title": "Log URL",
                        "value": log_url,
                        "short": False
                    }
                ]
            }
        ]
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.post(slack_webhook_url, json=slack_msg, headers=headers)
    if response.status_code != 200:
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")
