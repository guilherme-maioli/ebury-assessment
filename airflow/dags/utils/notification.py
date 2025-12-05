from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
import os


ENV = Variable.get('ENV')

slack_channels ={
    "local": "C03T5TYDGTD",
    "dev": "C03T5TYDGTD",
    "staging": "C03T5TYDGTD",
    "main": "C03Q2TZ08UR"
}

SLACK_CONN_ID = 'slack_connection_example'

def send_slack_notification(slack_msg, task_id):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    slack_task = SlackWebhookOperator(
        task_id=f'slack_notification_{task_id}',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=slack_channels[ENV],
        username='airflow'
    )

    return slack_task

def notification_failed_task_slack(context):

    slack_msg = """
        :alerta: Task Failed. 
        *Environment*: {ENV}
        *Task*: {task}  
        *Dag*: {dag} 
        *Execution Time*: {exec_date}  
        *Log Url*: {log_url} 
        """.format(
        ENV=os.environ.get("ENV"),
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )

    return send_slack_notification(slack_msg, context.get('task_instance').task_id).execute(context=context)
