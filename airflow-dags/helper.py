# -*- coding: utf-8 -*-

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime as dt

def slack_alert(name, headline, slack_conn_id):
    def _internal(context):
        slack_webhook_token = BaseHook.get_connection(slack_conn_id).password

        slack_msg = """
                [{env_level}] {headline}
                *Dag*: {dag}
                *Task*: {task}
                *Execution Datetime (scheduler)*: {scheduler_exec_date}
                *Execution Datetime*: {exec_date}
                *Log Url*: {log_url}
                """.format(
                    env_level='prod',
                    headline=headline,
                    dag=context.get('task_instance').dag_id,
                    task=context.get('task_instance').task_id,
                    scheduler_exec_date=context.get('execution_date'),
                    exec_date=dt.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00'),
                    log_url=context.get('task_instance').log_url,
                )

        alert_task = SlackWebhookOperator(
            task_id='slack_{}_alert'.format(name),
            http_conn_id=slack_conn_id,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow'
        )

        return alert_task.execute(context=context)

    return _internal