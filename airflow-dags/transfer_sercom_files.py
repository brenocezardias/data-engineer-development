# -*- coding: utf-8 -*-
# Nova Dag para importar arquivos da Sercom
from datetime import datetime as dt, date
from base64 import b64encode as b64e
import json

import airflow
from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable

from airflow.contrib.operators.pubsub_operator import PubSubPublishOperator

from debussy.operators.basic import StartOperator, FinishOperator

try:
    ENV_LEVEL = conf.get("core", "env_level")
except airflow.exceptions.AirflowConfigException:
    ENV_LEVEL = "dev"

PROJECT_ID = "modular-aileron-191222"
if ENV_LEVEL == "prod":
    PROJECT_ID = "dotzcloud-datalabs-sandbox"

CONFIG_BY_ENV = {
    "dev": {
        "region": "us-central1",
        "zone": "us-central1-f",
        "transfer-file-topic": "file-transfer",
        "sercom_bucket": "dotzcloud-sercom-return",
        "clube_bucket": "clube-sercom-envio-processados",
        "ftp_file": "base_alvos_clube_dotz_{}.csv".format(
            date.today().strftime("%Y_%m_%d")
        ),
    },
    "prod": {
        "region": "us-central1",
        "zone": "us-central1-f",
        "transfer-file-topic": "file-transfer",
        "sercom_bucket": "dotzcloud-sercom-return",
        "clube_bucket": "clube-sercom-envio-processados",
        "ftp_file": "base_alvos_clube_dotz_{}.csv".format(
            date.today().strftime("%Y_%m_%d")
        ),
    },
}

FTP_CONN_DATA = {
    "host_sercom": Variable.get("secret_sercom_host"),
    "port_sercom": Variable.get("secret_sercom_port"),
    "user_sercom": Variable.get("secret_sercom_user"),
    "password_sercom": Variable.get("secret_sercom_password"),
    "host_svg": Variable.get("secret_clube_host"),
    "port_svg": Variable.get("secret_clube_port"),
    "user_svg": Variable.get("secret_clube_user"),
    "password_svg": Variable.get("secret_clube_password"),
}

config = CONFIG_BY_ENV[ENV_LEVEL]

CONFIG_MESSAGE = {
    "clube_sercom": {
        "source": "ftps://{}:{}/DOTZ/RetornoDotz/*?username={}&password={}".format(
            FTP_CONN_DATA["host_sercom"],
            FTP_CONN_DATA["port_sercom"],
            FTP_CONN_DATA["user_sercom"],
            FTP_CONN_DATA["password_sercom"],
        ),
        "destination": "gs://{}/".format(config["sercom_bucket"]),
    },
    "clube_savegnago": {
        "source": "sftp://{}:{}/BASE CLUBE DOTZ/{}?username={}&password={}".format(
            FTP_CONN_DATA["host_svg"],
            FTP_CONN_DATA["port_svg"],
            config["ftp_file"],
            FTP_CONN_DATA["user_svg"],
            FTP_CONN_DATA["password_svg"],
        ),
        "destination": "gs://{}/".format(config["clube_bucket"]),
    },
}

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": dt(2021, 2, 7, 5),
}

root_dag = DAG(
    dag_id="transfer_sercom_files",
    schedule_interval="30 23 * * *" if ENV_LEVEL == "prod" else "@once",
    concurrency=5,
    catchup=False,
    default_args=default_args,
)

start_dag = StartOperator("dag", **default_args)
root_dag >> start_dag
finish_dag = FinishOperator("dag", **default_args)
root_dag >> finish_dag

message = []

for partner in ["clube_sercom", "clube_savegnago"]:
    CONFIG_FTP = CONFIG_MESSAGE[partner]
    message.append(
        {
            "source_connection_string": CONFIG_FTP["source"],
            "destination_connection_string": CONFIG_FTP["destination"],
            "remove_file": False if partner == "clube_savegnago" else True,
            "event_date": dt.now().isoformat(),
        }
    )

msg_task = PubSubPublishOperator(
    task_id="sercom-transfer-file",
    project=PROJECT_ID,
    topic=config["transfer-file-topic"],
    messages=[
        {"data": b64e(json.dumps(m).encode("utf-8")).decode("utf-8")} for m in message
    ],
    **default_args
)

start_dag >> msg_task >> finish_dag
