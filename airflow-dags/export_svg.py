# -*- coding: utf-8 -*-

from datetime import datetime as dt
import requests, os

import airflow
from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyDatasetOperator,
)

from airflow.operators import BashOperator

from debussy.operators.basic import StartOperator, FinishOperator

try:
    ENV_LEVEL = conf.get("core", "env_level")
except airflow.exceptions.AirflowConfigException:
    ENV_LEVEL = "dev"

PROJECT_ID = "modular-aileron-191222"
if ENV_LEVEL == "prod":
    PROJECT_ID = "modular-aileron-191222"

CONFIG_BY_ENV = {
    "dev": {
        "region": "us-central1",
        "zone": "us-central1-f",
        "export_bucket": "dotz-export-files",
        "dataset_raw": "LEGALIST_RAW",
        "dataset_quality": "LEGALIST_QUALITY",
        "dataset_trusted": "LEGALIST_TRUSTED",
        "dataset_dimensions": "LEGALIST_DIMS",
        "table_raw_zone": "civil_cases_raw",
        "table_quality_zone": "civil_cases_quality",
        "table_trusted_zone": "civil_cases_trusted",
        "file_name": "cv88on.gz",
        "new_file": "fjc_idb_civil_cases_historical.txt",
    },
    "prod": {
        "region": "us-central1",
        "zone": "us-central1-f",
        "export_bucket": "dotz-export-files",
        "dataset_raw": "LEGALIST_RAW",
        "dataset_quality": "LEGALIST_QUALITY",
        "dataset_trusted": "LEGALIST_TRUSTED",
        "dataset_dimensions": "LEGALIST_DIMS",
        "table_raw_zone": "civil_cases_raw",
        "table_quality_zone": "civil_cases_quality",
        "table_trusted_zone": "civil_cases_trusted",
        "file_name": "cv88on.gz",
        "new_file": "fjc_idb_civil_cases_historical.txt",
    },
}

config = CONFIG_BY_ENV[ENV_LEVEL]

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": dt(2021, 5, 10, 5),
}

root_dag = DAG(
    dag_id="export_savegnago",
    schedule_interval="30 9 * * *" if ENV_LEVEL == "prod" else "@annualy",
    concurrency=5,
    catchup=False,
    default_args=default_args,
)

start_dag = StartOperator("dag", **default_args)
root_dag >> start_dag
finish_dag = FinishOperator("dag", **default_args)
root_dag >> finish_dag


def upload_file():

    url = "https://www.fjc.gov/sites/default/files/idb/textfiles/cv88on_0.zip"
    file_to_upload = requests.get(url, verify=False)

    client = storage.Client(project="dotzcloud-datalabs-sandbox")
    bucket = client.get_bucket(config["export_bucket"])
    blob = bucket.blob(config["file_name"])

    with open("/tmp/" + config["file_name"], "wb") as file:
        file.write(file_to_upload.content)

    with open("/tmp/" + config["file_name"], "rb") as send_file:
        blob.upload_from_file(send_file)


download_file_to_bucket = PythonOperator(
    task_id="download_file_to_bucket",
    python_callable=upload_file,
    **default_args,
)

remove_bad_records_from_file = BashOperator(
    task_id="remove_bad_records",
    bash_command=f"""
    gsutil cp gs://{config['export_bucket']}/{config['file_name']} - | gunzip | tr '\\0' ' ' | gsutil cp - gs://{config['export_bucket']}/{config['new_file']}
    """,
    **default_args,
)

create_dataset_raw_zone = BigQueryCreateEmptyDatasetOperator(
    project_id=PROJECT_ID,
    task_id="create_dataset_raw_zone",
    dataset_id=config["dataset_raw"],
    **default_args,
)

create_dataset_quality_zone = BigQueryCreateEmptyDatasetOperator(
    project_id=PROJECT_ID,
    task_id="create_dataset_quality_zone",
    dataset_id=config["dataset_quality"],
    **default_args,
)

create_dataset_trusted_zone = BigQueryCreateEmptyDatasetOperator(
    project_id=PROJECT_ID,
    task_id="create_dataset_trusted_zone",
    dataset_id=config["dataset_trusted"],
    **default_args,
)

create_dataset_dimensions_zone = BigQueryCreateEmptyDatasetOperator(
    project_id=PROJECT_ID,
    task_id="create_dataset_dimensions_zone",
    dataset_id=config["dataset_dimensions"],
    **default_args,
)

ingestion_raw_zone = GoogleCloudStorageToBigQueryOperator(
    task_id="ingestion_raw_zone",
    bucket=config["export_bucket"],
    source_objects=[config["new_file"]],
    destination_project_dataset_table="{}.{}.{}".format(
        PROJECT_ID, config["dataset_raw"], config["table_raw_zone"]
    ),
    schema_fields=[
        {"name": "CIRCUIT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DISTRICT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "OFFICE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DOCKET", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ORIGIN", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FILEDATE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FDATEUSE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JURIS", "type": "STRING", "mode": "NULLABLE"},
        {"name": "NOS", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TITL", "type": "STRING", "mode": "NULLABLE"},
        {"name": "SECTION", "type": "STRING", "mode": "NULLABLE"},
        {"name": "SUBSECT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RESIDENC", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JURY", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CLASSACT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DEMANDED", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FILEJUDG", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FILEMAG", "type": "STRING", "mode": "NULLABLE"},
        {"name": "COUNTY", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ARBIT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "MDLDOCK", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PLT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DEF", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRANSDAT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRANSOFF", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRANSDOC", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRANSORG", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TERMDATE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TDATEUSE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRCLACT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TERMJUDG", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TERMMAG", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PROCPROG", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DISP", "type": "STRING", "mode": "NULLABLE"},
        {"name": "NOJ", "type": "STRING", "mode": "NULLABLE"},
        {"name": "AMTREC", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JUDGMENT", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DJOINED", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PRETRIAL", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRIBEGAN", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRIALEND", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TRMARB", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PROSE", "type": "STRING", "mode": "NULLABLE"},
        {"name": "IFP", "type": "STRING", "mode": "NULLABLE"},
        {"name": "STATUSCD", "type": "STRING", "mode": "NULLABLE"},
        {"name": "TAPEYEAR", "type": "STRING", "mode": "NULLABLE"},
    ],
    source_format="CSV",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,
    field_delimiter="\t",
    autodetect=False,
    **default_args,
)

data_quality_zone = BigQueryOperator(
    task_id="data_quality_zone",
    sql=f"""
    SELECT
        CASE WHEN CIRCUIT IN ('-8') THEN NULL ELSE CAST(CIRCUIT AS INT64) END AS CIRCUIT,
        CASE WHEN DISTRICT IN ('-8') THEN NULL ELSE DISTRICT END as DISTRICT,
        CASE WHEN OFFICE IN ('-8') THEN NULL ELSE OFFICE END as OFFICE,
        CASE WHEN DOCKET IN ('-8') THEN NULL ELSE CAST(DOCKET AS INT64) END as DOCKET,
        CASE WHEN ORIGIN IN ('-8') THEN NULL ELSE CAST(ORIGIN AS INT64) END as ORIGIN,
        DATE(CONCAT(SPLIT(FILEDATE, '/')[OFFSET(2)],'-',SPLIT(FILEDATE, '/')[OFFSET(0)],'-',SPLIT(FILEDATE, '/')[OFFSET(1)])) as FILEDATE,
        DATE(CONCAT(SPLIT(FDATEUSE, '/')[OFFSET(2)],'-',SPLIT(FDATEUSE, '/')[OFFSET(0)],'-',SPLIT(FDATEUSE, '/')[OFFSET(1)])) as FDATEUSE,
        CASE WHEN JURIS IN ('-8') THEN NULL ELSE CAST(JURIS AS INT64) END as JURIS,
        CASE WHEN NOS IN ('-8') THEN NULL ELSE CAST(NOS AS INT64) END as NOS,
        CASE WHEN RESIDENC IN ('-8') THEN NULL ELSE CAST(RESIDENC AS INT64) END as RESIDENC,
        CASE WHEN JURY IN ('-8') THEN NULL ELSE JURY END as JURY,
        CASE WHEN CLASSACT IN ('-8') THEN NULL ELSE CAST(CLASSACT AS INT64) END as CLASSACT,
        CASE WHEN DEMANDED IN ('-8') THEN NULL ELSE CAST(DEMANDED AS INT64) END as DEMANDED,
        CASE WHEN COUNTY IN ('-8') THEN NULL ELSE CAST(COUNTY AS INT64) END as COUNTY,
        CASE WHEN MDLDOCK IN ('-8') THEN NULL ELSE MDLDOCK END as MDLDOCK,
        CASE WHEN PLT IN ('-8') THEN NULL ELSE PLT END as PLT,
        CASE WHEN DEF IN ('-8') THEN NULL ELSE DEF END as DEF,
        DATE(CONCAT(SPLIT(TRANSDAT, '/')[OFFSET(2)],'-',SPLIT(TRANSDAT, '/')[OFFSET(0)],'-',SPLIT(TRANSDAT, '/')[OFFSET(1)])) as TRANSDAT,
        CASE WHEN TRANSOFF IN ('-8', 'J', 'A', 'B','C','H','S','W','P','F','M','G','s') THEN NULL ELSE CAST(TRANSOFF AS INT64) END as TRANSOFF,
        CASE WHEN TRANSDOC IN ('-8', 'J', 'A', 'B','C','H','S','W','P','F','M','G','s') THEN NULL ELSE CAST(TRANSDOC AS INT64) END as TRANSDOC,
        CASE WHEN TRANSORG IN ('-8', 'J','A', 'B','C','H','S','W','P','F','M','G','s') THEN NULL ELSE CAST(TRANSORG AS INT64) END as TRANSORG,
        DATE(CONCAT(SPLIT(TERMDATE, '/')[OFFSET(2)],'-',SPLIT(TERMDATE, '/')[OFFSET(0)],'-',SPLIT(TERMDATE, '/')[OFFSET(1)])) as TERMDATE,
        DATE(CONCAT(SPLIT(TDATEUSE, '/')[OFFSET(2)],'-',SPLIT(TDATEUSE, '/')[OFFSET(0)],'-',SPLIT(TDATEUSE, '/')[OFFSET(1)])) as TDATEUSE,
        CASE WHEN TRCLACT IN ('-8') THEN NULL ELSE CAST(TRCLACT AS INT64) END as TRCLACT,
        CASE WHEN PROCPROG IN ('-8') THEN NULL ELSE CAST(PROCPROG AS INT64) END as PROCPROG,
        CASE WHEN DISP IN ('-8') THEN NULL ELSE CAST(DISP AS INT64) END as DISP,
        CASE WHEN NOJ IN ('-8') THEN NULL ELSE CAST(NOJ AS INT64) END as NOJ,
        CASE WHEN AMTREC IN ('-8','0') THEN NULL ELSE CAST(AMTREC AS INT64) END as AMTREC,
        CASE WHEN JUDGMENT IN ('-8','0') THEN NULL ELSE CAST(JUDGMENT AS INT64) END as JUDGMENT,
        CASE WHEN TRMARB IN ('-8') THEN NULL ELSE TRMARB END as TRMARB,
        CASE WHEN PROSE IN ('-8') THEN NULL ELSE CAST(PROSE AS INT64) END as PROSE,
        CASE WHEN IFP IN ('-8') THEN NULL ELSE IFP END as IFP,
        CASE WHEN STATUSCD IN ('-8') THEN NULL ELSE STATUSCD END as STATUSCD,
        CASE WHEN TAPEYEAR IN ('-8') THEN NULL ELSE CAST(TAPEYEAR AS INT64) END as TAPEYEAR
    FROM {PROJECT_ID}.{config["dataset_raw"]}.{config["table_raw_zone"]}
    """,
    destination_dataset_table=f'{PROJECT_ID}.{config["dataset_quality"]}.{config["table_quality_zone"]}',
    write_disposition="WRITE_TRUNCATE",
    allow_large_results=True,
    create_disposition="CREATE_IF_NEEDED",
    use_legacy_sql=False,
    **default_args,
)

trusted_data_zone = BigQueryOperator(
    task_id="trusted_data_zone",
    sql=f"""
    SELECT
        *
    FROM {PROJECT_ID}.{config["dataset_quality"]}.{config["table_quality_zone"]}
    """,
    destination_dataset_table=f'{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}',
    write_disposition="WRITE_TRUNCATE",
    allow_large_results=True,
    create_disposition="CREATE_IF_NEEDED",
    use_legacy_sql=False,
    **default_args,
)

create_dimensions_tables = BigQueryOperator(
    task_id="create_dimensions_tables",
    sql=f"""
    #TRANSACT

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimCircuit` AS 
    SELECT
    distinct (CIRCUIT),
    CASE WHEN CIRCUIT = 0  THEN 'District of Columbia'
        WHEN CIRCUIT = 1  THEN 'First Circuit'
        WHEN CIRCUIT = 2  THEN 'Second Circuit'
        WHEN CIRCUIT = 3  THEN 'Third Circuit'
        WHEN CIRCUIT = 4  THEN 'Fourth Circuit'
        WHEN CIRCUIT = 5  THEN 'Fifth Circuit'
        WHEN CIRCUIT = 6  THEN 'Sixth Circuit'
        WHEN CIRCUIT = 7  THEN 'Seventh Circuit'
        WHEN CIRCUIT = 8  THEN 'Eighth Circuit'
        WHEN CIRCUIT = 9  THEN 'Ninth Circuit'
        WHEN CIRCUIT = 10 THEN 'Tenth Circuit'
        WHEN CIRCUIT = 11 THEN 'Eleventh Circuit'
    END AS CIRCUIT_NAME 
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where CIRCUIT is not null
    ORDER BY CIRCUIT;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimDistrict` AS 
    SELECT
        DISTINCT(DISTRICT),
        CASE WHEN DISTRICT IN ('00') THEN 'Maine'
        WHEN DISTRICT IN ('01') THEN 'Massachusetts'
        WHEN DISTRICT IN ('02') THEN 'New Hampshire'
        WHEN DISTRICT IN ('03') THEN 'Rhode Island'
        WHEN DISTRICT IN ('04') THEN 'Puerto Rico'
        WHEN DISTRICT IN ('05') THEN 'Connecticut'
        WHEN DISTRICT IN ('06') THEN 'New York - Northern'
        WHEN DISTRICT IN ('07') THEN 'New York - Eastern'
        WHEN DISTRICT IN ('08') THEN 'New York - Southern'
        WHEN DISTRICT IN ('09') THEN 'New York - Western'
        WHEN DISTRICT IN ('10') THEN 'Vermont'
        WHEN DISTRICT IN ('11') THEN 'Delaware'
        WHEN DISTRICT IN ('12') THEN 'New Jersey'
        WHEN DISTRICT IN ('13') THEN 'Pennsylvania - Eastern'
        WHEN DISTRICT IN ('14') THEN 'Pennsylvania - Middle'
        WHEN DISTRICT IN ('15') THEN 'Pennsylvania - Western'
        WHEN DISTRICT IN ('16') THEN 'Maryland'
        WHEN DISTRICT IN ('17') THEN 'North Carolina - Eastern'
        WHEN DISTRICT IN ('18') THEN 'North Carolina - Middle'
        WHEN DISTRICT IN ('19') THEN 'North Carolina - Western'
        WHEN DISTRICT IN ('20') THEN 'South Carolina'
        WHEN DISTRICT IN ('22') THEN 'Virginia - Eastern'
        WHEN DISTRICT IN ('23') THEN 'Virginia - Western'
        WHEN DISTRICT IN ('24') THEN 'West Virginia - Northern'
        WHEN DISTRICT IN ('25') THEN 'West Virginia - Southern'
        WHEN DISTRICT IN ('26') THEN 'Alabama - Northern'
        WHEN DISTRICT IN ('27') THEN 'Alabama - Middle'
        WHEN DISTRICT IN ('28') THEN 'Alabama - Southern'
        WHEN DISTRICT IN ('29') THEN 'Florida - Northern'
        WHEN DISTRICT IN ('3A') THEN 'Florida - Middle'
        WHEN DISTRICT IN ('3C') THEN 'Florida - Southern'
        WHEN DISTRICT IN ('3E') THEN 'Georgia - Northern'
        WHEN DISTRICT IN ('3G') THEN 'Georgia - Middle'
        WHEN DISTRICT IN ('3J') THEN 'Georgia - Southern'
        WHEN DISTRICT IN ('3L') THEN 'Louisiana - Eastern '
        WHEN DISTRICT IN ('3N') THEN 'Louisiana - Middle'
        WHEN DISTRICT IN ('36') THEN 'Louisiana - Western'
        WHEN DISTRICT IN ('37') THEN 'Mississippi - Northern'
        WHEN DISTRICT IN ('38') THEN 'Mississippi - Southern'
        WHEN DISTRICT IN ('39') THEN 'Texas - Northern'
        WHEN DISTRICT IN ('40') THEN 'Texas - Eastern'
        WHEN DISTRICT IN ('41') THEN 'Texas - Southern'
        WHEN DISTRICT IN ('42') THEN 'Texas - Western'
        WHEN DISTRICT IN ('43') THEN 'Kentucky - Eastern'
        WHEN DISTRICT IN ('44') THEN 'Kentucky - Western'
        WHEN DISTRICT IN ('45') THEN 'Michigan - Eastern'
        WHEN DISTRICT IN ('46') THEN 'Michigan - Western'
        WHEN DISTRICT IN ('47') THEN 'Ohio - Northern'
        WHEN DISTRICT IN ('48') THEN 'Ohio - Southern'
        WHEN DISTRICT IN ('49') THEN 'Tennessee - Eastern'
        WHEN DISTRICT IN ('50') THEN 'Tennessee - Middle'
        WHEN DISTRICT IN ('51') THEN 'Tennessee - Western'
        WHEN DISTRICT IN ('52') THEN 'Illinois - Northern'
        WHEN DISTRICT IN ('53') THEN 'Illinois - Central'
        WHEN DISTRICT IN ('54') THEN 'Illinois - Southern'
        WHEN DISTRICT IN ('55') THEN 'Indiana - Northern'
        WHEN DISTRICT IN ('56') THEN 'Indiana - Southern'
        WHEN DISTRICT IN ('57') THEN 'Wisconsin - Eastern'
        WHEN DISTRICT IN ('58') THEN 'Wisconsin - Western'
        WHEN DISTRICT IN ('60') THEN 'Arkansas - Eastern'
        WHEN DISTRICT IN ('61') THEN 'Arkansas - Western'
        WHEN DISTRICT IN ('62') THEN 'Iowa - Northern'
        WHEN DISTRICT IN ('63') THEN 'Iowa - Southern'
        WHEN DISTRICT IN ('64') THEN 'Minnesota'
        WHEN DISTRICT IN ('65') THEN 'Missouri - Eastern'
        WHEN DISTRICT IN ('66') THEN 'Missouri - Western'
        WHEN DISTRICT IN ('67') THEN 'Nebraska'
        WHEN DISTRICT IN ('68') THEN 'North Dakota'
        WHEN DISTRICT IN ('69') THEN 'South Dakota'
        WHEN DISTRICT IN ('7-') THEN 'Alaska'
        WHEN DISTRICT IN ('70') THEN 'Arizona'
        WHEN DISTRICT IN ('71') THEN 'California - Northern'
        WHEN DISTRICT IN ('72') THEN 'California - Eastern'
        WHEN DISTRICT IN ('73') THEN 'California - Central'
        WHEN DISTRICT IN ('74') THEN 'California - Southern'
        WHEN DISTRICT IN ('75') THEN 'Hawaii'
        WHEN DISTRICT IN ('76') THEN 'Idaho'
        WHEN DISTRICT IN ('77') THEN 'Montana'
        WHEN DISTRICT IN ('78') THEN 'Nevada'
        WHEN DISTRICT IN ('79') THEN 'Oregon'
        WHEN DISTRICT IN ('80') THEN 'Washington - Eastern'
        WHEN DISTRICT IN ('81') THEN 'Washington - Western'
        WHEN DISTRICT IN ('82') THEN 'Colorado'
        WHEN DISTRICT IN ('83') THEN 'Kansas'
        WHEN DISTRICT IN ('84') THEN 'New Mexico'
        WHEN DISTRICT IN ('85') THEN 'Oklahoma - Northern'
        WHEN DISTRICT IN ('86') THEN 'Oklahoma - Eastern'
        WHEN DISTRICT IN ('87') THEN 'Oklahoma - Western'
        WHEN DISTRICT IN ('88') THEN 'Utah'
        WHEN DISTRICT IN ('89') THEN 'Wyoming'
        WHEN DISTRICT IN ('90') THEN 'District of Columbia'
        WHEN DISTRICT IN ('91') THEN 'Virgin Islands'
        WHEN DISTRICT IN ('93') THEN 'Guam'
        WHEN DISTRICT IN ('94') THEN 'Northern Mariana Islands'
        END AS DISTRICT_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where DISTRICT is not null
    ORDER BY DISTRICT;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimOrigin` AS 
    SELECT
        DISTINCT(ORIGIN),
        CASE WHEN ORIGIN = 1  THEN 'original proceeding'
            WHEN ORIGIN = 2  THEN 'removed'
            WHEN ORIGIN = 3  THEN 'remanded for further action'
            WHEN ORIGIN = 4  THEN 'reinstated/reopened'
            WHEN ORIGIN = 5  THEN 'transferred from another district'
            WHEN ORIGIN = 6  THEN 'multi district litigation'
            WHEN ORIGIN = 7  THEN 'appeal to a district judge of a magistrate judges decision'
            WHEN ORIGIN = 8  THEN 'second reopen'
            WHEN ORIGIN = 9  THEN 'third reopen'
            WHEN ORIGIN = 10 THEN 'fourth reopen'
            WHEN ORIGIN = 11 THEN 'fifth reopen'
            WHEN ORIGIN = 12 THEN 'sixth reopen'
            WHEN ORIGIN = 13 THEN 'multi district litigation originating in the district'
        END AS ORIGIN_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where ORIGIN is not null
    ORDER BY ORIGIN;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimJuris` AS 
    SELECT
        DISTINCT (JURIS),
        CASE WHEN JURIS = 1 THEN 'US government plaintiff'
            WHEN JURIS = 2 THEN 'US government defendant'
            WHEN JURIS = 3 THEN 'federal question'
            WHEN JURIS = 4 THEN 'diversity of citizenship'
            WHEN JURIS = 5 THEN 'local question'
        END AS JURIS_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where JURIS is not null
    ORDER BY JURIS;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimNos` AS 
    SELECT
        DISTINCT(NOS),
    CASE WHEN NOS = 110 THEN 'INSURANCE'
        WHEN NOS = 120 THEN 'MARINE CONTRACT ACTIONS'
        WHEN NOS = 130 THEN 'MILLER ACT'
        WHEN NOS = 140 THEN 'NEGOTIABLE INSTRUMENTS'
        WHEN NOS = 150 THEN 'OVERPAYMENTS & ENFORCEMENT OF JUDGMENTS'
        WHEN NOS = 151 THEN 'OVERPAYMENTS UNDER THE MEDICARE ACT'
        WHEN NOS = 152 THEN 'RECOVERY OF DEFAULTED STUDENT LOANS'
        WHEN NOS = 153 THEN 'RECOVERY OF OVERPAYMENTS OF VET BENEFITS'
        WHEN NOS = 160 THEN 'STOCKHOLDER S SUITS'
        WHEN NOS = 190 THEN 'OTHER CONTRACT ACTIONS'
        WHEN NOS = 195 THEN 'CONTRACT PRODUCT LIABILITY'
        WHEN NOS = 196 THEN 'CONTRACT FRANCHISE'
        WHEN NOS = 210 THEN 'LAND CONDEMNATION'
        WHEN NOS = 220 THEN 'FORECLOSURE'
        WHEN NOS = 230 THEN 'RENT, LEASE, EJECTMENT'
        WHEN NOS = 240 THEN 'TORTS TO LAND'
        WHEN NOS = 245 THEN 'TORT PRODUCT LIABILITY'
        WHEN NOS = 290 THEN 'OTHER REAL PROPERTY ACTIONS'
        WHEN NOS = 310 THEN 'AIRPLANE PERSONAL INJURY'
        WHEN NOS = 315 THEN 'AIRPLANE PRODUCT LIABILITY'
        WHEN NOS = 320 THEN 'ASSAULT, LIBEL, AND SLANDER'
        WHEN NOS = 330 THEN 'FEDERAL EMPLOYERS LIABILITY'
        WHEN NOS = 340 THEN 'MARINE PERSONAL INJURY'
        WHEN NOS = 345 THEN 'MARINE - PRODUCT LIABILITY'
        WHEN NOS = 350 THEN 'MOTOR VEHICLE PERSONAL INJURY'
        WHEN NOS = 355 THEN 'MOTOR VEHICLE PRODUCT LIABILITY'
        WHEN NOS = 360 THEN 'OTHER PERSONAL INJURY'
        WHEN NOS = 362 THEN 'MEDICAL MALPRACTICE'
        WHEN NOS = 365 THEN 'PERSONAL INJURY -PRODUCT LIABILITY'
        WHEN NOS = 367 THEN 'HEALTH CARE / PHARM'
        WHEN NOS = 368 THEN 'ASBESTOS PERSONAL INJURY - PROD.LIAB.'
        WHEN NOS = 370 THEN 'OTHER FRAUD'
        WHEN NOS = 371 THEN 'TRUTH IN LENDING'
        WHEN NOS = 375 THEN 'FALSE CLAIMS ACT'
        WHEN NOS = 380 THEN 'OTHER PERSONAL PROPERTY DAMAGE'
        WHEN NOS = 385 THEN 'PROPERTY DAMAGE -PRODUCT LIABILTY'
        WHEN NOS = 400 THEN 'STATE RE-APPORTIONMENT'
        WHEN NOS = 410 THEN 'ANTITRUST'
        WHEN NOS = 422 THEN 'BANKRUPTCY APPEALS RULE 28 USC 158'
        WHEN NOS = 423 THEN 'BANKRUPTCY WITHDRAWAL 28 USC 157'
        WHEN NOS = 430 THEN 'BANKS AND BANKING'
        WHEN NOS = 440 THEN 'OTHER CIVIL RIGHTS'
        WHEN NOS = 441 THEN 'CIVIL RIGHTS VOTING'
        WHEN NOS = 442 THEN 'CIVIL RIGHTS JOBS'
        WHEN NOS = 443 THEN 'CIVIL RIGHTS ACCOMMODATIONS'
        WHEN NOS = 444 THEN 'CIVIL RIGHTS WELFARE'
        WHEN NOS = 445 THEN 'CIVIL RIGHTS ADA EMPLOYMENT'
        WHEN NOS = 446 THEN 'CIVIL RIGHTS ADA OTHER'
        WHEN NOS = 448 THEN 'EDUCATION'
        WHEN NOS = 450 THEN 'INTERSTATE COMMERCE'
        WHEN NOS = 460 THEN 'DEPORTATION'
        WHEN NOS = 462 THEN 'NATURALIZATION, PETITION FOR HEARING OF DENIAL'
        WHEN NOS = 463 THEN 'HABEAS CORPUS – ALIEN DETAINEE'
        WHEN NOS = 465 THEN 'OTHER IMMIGRATION ACTIONS'
        WHEN NOS = 470 THEN 'CIVIL (RICO)'
        WHEN NOS = 480 THEN 'CONSUMER CREDIT'
        WHEN NOS = 490 THEN 'CABLE/SATELLITE TV'
        WHEN NOS = 510 THEN 'PRISONER PETITIONS -VACATE SENTENCE'
        WHEN NOS = 530 THEN 'PRISONER PETITIONS -HABEAS CORPUS'
        WHEN NOS = 535 THEN 'HABEAS CORPUS: DEATH PENALTY'
        WHEN NOS = 540 THEN 'PRISONER PETITIONS -MANDAMUS AND OTHER'
        WHEN NOS = 550 THEN 'PRISONER -CIVIL RIGHTS'
        WHEN NOS = 555 THEN 'PRISONER - PRISON CONDITION'
        WHEN NOS = 560 THEN 'CIVIL DETAINEE'
        WHEN NOS = 610 THEN 'AGRICULTURAL ACTS'
        WHEN NOS = 620 THEN 'FOOD AND DRUG ACTS'
        WHEN NOS = 625 THEN 'DRUG RELATED SEIZURE OF PROPERTY'
        WHEN NOS = 630 THEN 'LIQUOR LAWS'
        WHEN NOS = 640 THEN 'RAILROAD AND TRUCKS'
        WHEN NOS = 650 THEN 'AIRLINE REGULATIONS'
        WHEN NOS = 660 THEN 'OCCUPATIONAL SAFETY/HEALTH'
        WHEN NOS = 690 THEN 'OTHER FORFEITURE AND PENALTY SUITS'
        WHEN NOS = 710 THEN 'FAIR LABOR STANDARDS ACT'
        WHEN NOS = 720 THEN 'LABOR/MANAGEMENT RELATIONS ACT'
        WHEN NOS = 730 THEN 'LABOR/MANAGEMENT REPORT & DISCLOSURE'
        WHEN NOS = 740 THEN 'RAILWAY LABOR ACT'
        WHEN NOS = 751 THEN 'FAMILY AND MEDICAL LEAVE ACT'
        WHEN NOS = 790 THEN 'OTHER LABOR LITIGATION'
        WHEN NOS = 791 THEN 'EMPLOYEE RETIREMENT INCOME SECURITY ACT'
        WHEN NOS = 810 THEN 'SELECTIVE SERVICE'
        WHEN NOS = 820 THEN 'COPYRIGHT'
        WHEN NOS = 830 THEN 'PATENT'
        WHEN NOS = 840 THEN 'TRADEMARK'
        WHEN NOS = 850 THEN 'SECURITIES, COMMODITIES, EXCHANGE'
        WHEN NOS = 860 THEN 'SOCIAL SECURITY'
        WHEN NOS = 861 THEN 'HIA (1395 FF)/ MEDICARE'
        WHEN NOS = 862 THEN 'BLACK LUNG'
        WHEN NOS = 863 THEN 'D.I.W.C./D.I.W.W.'
        WHEN NOS = 864 THEN 'S.S.I.D.'
        WHEN NOS = 865 THEN 'R.S.I.'
        WHEN NOS = 870 THEN 'TAX SUITS'
        WHEN NOS = 871 THEN 'IRS 3RD PARTY SUITS 26 USC 7609'
        WHEN NOS = 875 THEN 'CUSTOMER CHALLENGE 12 USC 3410'
        WHEN NOS = 890 THEN 'OTHER STATUTORY ACTIONS'
        WHEN NOS = 891 THEN 'AGRICULTURAL ACTS'
        WHEN NOS = 892 THEN 'ECONOMIC STABILIZATION ACT'
        WHEN NOS = 893 THEN 'ENVIRONMENTAL MATTERS'
        WHEN NOS = 894 THEN 'ENERGY ALLOCATION ACT'
        WHEN NOS = 895 THEN 'FREEDOM OF INFORMATION ACT OF 1974'
        WHEN NOS = 896 THEN 'ARBITRATION'
        WHEN NOS = 899 THEN 'ADMINISTRATIVE PROCEDURE ACT/REVIEW OR APPEAL OF AGENCY DECISION'
        WHEN NOS = 900 THEN 'APPEAL OF FEE -EQUAL ACCESS TO JUSTICE'
        WHEN NOS = 910 THEN 'DOMESTIC RELATIONS'
        WHEN NOS = 920 THEN 'INSANITY'
        WHEN NOS = 930 THEN 'PROBATE'
        WHEN NOS = 940 THEN 'SUBSTITUTE TRUSTEE'
        WHEN NOS = 950 THEN 'CONSTITUTIONALITY OF STATE STATUTES'
        WHEN NOS = 990 THEN 'OTHER'
        WHEN NOS = 992 THEN 'LOCAL JURISDICTIONAL APPEAL'
        WHEN NOS = 999 THEN 'MISCELLANEOUS'
    END AS NOS_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where NOS is not null
    ORDER BY NOS;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimResidenc` AS 
    SELECT
        DISTINCT(RESIDENC),
        CASE WHEN RESIDENC = 1 THEN 'Citizen of this State'
            WHEN RESIDENC = 2 THEN 'Citizen of another State'
            WHEN RESIDENC = 3 THEN 'Citizen or Subject of a Foreign Country'
            WHEN RESIDENC = 4 THEN 'Incorporated or principal place of business in this State'
            WHEN RESIDENC = 5 THEN 'Incorporated and principal place of business in another State'
            WHEN RESIDENC = 6 THEN 'Foreign Nation'
        END AS RESIDENC_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where RESIDENC is not null
    ORDER BY RESIDENC;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimJury` AS 
    SELECT
        DISTINCT(JURY),
        CASE WHEN JURY IN ('B') THEN 'Both plaintiff and defendant demand jury'
            WHEN JURY IN ('D') THEN 'Defendant demands jury'
            WHEN JURY IN ('P') THEN 'Plaintiff demands jury'
            WHEN JURY IN ('N') THEN 'Neither plaintiff nor defendant demands jury'
        END AS JURY_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where JURY is not null
    ORDER BY JURY;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimTrclact` AS 
    SELECT
        DISTINCT(TRCLACT),
        CASE WHEN TRCLACT = 2 THEN 'denied'
            WHEN TRCLACT = 3 THEN 'granted'
        END AS TRCLACT_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where TRCLACT is not null
    ORDER BY TRCLACT;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimProcprog` AS 
    SELECT
        DISTINCT(PROCPROG),
        CASE WHEN PROCPROG = 1  THEN 'no court action'
            WHEN PROCPROG = 2  THEN 'order entered'
            WHEN PROCPROG = 3  THEN 'no court action'
            WHEN PROCPROG = 4  THEN 'judgement on motion'
            WHEN PROCPROG = 5  THEN 'pretrial conference held'
            WHEN PROCPROG = 6  THEN 'during court trial'
            WHEN PROCPROG = 7  THEN 'during jury trial'
            WHEN PROCPROG = 8  THEN 'after court trial'
            WHEN PROCPROG = 9  THEN 'after jury trial'
            WHEN PROCPROG = 10 THEN 'other'
            WHEN PROCPROG = 11 THEN 'hearing held'
            WHEN PROCPROG = 12 THEN 'order decided'
            WHEN PROCPROG = 13 THEN 'request for trial de novo after arbitration'
        END AS PROCPROG_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where PROCPROG is not null
    ORDER BY PROCPROG;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimDisp` AS 
    SELECT
        DISTINCT(DISP),
        CASE WHEN DISP = 0  THEN 'transfer to another district'
            WHEN DISP = 1  THEN 'remanded to state court'
            WHEN DISP = 2  THEN 'want of prosecution'
            WHEN DISP = 3  THEN 'lack of jurisdiction'
            WHEN DISP = 4  THEN 'default'
            WHEN DISP = 5  THEN 'consent'
            WHEN DISP = 6  THEN 'motion before trial'
            WHEN DISP = 7  THEN 'jury verdict'
            WHEN DISP = 8  THEN 'directed verdict'
            WHEN DISP = 9  THEN 'court trial'
            WHEN DISP = 10 THEN 'multi district litigation transfer'
            WHEN DISP = 11 THEN 'remanded to U.S. Agency'
            WHEN DISP = 12 THEN 'voluntarily'
            WHEN DISP = 13 THEN 'settled'
            WHEN DISP = 14 THEN 'other'
            WHEN DISP = 15 THEN 'award of arbitrator'
            WHEN DISP = 16 THEN 'stayed pending bankruptcy'
            WHEN DISP = 17 THEN 'other'
            WHEN DISP = 18 THEN 'statistical closing'
            WHEN DISP = 19 THEN 'appeal affirmed'
            WHEN DISP = 20 THEN 'appeal denied'
        END AS DISP_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where DISP is not null
    ORDER BY DISP;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimNoj` AS 
    SELECT
        DISTINCT(NOJ),
        CASE WHEN NOJ = 0 THEN 'no monetary award'
            WHEN NOJ = 1 THEN 'monetary award only'
            WHEN NOJ = 2 THEN 'monetary award and other'
            WHEN NOJ = 3 THEN 'injunction'
            WHEN NOJ = 4 THEN 'forfeiture/foreclosure/condemnation, etc'
            WHEN NOJ = 5 THEN 'costs only'
            WHEN NOJ = 6 THEN 'costs and attorney fees'
        END AS NOJ_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where NOJ is not null
    ORDER BY NOJ;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimJudgment` AS 
    SELECT
        DISTINCT(JUDGMENT),
        CASE WHEN JUDGMENT = 1 THEN 'plaintiff'
            WHEN JUDGMENT = 2 THEN 'defendant'
            WHEN JUDGMENT = 3 THEN 'both'
            WHEN JUDGMENT = 4 THEN 'unknown'
        END AS JUDGMENT_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where JUDGMENT is not null
    ORDER BY JUDGMENT;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimTrmarb` AS 
    SELECT
        DISTINCT(TRMARB),
        CASE WHEN TRMARB IN ('M') THEN 'mandatory'
            WHEN TRMARB IN ('V') THEN 'voluntary'
            WHEN TRMARB IN ('E') THEN 'exempt'
        END AS TRMARB_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where TRMARB is not null
    ORDER BY TRMARB;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimProse` AS 
    SELECT
        DISTINCT(PROSE),
        CASE WHEN PROSE = 0 THEN 'no Pro Se plaintiffs or defendants'
            WHEN PROSE = 1 THEN 'Pro Se plaintiffs, but no Pro Se defendants'
            WHEN PROSE = 2 THEN 'Pro Se defendants, but no Pro Se plaintiffs'
            WHEN PROSE = 3 THEN 'both Pro Se plaintiffs & defendants'
        END AS PROSE_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where PROSE is not null
    ORDER BY PROSE;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{config["dataset_dimensions"]}.DimStatuscd` AS 
    SELECT
        DISTINCT(STATUSCD),
        CASE WHEN STATUSCD IN ('S') THEN 'pending record'
            WHEN STATUSCD IN ('L') THEN 'terminated record'
        END AS STATUSCD_NAME
    FROM `{PROJECT_ID}.{config["dataset_trusted"]}.{config["table_trusted_zone"]}`
    where STATUSCD is not null
    ORDER BY STATUSCD;
    """,
    destination_dataset_table=None,
    write_disposition=None,
    allow_large_results=True,
    create_disposition=None,
    use_legacy_sql=False,
    **default_args,
)

(
    start_dag
    >> download_file_to_bucket
    >> remove_bad_records_from_file
    >> create_dataset_dimensions_zone
    >> create_dataset_raw_zone
    >> create_dataset_quality_zone
    >> create_dataset_trusted_zone
    >> ingestion_raw_zone
    >> data_quality_zone
    >> trusted_data_zone
    >> create_dimensions_tables
    >> finish_dag
)
