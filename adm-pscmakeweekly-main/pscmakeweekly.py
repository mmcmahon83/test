#all dependencies and imports
from __future__ import annotations

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pendulum
from airflow import models

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.samba.hooks.samba import SambaHook

#mount information and DAGID
load_dotenv()
from docker.types import Mount 
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "pscreportSundaytoSunday" #update this, this will be the DAG name in Airflow

#variable assignment
local_tz = pendulum.timezone("America/Chicago") #sets timezone to CST
reportID = os.getenv(f"{DAG_ID}ri") 

def report_failure(context):
    send_email = EmailOperator(task_id="Send_Results",
         from_email="airflow@sonichealthcareusa.com",
         to= os.getenv('Callbackelist'), 
         subject=f"{reportID}{os.getenv('Callbacksubject')}",
         html_content="Report Failed",)
    send_email.execute(context)

with models.DAG(
    DAG_ID,
    schedule= os.getenv(f"{DAG_ID}sc"), 
    start_date=datetime(2021, 1, 1, tzinfo=local_tz),
    default_args = {
        "on_failure_callback": report_failure },
    catchup=False,
    tags=["docker","weekly","Nielsa","Production"],
) as dag:

    # docker operator that runs script, returns jinja that can be read to get dynamic file names
    script = DockerOperator(
        docker_url=os.getenv("docker_url"),  # Set your docker URL
        command="Rscript /opt/airflow/dags/Scripts/adm-pscmakeweekly/psc_makeWeekly.R",
        image="sonic/r-base",
        working_dir="/opt/airflow/dags/tempfiles",
        network_mode="bridge",
        task_id="script_task",
        environment={"TZ":"America/Chicago"},
        retries=3,
        retry_delay=timedelta(minutes=5),
        mount_tmp_dir=False,
        mounts=[Mount(source='/root/zdir/docker/airflow/dags', target='/opt/airflow/dags', type='bind')],
        dag=dag,
    )
        
    #operator that will convert jinga into key value pair for dynamic file produced in t2
    jin = PythonOperator(
        task_id="render_output",
        python_callable=lambda **c: c['task'].render_template(content=c['ti'].xcom_pull(task_ids=script.task_id),context=c),
        provide_context=True,
        dag=dag
    )

    def samba_write_files(**kwargs):
        ti = kwargs['ti']
        filename=ti.xcom_pull(task_ids="render_output",key="filename")
        samba = SambaHook(samba_conn_id='TDrive', share = 'Client')
        filename1 = os.path.basename(filename)

        local_path = f'/opt/airflow/dags/tempfiles/{filename1}'
        remote_path = f'/OPERATIONS/Reporting/Files from Mike Murphy/{filename1}'
        
        local_path2 = f'/opt/airflow/dags/tempfiles/{filename1}'
        remote_path2 = f'/SHARED/Files from Rick Sanders/{filename1}'

        samba.push_from_local(remote_path, local_path)
        samba.push_from_local(remote_path2, local_path2)
           
    #task to write to outside server
    wfiles = PythonOperator(
        task_id='samba_write_task',
        python_callable=samba_write_files,
        provide_context=True,
        dag=dag
    )

    #function to delete file that was imported from outside server
    def delete_files(**kwargs):
        ti = kwargs['ti']
        filename=ti.xcom_pull(task_ids="render_output",key="filename")
        os.remove(filename) #created file to remove
    #task to delete file imported from outside server
    dfiles = PythonOperator(
        task_id='delete_files_task',
        python_callable=delete_files,
        provide_context=True,
        dag=dag
        )
    
    (
        # TEST BODY
        script >> jin >> wfiles >> dfiles
    )
