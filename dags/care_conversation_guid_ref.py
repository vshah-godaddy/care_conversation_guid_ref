from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from datetime import datetime, timedelta
import boto3 as boto3
from pytz import timezone
from lib.sc_emr_operators import (
    CreateEMROperator,
    CreateEMRSensor,
    AddEMRStepOperator,
    EMRStepSensor,
    TerminateEMROperator
)
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
import pendulum

schedule_interval = None
dag_start_date = datetime(2022, 1, 10)
local_tz = pendulum.timezone("America/Phoenix")
mst = timezone('MST')
now_ts = "{{ ts_nodash }}"
today = datetime.now(mst).date()
region_name = 'us-west-2'
ssm = boto3.client('ssm', region_name=region_name)
environment = ssm.get_parameter(Name="/AdminParams/Team/Environment").get('Parameter').get('Value')
s3_code_home_path = f"s3://gd-ckpetlbatch-{environment}-code/GDLakeDataProcessors/care_conversation_guid_ref"
s3_ddl_path = f"{s3_code_home_path}/ddl/care_conversation_guid_ref.ddl"
s3_py_files_path = f"{s3_code_home_path}/dist/pyspark_app/dependencies.zip"
s3_pyspark_path = f"{s3_code_home_path}/pyspark_app"

aws_conn_id = f'ckp-etl-{environment}'
database_name = "care_local"
table_name = "conversation_guid_ref"
default_data_pipeline_name = database_name.replace('_', '-') + '-' + table_name.replace('_', '-')
default_core_instance_type = 'r5.4xlarge'
dag_name = "care_conversation_guid_ref"

emr_nm = dag_name + now_ts

critical_arn = "arn:aws:sns:us-west-2:255575434142:critical-moog-ckpetlbatch-dev-private-snsmoogforincidents"
major_arn = "arn:aws:sns:us-west-2:255575434142:major-moog-ckpetlbatch-dev-private-snsmoogforincidents"
minor_arn = "arn:aws:sns:us-west-2:255575434142:minor-moog-ckpetlbatch-dev-private-snsmoogforincidents"
warning_arn = "arn:aws:sns:us-west-2:255575434142:warning-moog-ckpetlbatch-dev-private-snsmoogforincidents"

s3_bootstrap_path = f"{s3_code_home_path}/bootstrap/bootstrap.sh"
failure_email_list = ['kjetti@godaddy.com']
slack_failure_conn_ids = []

EMR_TAGS = [
    {"Key": "dataPipeline", "Value": dag_name},
    {"Key": "teamName", "Value": "EDT"},
    {"Key": "organization", "Value": "D-SPA"},
    {"Key": "onCallGroup", "Value": "DEV-EDT-OnCall"},
    {"Key": "teamSlackChannel", "Value": "edt-airflow-alerts"},
    {"Key": "managedByMWAA", "Value": "true"},
    {"Key": "doNotShutDown", "Value": "true"},
]

spark_command = "spark-submit \
                    --conf spark.driver.memoryOverhead={0} \
                    --conf spark.executor.memoryOverhead={1} \
                    --py-files {2} \
                    --driver-memory {3} \
                    --executor-memory {4} \
                    --deploy-mode client {5}"


def task_fail_slack_alert(context):
    task = context.get('task_instance').task_id
    log_url = context.get('task_instance').log_url
    dimension_tag = f"""{{"value":"{dag_name}","name":"Task"}},{{"value":"analytic","name":"Environment"}},{{"value":"{dag_name}","name":"DAG"}}"""
    trigger_tag = f"""{{"MetricName":"TaskInstanceDuration","Namespace":"AmazonMWAA","StatisticType":"Statistic","Statistic":"AVERAGE","Unit":null,"Dimensions":[{dimension_tag}],"Period":60,"EvaluationPeriods":1,"ComparisonOperator":"GreaterThanThreshold","Threshold":0.0,"TreatMissingData":"missing","EvaluateLowSampleCountPercentile":""}}"""
    slack_msg = """
                :red_circle: Task Failed.
                *Environment*: {environment}
                *Dag*: {dag}
                *Task*: {task}
                *Execution Time*: {exec_date}
                *Log Url*: {log_url}
                """.format(
        task=task,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=log_url,
        environment=environment
    )
    # Send Failure E-mail
    EmailOperator(
        task_id="failure_email",
        to=failure_email_list,
        subject=f"Failure:{dag_name} on {today}",
        html_content=f"""
                    DAG: {dag_name} Failed at {task}.
                    You will receive an E-mail notification when the issue is identified/resolved.
                    Thanks!
                    GDE Team
                    GlobalDataEngineering@godaddy.com 
                    """,
        dag=dag
    ).execute(context=context)
    # Send Slack message to all the Slack channels
    for slack_failure_conn_id in slack_failure_conn_ids:
        slack_webhook_token = BaseHook.get_connection(slack_failure_conn_id).password
        slack_failure_channel = BaseHook.get_connection(slack_failure_conn_id).schema
        failed_alert = SlackWebhookOperator(
            task_id='slack_alert',
            http_conn_id=slack_failure_conn_id,
            webhook_token=slack_webhook_token,
            channel=slack_failure_channel,
            message=slack_msg,
            link_names=True)
        failed_alert.execute(context=context)
        print(f"Slack notification was sent to #{slack_failure_channel}")
    # Send Incident to configured assignment group
    SnsPublishOperator(
        dag=dag,
        task_id="create_incident",
        target_arn=major_arn,
        subject=f"ALARM: {dag_name} DAG FAILED in {region_name}",
        message=f"""{{"AlarmName":"{dag_name} Failure","AlarmDescription":"{dag_name} Dag Failure","AWSAccountId":"255575434142","NewStateValue":"ALARM","NewStateReason":"{task}_Failure","StateChangeTime":"2022-01-24T18:42:49.638+0000","Region":"{region_name}","AlarmArn":"arn:aws:cloudwatch:us-west-2:255575434142:alarm:IncidentTest","OldStateValue":"INSUFFICIENT_DATA","Trigger":{trigger_tag}}} """
    ).execute(context=context)


default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'provide_context': True,
                'start_date': local_tz.convert(dag_start_date),
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(seconds=30),
                'concurrency': 3,
                'on_failure_callback': task_fail_slack_alert
                }

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
    catchup=False)

create_temp_tables = AddEMRStepOperator(
    task_id='create_temp_tables',
    aws_conn_id=aws_conn_id,
    aws_environment=environment,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
    step_cmd=f"hive -v --hivevar environment={environment} -f {s3_ddl_path}",
    step_name="create_temp_tables",
    action_on_failure='CONTINUE',
    dag=dag,
)

create_emr = CreateEMROperator(
    task_id='create_emr',
    aws_conn_id=aws_conn_id,
    aws_environment=environment,
    emr_cluster_name=emr_nm,
    master_instance_type='m5.4xlarge',
    num_core_nodes=8,
    core_instance_type=default_core_instance_type,
    data_pipeline_name=default_data_pipeline_name,
    emr_sc_bootstrap_file_path=s3_bootstrap_path,
    emr_step_concurrency="4",
    emr_custom_applications=[
        {"Name": "hadoop"},
        {"Name": "hive"},
        {"Name": "pig"},
        {"Name": "HCatalog"},
        {"Name": "Spark"},
        {"Name": "sqoop"}
    ],
    emr_sc_tags=EMR_TAGS,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

terminate_emr = TerminateEMROperator(
    task_id='terminate_emr',
    aws_conn_id=aws_conn_id,
    aws_environment=environment,
    provisioned_product_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value')[1] }}",
    trigger_rule=TriggerRule.ALL_DONE,
    on_failure_callback=None,
    dag=dag,
)

check_emr = CreateEMRSensor(
    task_id='check_emr',
    aws_conn_id=aws_conn_id,
    aws_environment=environment,
    provisioned_record_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value')[0] }}",
    dag=dag
)

refresh_employee_master = AddEMRStepOperator(
    task_id='refresh_employee_master',
    aws_conn_id=aws_conn_id,
    aws_environment=environment,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='check_emr', key='job_flow_id') }}",
    step_cmd=spark_command.format(1024, 2048, s3_py_files_path, '4G', '4G', s3_pyspark_path + "/CareConversationGuidRefs.py"),
    step_name="refresh_employee_master",
    action_on_failure='CONTINUE',
    dag=dag
)

check_emr.set_upstream(create_emr)
create_temp_tables.set_upstream(check_emr)
refresh_employee_master.set_upstream(create_temp_tables)
terminate_emr.set_upstream(refresh_employee_master)
