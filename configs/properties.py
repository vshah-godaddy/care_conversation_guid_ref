from datetime import datetime, timedelta, date
import boto3
from pytz import timezone

class properties:
    notification_email_list = ['kjetti@godaddy.com']
    mst = timezone('MST')
    today = datetime.now(mst).date()
    yesterday = today - timedelta(days=1)
    JDBCDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # AWS Properties/variables
    region_name = 'us-west-2'
    ssm = boto3.client('ssm', region_name=region_name)
    environment = ssm.get_parameter(Name="/AdminParams/Team/Environment").get('Parameter').get('Value')
    account_id = boto3.client('sts').get_caller_identity().get('Account')
    # Hive DB details
    hive_warehouse_path = f"s3://gd-ckpetlbatch-{environment}-tmp/hive-temp/"
    hive_temp_db = "care_local"
    hive_master_table = "conversation_guid_ref"

    # S3 details
    s3_redshift_export_path = f"s3://gd-ckpetlbatch-{environment}-stg/stage/conversation_guid_ref/"

    #history_flag = True
    history_flag = False
    if history_flag:
        # yesterday = '2017-12-31'
        yesterday_minus_10days = '2016-01-01'

    else:
        yesterday_minus_10days = yesterday - timedelta(days=10)


