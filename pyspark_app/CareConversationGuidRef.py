#####################################################################################
#                    Created By  :  Vimal Shah                                      #
#                    Created On  :  06-13-2022                                      #
#                    Created For :  Conversation Guid Reference Table               #
#                    Modified On :                                                  #
#####################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from configs.properties import properties
from pyspark.sql.functions import explode_outer


def get_df_with_insert_ts(df):
    df = df.withColumn('conversation_end_utc_ts', lit(curr_ts).cast("timestamp"))
    return df

startDate = properties.yesterday_minus_10days
endDate = properties.yesterday

if __name__ == "__main__":
    start_datetime = datetime.utcnow()
    start_ts_str = start_datetime.strftime("%Y%m%d%H%M%S")
    curr_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    current_date = start_datetime.strftime("%Y-%m-%d")

    spark = SparkSession \
        .builder \
        .appName(f"Care_Conversation_Guid_Ref_Spark") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.optimize.sort.dynamic.partition", "true") \
        .config("hive.merge.mapfiles", "true") \
        .config("hive.merge.mapredfiles", "true") \
        .config("hive.merge.size.per.task", 256000000) \
        .config("hive.merge.smallfiles.avgsize", 256000000) \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    
    
    
    df = spark.sql("""
        SELECT DISTINCT CONCAT(l2.account_id, '-', l.conversation_id) as parent_contact_id,
        l.conversation_start_utc_ts as conversation_start_utc_ts,
        l.conversation_end_utc_ts as conversation_end_utc_ts,
        l.conversation_end_utc_date  as conversation_end_utc_date,
        l.visit_guids as visit_guids,
        l2.visitor_app_id as app_id,
        l2.conversation_source as conversation_source,
        l2.visitor_device_type as visitor_device_type,
        CURRENT_TIMESTAMP AS etl_build_mst_ts
        FROM live_person_cln.care_messaging_customer_cln l
        inner JOIN live_person_cln.care_messaging_cln l2 ON l.conversation_id = l2.conversation_id
        WHERE l.conversation_end_utc_date >= '{0}'
            AND l.conversation_end_utc_date <= '{1}'
        GROUP BY
        CONCAT(l2.account_id, '-', l.conversation_id),
        l.conversation_start_utc_ts,
        l.conversation_end_utc_ts,
        l.conversation_end_utc_date,
        l.visit_guids,
        l2.visitor_app_id,
        l2.conversation_source,
        l2.visitor_device_type,
        CURRENT_TIMESTAMP
    """.format(startDate, endDate))


    df1 = df.select(df.parent_contact_id,
                    df.conversation_start_utc_ts,
                    df.conversation_end_utc_ts,
                    df.conversation_end_utc_date,
                    df.app_id,
                    df.conversation_source,
                    df.visitor_device_type,
                    explode_outer(df.visit_guids).alias('visit_guid'),
                    df.etl_build_mst_ts,
                   )
    df1.createOrReplaceTempView("conversation_guid_ref_snap")
    
    df2 = spark.sql("""select * 
                   from conversation_guid_ref_snap
                   group by parent_contact_id,
                   conversation_start_utc_ts, 
                   conversation_end_utc_ts, 
                   conversation_end_utc_date,
                   app_id,
                   conversation_source,
                   visitor_device_type,
                   visit_guid,
                   etl_build_mst_ts""")
    
    df3 = df2.select(df2.parent_contact_id,
                    df2.conversation_start_utc_ts,
                    df2.conversation_end_utc_ts,
                    df2.conversation_end_utc_date,
                    df2.app_id,
                    df2.conversation_source,
                    df2.visitor_device_type,
                    df2.visit_guid,
                    df2.etl_build_mst_ts,
                    to_date(from_utc_timestamp(df2.conversation_end_utc_date, 'MST'),"MM-dd-yyyy").alias("rpt_mst_date")
                   )
    df3.createOrReplaceTempView("conversation_guid_ref_temp")

    spark.sql(
        f"""INSERT OVERWRITE TABLE {properties.hive_temp_db}.{properties.hive_master_table} 
        select * from conversation_guid_ref_temp""")

#     spark.sql(
#         """
#         INSERT OVERWRITE TABLE {0}.conversation_guid_ref_redshift
#         SELECT * FROM conversation_guid_ref_temp
#         """.format(properties.hive_temp_db)
#     )

