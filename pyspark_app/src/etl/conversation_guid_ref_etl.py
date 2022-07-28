import logging
import sys
from datetime import datetime

from pyspark.sql.functions import explode_outer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pytz import timezone
# from configs.properties import properties


class ConversationGuidRef():

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_name = "Care_Conversation_Guid_Ref_Spark"
    start_datetime = datetime.utcnow()
    curr_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def __init__(self):
        # self.startDate = properties.yesterday_minus_10days
        # self.endDate = properties.yesterday
        self.startDate = '2022-05-16'
        self.endDate = '2022-05-17'
        self.source_care_messaging_customer_cln = "live_person_cln.care_messaging_customer_cln"
        self.source_care_messaging_cln = "live_person_cln.care_messaging_cln"
        # self.target_db = properties.hive_temp_db        
        self.target_db = 'care_local'

        # self.target_table = properties.hive_master_table        
        self.target_table = 'conversation_guid_ref'

        self.target_table_redshift = "conversation_guid_ref_redshift"

    def run(self):
        spark = SparkSession \
            .builder \
            .appName(self.app_name) \
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

        # df_source_conversation_web_traffic = ConversationGuidRef.conversation_web_traffic_between_start_end_raw(spark, self.source_care_messaging_customer_cln, self.source_care_messaging_cln, self.startDate, self.endDate)

        source_msg_cus_cln = ConversationGuidRef.messaging_customer_cln_raw(spark, self.source_care_messaging_customer_cln, self.startDate, self.endDate)
        source_msg_cln = ConversationGuidRef.messaging_cln_raw(spark, self.source_care_messaging_cln, self.startDate, self.endDate)
        df_conversation_web_traffic = ConversationGuidRef.transfrom_conversation_web_traffic(source_msg_cus_cln, source_msg_cln)
        df_conversation_web_traffic = ConversationGuidRef.remove_duplicates(df_conversation_web_traffic)
        df_conversation_web_traffic = ConversationGuidRef.explode_visit_guids(df_conversation_web_traffic)
        df_conversation_web_traffic = ConversationGuidRef.remove_duplicates(df_conversation_web_traffic)
        df_conversation_web_traffic = ConversationGuidRef.add_rpt_mst_date(df_conversation_web_traffic)
        df_conversation_web_traffic = ConversationGuidRef.add_etl_build_mst_ts(spark, df_conversation_web_traffic)
        final_df = ConversationGuidRef.select_final_columns(df_conversation_web_traffic)
        ConversationGuidRef.sink(spark, final_df, self.target_db, self.target_table)

        # Run only in prod
        # ConversationGuidRef.sink_redshift(spark, final_df, self.target_db, self.target_table_redshift)

    @staticmethod
    def messaging_customer_cln_raw(spark_session: SparkSession, customer_messaging_cln: str, startDate: str, endDate: str) -> DataFrame:
        return spark_session.sql(f"""
            SELECT conversation_id,
                conversation_start_utc_ts,
                conversation_end_utc_ts,
                conversation_end_utc_date,
                visit_guids
            FROM {customer_messaging_cln}
            WHERE conversation_end_utc_date >= '{startDate}' 
                AND conversation_end_utc_date <= '{endDate}'
        """)

    @staticmethod
    def messaging_cln_raw(spark_session: SparkSession, messaging_cln: str, startDate: str, endDate: str) -> DataFrame:
        return spark_session.sql(f"""
            SELECT conversation_id,
                account_id,
                visitor_app_id,
                conversation_source,
                visitor_device_type
            FROM {messaging_cln}
            WHERE conversation_end_utc_date >= '{startDate}' 
                AND conversation_end_utc_date <= '{endDate}'
        """)

    @staticmethod
    def transfrom_conversation_web_traffic(source_msg_cus_cln: DataFrame, source_msg_cln: DataFrame) -> DataFrame:
        msg_cln_format = source_msg_cln.withColumnRenamed('visitor_app_id', 'app_id')
        df_temp = source_msg_cus_cln.join(msg_cln_format, 'conversation_id', 'inner')
        df_temp = df_temp.withColumn('parent_contact_id', F.concat(df_temp.account_id, F.lit('-'), df_temp.conversation_id))
        return df_temp.select(
            df_temp.parent_contact_id,
            df_temp.conversation_start_utc_ts,
            df_temp.conversation_end_utc_ts,
            df_temp.conversation_end_utc_date,
            df_temp.visit_guids,
            df_temp.app_id,
            df_temp.visitor_device_type,
            df_temp.conversation_source
        )


    # @staticmethod
    # def conversation_web_traffic_between_start_end_raw(spark_session: SparkSession, customer_messaging_cln, messaging_cln, startDate, endDate) -> DataFrame:
    #     return spark_session.sql(f"""
    #         SELECT CONCAT(msg_cln.account_id, '-', cus_msg_cln.conversation_id) as parent_contact_id,
    #         cus_msg_cln.conversation_start_utc_ts as conversation_start_utc_ts,
    #         cus_msg_cln.conversation_end_utc_ts as conversation_end_utc_ts,
    #         cus_msg_cln.conversation_end_utc_date  as conversation_end_utc_date,
    #         cus_msg_cln.visit_guids as visit_guids,
    #         msg_cln.visitor_app_id as app_id,
    #         msg_cln.conversation_source as conversation_source,
    #         msg_cln.visitor_device_type as visitor_device_type,
    #         CURRENT_TIMESTAMP AS etl_build_mst_ts
    #         FROM {customer_messaging_cln} cus_msg_cln
    #         inner JOIN {messaging_cln} msg_cln ON cus_msg_cln.conversation_id = msg_cln.conversation_id
    #         WHERE (cus_msg_cln.conversation_end_utc_date >= '{startDate}'
    #             AND cus_msg_cln.conversation_end_utc_date <= '{endDate}')
    #             AND (msg_cln.conversation_end_utc_date >= '{startDate}'
    #             AND msg_cln.conversation_end_utc_date <= '{endDate}')
    #         """)

    @staticmethod
    def remove_duplicates(df:DataFrame) -> DataFrame:
        return df.dropDuplicates()

    @staticmethod
    def explode_visit_guids(df: DataFrame) -> DataFrame:
        return df.select(
            df.parent_contact_id,
            df.conversation_start_utc_ts,
            df.conversation_end_utc_ts,
            df.conversation_end_utc_date,
            df.app_id,
            df.conversation_source,
            df.visitor_device_type,
            explode_outer(df.visit_guids).alias('visit_guid'),
            df.etl_build_mst_ts,
        )

    @staticmethod
    def add_rpt_mst_date(df: DataFrame) -> DataFrame:
        return df.withColumn('rpt_mst_date', F.to_date(F.from_utc_timestamp(df.conversation_end_utc_date, 'MST'),"MM-dd-yyyy"))

    @staticmethod
    def add_etl_build_mst_ts(spark_session:SparkSession, df: DataFrame):
        return  df.withColumn('etl_build_mst_ts', F.lit(str(datetime.now(timezone('MST')))).cast(TimestampType()))
    @staticmethod
    def select_final_columns(df: DataFrame) -> DataFrame:
        return df.select(
            'parent_contact_id',
            'conversation_start_utc_ts',
            'conversation_end_utc_ts',
            'conversation_end_utc_date',
            'app_id',
            'conversation_source',
            'visitor_device_type',
            'visit_guid',
            'etl_build_mst_ts',
            'rpt_mst_date'
        )

    @staticmethod
    def sink(spark_session: SparkSession, df: DataFrame, target_db:str, target_table: str):
        ConversationGuidRef.logger.info(f'Writing to {target_db}.{target_table}')
        df.createOrReplaceTempView("conversation_guid_ref_temp")
        spark_session.sql(
            f"""INSERT OVERWRITE TABLE {target_db}.{target_db} 
              select * from conversation_guid_ref_temp""")

    @staticmethod
    def sink_redshift(spark_session: SparkSession, df: DataFrame, target_db: str, target_table: str):
        ConversationGuidRef.logger.info(f'Writing to {target_db}.{target_table}')
        df.createOrReplaceTempView("conversation_guid_ref_temp")
        spark_session.sql(
            f"""INSERT OVERWRITE TABLE {target_db}.{target_db} 
                  select * from conversation_guid_ref_temp""")

if __name__ == '__main__':
    job = ConversationGuidRef()
    job.run()