from datetime import datetime


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, TimestampType,DateType
from pyspark.sql import functions as F
import unittest
from etl.conversation_guid_ref_etl import ConversationGuidRef
from pytz import timezone

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("conversation_guid_ref_Logic_unit_test") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_conversation_web_traffic(self):
        input_schema_msg_cus= StructType([
            StructField('conversation_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guids', ArrayType(StringType(), True), True),
        ])

        input_msg_cus = [
            ('116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558',  '2019-10-09 20:29:49.716', '2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854',' 31a8dd63-92ae-4922-8d0a-70a063e95142']),
            ('b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd']),
            ('0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7']),
            ('3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', None),
            ('hjsghsjkdd28378291', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', None),
        ]

        input_schema_msg = StructType([
            StructField('conversation_id', StringType(), True),
            StructField('account_id', IntegerType(), True),
            StructField('visitor_app_id', StringType(), True),
            StructField('conversation_source', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
        ])

        input_msg = [
            ('116f7d45-f027-41f4-aaa7-4652122d7c1e', 30187337, 'twilio', 'SMS', None),
            ('b18b621b-880a-487a-ada2-27e98e7d91b5', 30187337, 'twilio', 'SMS', None),
            ('3507506b-78f5-4179-b603-1740c5d24b2e', 30187337, 'twilio', 'SMS', 'Desktop'),
            ('0457bec0-2d97-4e50-85d8-490f16c3659d', 30187337, 'twilio', 'SMS', 'Desktop'),
            ('ASVSJJS', 128732, 'twilio', 'SMS', 'Desktop')
        ]

        msg_cus_df = self.spark.createDataFrame(data=input_msg_cus, schema=input_schema_msg_cus)
        msg_df = self.spark.createDataFrame(data=input_msg, schema=input_schema_msg)
        
        expected_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guids', ArrayType(StringType()), True),
            StructField('app_id', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('conversation_source', StringType(), True),
        ])

        expected_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio', None, 'SMS'),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS'),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS'),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e',  '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', None, 'twilio', 'Desktop', 'SMS')
        ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = ConversationGuidRef.transfrom_conversation_web_traffic(msg_cus_df, msg_df)
        
        transformed_df = transformed_df.select(
            transformed_df.parent_contact_id,
            transformed_df.conversation_start_utc_ts,
            transformed_df.conversation_end_utc_ts,
            transformed_df.conversation_end_utc_date,
            transformed_df.visit_guids,
            transformed_df.app_id,
            transformed_df.visitor_device_type,
            transformed_df.conversation_source
        )

        transformed_df.show()
        expected_df.show()

        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        # print(res)
        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

    def test_remove_duplicates(self):
        print('Testing Removing Duplicates')
        input_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guids', ArrayType(StringType()), True),
            StructField('app_id', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('conversation_source', StringType(), True),
        ])

        input_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio',None, 'SMS'),
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio', None, 'SMS'),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS'),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS'),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', None, 'twilio', 'Desktop', 'SMS')
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guids', ArrayType(StringType()), True),
            StructField('app_id', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('conversation_source', StringType(), True),
        ])

        expected_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio',None, 'SMS'),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS'),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS'),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', None, 'twilio', 'Desktop', 'SMS')
        ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = ConversationGuidRef.remove_duplicates(input_df)

        output_df.show()
        expected_df.show()

        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, output_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        # print(res)
        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(output_df.collect()))

    def test_add_rpt_mst_date(self):
        print('Testing ADD_RPT_MST_DATE')
        input_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guids', ArrayType(StringType()), True),
            StructField('app_id', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('conversation_source', StringType(), True),
        ])

        input_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio',None, 'SMS'),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS'),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS'),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716', '2019-10-21', None, 'twilio', 'Desktop', 'SMS')
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guids', ArrayType(StringType()), True),
            StructField('app_id', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('conversation_source', StringType(), True),
            StructField('rpt_mst_date', DateType(), True),
        ])

        expected_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio',None, 'SMS', datetime.strptime('2019-10-20', "%Y-%m-%d").date()),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS',  datetime.strptime('2019-10-20', "%Y-%m-%d").date()),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS', datetime.strptime('2019-10-20', "%Y-%m-%d").date()),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', None, 'twilio', 'Desktop', 'SMS', datetime.strptime('2019-10-20', "%Y-%m-%d").date())
        ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = ConversationGuidRef.add_rpt_mst_date(input_df)

        output_df.show()
        expected_df.show()

        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, output_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        # print(fields2)
        # print(fields1)
        # print(res)
        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(output_df.collect()))

    # def test_add_etl_build_mst_ts(self):
    #     print('Testing ADD_ETL_BUILD_MST_TS')
    #     input_schema = StructType([
    #         StructField('parent_contact_id', StringType(), True),
    #         StructField('conversation_start_utc_ts', StringType(), True),
    #         StructField('conversation_end_utc_ts', StringType(), True),
    #         StructField('conversation_end_utc_date', StringType(), True),
    #         StructField('visit_guids', ArrayType(StringType()), True),
    #         StructField('app_id', StringType(), True),
    #         StructField('visitor_device_type', StringType(), True),
    #         StructField('conversation_source', StringType(), True),
    #         StructField('rpt_mst_date', StringType(), True),
    #     ])
    #
    #     input_data = [
    #         ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio',None, 'SMS', '2019-10-20'),
    #         ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS', '2019-10-20'),
    #         ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS', '2019-10-20'),
    #         ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', None, 'twilio', 'Desktop', 'SMS','2019-10-20')
    #     ]
    #
    #     input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
    #
    #     expected_schema = StructType([
    #         StructField('parent_contact_id', StringType(), True),
    #         StructField('conversation_start_utc_ts', StringType(), True),
    #         StructField('conversation_end_utc_ts', StringType(), True),
    #         StructField('conversation_end_utc_date', StringType(), True),
    #         StructField('visit_guids', ArrayType(StringType()), True),
    #         StructField('app_id', StringType(), True),
    #         StructField('visitor_device_type', StringType(), True),
    #         StructField('conversation_source', StringType(), True),
    #         StructField('rpt_mst_date', StringType(), True),
    #         StructField('etl_build_mst_ts', TimestampType(), True),
    #     ])
    #
    #     etl_build_timestamp_ts_expected = datetime.now(timezone('MST'))
    #     expected_data = [
    #         ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['2b6315d4-1254-5b15-b6f7-1f6570893854', ' 31a8dd63-92ae-4922-8d0a-70a063e95142'], 'twilio',None, 'SMS', '2019-10-20',etl_build_timestamp_ts_expected ),
    #         ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['3762e72e-218e-5076-aed0-bb95558686fd'], 'twilio', None, 'SMS', '2019-10-20', etl_build_timestamp_ts_expected),
    #         ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ['21117d4f-3f13-5429-9d98-dcce83a889a7'], 'twilio', 'Desktop', 'SMS', '2019-10-20', etl_build_timestamp_ts_expected),
    #         ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', None, 'twilio', 'Desktop', 'SMS','2019-10-20',etl_build_timestamp_ts_expected)
    #     ]
    #
    #     expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
    #     output_df = ConversationGuidRef.add_etl_build_mst_ts(self.spark, input_df)
    #
    #     output_df.show()
    #     expected_df.show()
    #
    #     # Compare schema of transformed_df and expected_df
    #     field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    #     fields1 = [*map(field_list, output_df.schema.fields)]
    #     fields2 = [*map(field_list, expected_df.schema.fields)]
    #     res = set(fields1) == set(fields2)
    #     print(fields1)
    #     print(fields2)
    #     # print(res)
    #     # assert
    #     self.assertTrue(res)
    #     # Compare data in transformed_df and expected_df
    #     self.assertEqual(sorted(expected_df.collect()), sorted(output_df.collect()))

    def test_select_final_columns(self):
        print('Testing ADD_ETL_BUILD_MST_TS')
        input_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('visit_guid', StringType(), True),
            StructField('app_id', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('conversation_source', StringType(), True),
            StructField('rpt_mst_date', StringType(), True),
            StructField('etl_build_mst_ts', TimestampType(), True),
        ])
        etl_build_timestamp_ts = datetime.now(timezone('MST'))

        input_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', '2b6315d4-1254-5b15-b6f7-1f6570893854', 'twilio',None, 'SMS', '2019-10-20', etl_build_timestamp_ts),
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', ' 31a8dd63-92ae-4922-8d0a-70a063e95142', 'twilio',None, 'SMS', '2019-10-20', etl_build_timestamp_ts),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', '3762e72e-218e-5076-aed0-bb95558686fd', 'twilio', None, 'SMS', '2019-10-20', etl_build_timestamp_ts),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', '21117d4f-3f13-5429-9d98-dcce83a889a7', 'twilio', 'Desktop', 'SMS', '2019-10-20', etl_build_timestamp_ts),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', None, 'twilio', 'Desktop', 'SMS', '2019-10-20', etl_build_timestamp_ts)
        ]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType([
            StructField('parent_contact_id', StringType(), True),
            StructField('conversation_start_utc_ts', StringType(), True),
            StructField('conversation_end_utc_ts', StringType(), True),
            StructField('conversation_end_utc_date', StringType(), True),
            StructField('app_id', StringType(), True),
            StructField('conversation_source', StringType(), True),
            StructField('visitor_device_type', StringType(), True),
            StructField('visit_guid', StringType(), True),
            StructField('etl_build_mst_ts', TimestampType(), True),
            StructField('rpt_mst_date', StringType(), True),
        ])

        etl_build_timestamp_ts_expected = datetime.now(timezone('MST'))
        expected_data = [
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', 'twilio', 'SMS', None,'2b6315d4-1254-5b15-b6f7-1f6570893854', etl_build_timestamp_ts, '2019-10-20'),
            ('30187337-116f7d45-f027-41f4-aaa7-4652122d7c1e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', 'twilio', 'SMS', None, ' 31a8dd63-92ae-4922-8d0a-70a063e95142',  etl_build_timestamp_ts, '2019-10-20'),
            ('30187337-b18b621b-880a-487a-ada2-27e98e7d91b5', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', 'twilio', 'SMS', None,  '3762e72e-218e-5076-aed0-bb95558686fd',  etl_build_timestamp_ts, '2019-10-20'),
            ('30187337-0457bec0-2d97-4e50-85d8-490f16c3659d', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', 'twilio', 'SMS', 'Desktop', '21117d4f-3f13-5429-9d98-dcce83a889a7', etl_build_timestamp_ts, '2019-10-20'),
            ('30187337-3507506b-78f5-4179-b603-1740c5d24b2e', '2019-10-09 20:28:56.558', '2019-10-09 20:29:49.716','2019-10-21', 'twilio', 'SMS', 'Desktop', None,  etl_build_timestamp_ts, '2019-10-20')
        ]



        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        output_df = ConversationGuidRef.select_final_columns(input_df)

        output_df.show()
        expected_df.show()

        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, output_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # print(res)
        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(output_df.collect()))

if __name__ == '__main__':
    unittest.main()