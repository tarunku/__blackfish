import json
from logging import exception
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from schema import DBZ_CDC
import time

__base_delta_path    = 's3://bucket/roi_dashboard/qaoneeks-perf/delta_table/{}'
__jdbc_redshift      = 'jdbc:redshift://host:5439/db_name'
__redshift_schema    = 'roi_salesforce_qaoneeks'
__redshift_tmp_dir   = 's3://bucket/redshift-tmp/'
__redshift_user      = 'admin'
__redshift_password  = 'JEeHfPhQ2OVxjDb'

__ksfka_sf_payload_broker        = 'sf-payload-broker:9092'
__CDC_Kafka_brokers              = 'dbz-broker:9092'
__CDC_topic_pattern              = 'qaoneeks.*.production_qaoneeks.(?:users|communities)'

def write_redshift(df, table):
    df.write\
    .format("io.github.spark_redshift_community.spark.redshift")\
    .option("dbtable", '{}.{}'.format(__redshift_schema, table))\
    .option("tempdir", __redshift_tmp_dir)\
    .option("url", __jdbc_redshift)\
    .option("user", __redshift_user)\
    .option("password", __redshift_password)\
    .option("forward_spark_s3_credentials", "true")\
    .mode("append").save()

def read_redshift(table):
    return spark.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", __jdbc_redshift) \
        .option("dbtable", '{}.{}'.format(__redshift_schema, table)) \
        .option("tempdir", __redshift_tmp_dir) \
        .option("user", __redshift_user) \
        .option("password", __redshift_password) \
        .option("forward_spark_s3_credentials", "true")\
        .load()
        
def __mongo_hubilousers(ctx_streaming_df):
    print("1. ***************************************")
    print("{}: Hubilo users CDC Processing begin".format(int(time.time())))
    
    
    hubilo_users_delta = DeltaTable.forPath(spark, __base_delta_path.format('dbs_users'))
    #
    # INSERT CASE <<ONLY INSERT IN USER AS email and organiser_id are mandatory in this collection>>
    #
    net_new = ctx_streaming_df.filter(col('after').isNotNull()).select('after')
    net_new = net_new.withColumn('_value', from_json("after", DBZ_CDC.hubilousers_schema))
    

    net_new = net_new.select('_value.*').select(
        col('_id.$oid').alias('_id'),
        col('organiser_id'),
        col('email'))
    
    net_new = net_new.filter((col('organiser_id').isNotNull()) & (col('email').isNotNull()))
    
    print('{}: hubilo user new record count: {}'.format(int(time.time()), net_new.count()))

    hubilo_users_delta.alias('base_hubilo_users') \
        .merge(net_new.alias('new_hubilo_users_rows'), 'base_hubilo_users._id = new_hubilo_users_rows._id') \
        .whenNotMatchedInsert(values =
            {
            "_id": "new_hubilo_users_rows._id",
            "organiser_id": "new_hubilo_users_rows.organiser_id",
            "email": "new_hubilo_users_rows.email",
            }
        ) \
        .execute()

    print("{}: Hubilo users CDC Processing ended".format(int(time.time())))

def __mongo_communities(ctx_streaming_df):

    print("2. ***************************************")
    print("{}: Community CDC Processing begin".format(int(time.time())))
    
    net_new = ctx_streaming_df.filter(col('after').isNotNull()).select('after', 'ts_ms')
    net_new = net_new.withColumn('_value', from_json("after", DBZ_CDC.community_schema)).select(col('_value._id.$oid').alias('_id'), 
                    '_value.event_id',
                    '_value.exhibitor_id',
                    '_value.firstName',
                    '_value.lastName',
                    '_value.immutable.initLoginAt',
                    '_value.immutable.isLoggedIn',
                    '_value.localCreatedAt',
                    '_value.organiser_id',
                    '_value.source',
                    '_value.speaker_id',
                    col('_value.userId.$oid').alias('userId'),
                    'ts_ms')

    net_new = net_new.filter(col('organiser_id').isNotNull())
    net_new = net_new.withColumn('len_first_name', length('firstName')).withColumn('len_last_name', length('lastName'))
    net_new = net_new.filter((col('organiser_id').isNotNull()) & (col('len_first_name') < 265) & (col('len_last_name') < 265))
    net_new = net_new.drop('len_first_name', 'len_last_name')
    net_new = net_new.withColumn('OPS', lit('INSERT'))


    patch_Set = ctx_streaming_df.filter(col('after').isNull())
    patch_Set = patch_Set.withColumn('_value_id', from_json("_id", DBZ_CDC.community_patch_schema))\
                .withColumn('_value_set', from_json("$set", DBZ_CDC.community_schema))

    patch_Set = patch_Set.select(col('_value_id.$oid').alias('_id'), 
                    '_value_set.event_id',
                    '_value_set.exhibitor_id',
                    '_value_set.firstName',
                    '_value_set.lastName',   
                    '_value_set.immutable.initLoginAt',
                    '_value_set.immutable.isLoggedIn',
                    '_value_set.localCreatedAt',
                    '_value_set.organiser_id',
                    '_value_set.source',
                    '_value_set.speaker_id',
                    col('_value_set.userId.$oid').alias('userId'),
                                'ts_ms')
    patch_Set = patch_Set.withColumn('OPS', lit('UPDATE'))

    ## Final list to be inserted or updated in Delta-table
    updated_Set = net_new.union(patch_Set.select(*net_new.columns))
    updated_Set = updated_Set.withColumn('isLoggedIn', col('isLoggedIn').cast(BooleanType()))\
                    .withColumn('initLoginAt', round(col('initLoginAt')).cast(LongType()))

    updated_Set = updated_Set.filter(                            
                        col('event_id').isNotNull()
                        | col('exhibitor_id').isNotNull()
                        | col('firstName').isNotNull()
                        | col('lastName').isNotNull()
                        | col('initLoginAt').isNotNull()
                        | col('isLoggedIn').isNotNull()
                        | col('localCreatedAt').isNotNull()
                        | col('organiser_id').isNotNull()
                        | col('source').isNotNull()
                        | col('speaker_id').isNotNull()
                        | col('userId').isNotNull()
                                    )

    def ascci_sum(value):
        arr = [ord(i) for i in value]
        _total = 0
        for a in arr:
            _total = _total + a
        return _total%100

    ascci_sum_udf = udf(ascci_sum, IntegerType())

    updated_Set = updated_Set.withColumn('bucket', ascci_sum_udf('_id'))

    ## lets apply the change set on delta table
    community_delta = DeltaTable.forPath(spark, __base_delta_path.format('dbs_communities'))

    ## STEP-1 we will always insert first
    insert_df = updated_Set.filter(col('OPS') == 'INSERT')
    print('{}: Community new record count: {}'.format(int(time.time()),insert_df.count()))

    community_delta.alias('base_community') \
            .merge(insert_df.alias('new_rows'), 'base_community._id = new_rows._id AND base_community.bucket = new_rows.bucket') \
            .whenNotMatchedInsert(values =
                {
                    "_id": "new_rows._id",
                    "event_id": "new_rows.event_id",
                    "exhibitor_id": "new_rows.exhibitor_id",
                    "firstName": "new_rows.firstName",
                    "lastName": "new_rows.lastName",
                    "initLoginAt": "new_rows.initLoginAt",
                    "isLoggedIn": "new_rows.isLoggedIn",
                    "localCreatedAt": "new_rows.localCreatedAt",
                    "organiser_id": "new_rows.organiser_id",
                    "source": "new_rows.source",
                    "speaker_id": "new_rows.speaker_id",
                    "userId": "new_rows.userId",
                    "bucket": "new_rows.bucket",
                
                }
            )\
            .execute()


    ## STEP-2 after insert is done lets update
    # Ensure we always select the latet updated row as multiple updates can arrive in same batch

    from pyspark.sql.window import Window

    winspec = Window.partitionBy("_id").orderBy(col("ts_ms").desc())

    update_df = updated_Set.filter(col('OPS') == 'UPDATE')
    update_df = update_df.withColumn('row_num', row_number().over(winspec)).filter(col('row_num') == 1)
    print('{}: Community updated record count: {}'.format(int(time.time()), update_df.count()))

    community_delta.alias('base_community') \
            .merge(update_df.alias('new_rows'), 'base_community._id = new_rows._id AND base_community.bucket = new_rows.bucket') \
            .whenMatchedUpdate(set =
                {
                    "event_id": when(col("new_rows.event_id").isNotNull(), col("new_rows.event_id")).otherwise(col("base_community.event_id")),
                    "initLoginAt": when(col("new_rows.initLoginAt").isNotNull(), col("new_rows.initLoginAt")).otherwise(col("base_community.initLoginAt")),
                    "isLoggedIn": when(col("new_rows.isLoggedIn").isNotNull(), col("new_rows.isLoggedIn")).otherwise(col("base_community.isLoggedIn")),
                    "localCreatedAt": when(col("new_rows.localCreatedAt").isNotNull(), col("new_rows.localCreatedAt")).otherwise(col("base_community.localCreatedAt")),
                    "organiser_id": when(col("new_rows.organiser_id").isNotNull(), col("new_rows.organiser_id")).otherwise(col("base_community.organiser_id")),
                    "userId": when(col("new_rows.userId").isNotNull(), col("new_rows.userId")).otherwise(col("base_community.userId")),
                    "firstName": when(col("new_rows.firstName").isNotNull(), col("new_rows.firstName")).otherwise(col("base_community.firstName")),
                    "lastName": when(col("new_rows.lastName").isNotNull(), col("new_rows.lastName")).otherwise(col("base_community.lastName")),
                    "source": when(col("new_rows.source").isNotNull(), col("new_rows.source")).otherwise(col("base_community.source")),
                    "speaker_id": when(col("new_rows.speaker_id").isNotNull(), col("new_rows.speaker_id")).otherwise(col("base_community.speaker_id")),
                    "exhibitor_id": when(col("new_rows.exhibitor_id").isNotNull(), col("new_rows.exhibitor_id")).otherwise(col("base_community.exhibitor_id"))
                })\
            .execute()

    print("{}: Community CDC Processing ended".format(int(time.time())))
    return updated_Set.select('_id', 'bucket').dropDuplicates()
    

def __user_communities(updated_Set_ids):
    print("3. ***************************************")
    print("{}: User-Community CDC Processing begin".format(int(time.time())))

    
    community_delta = DeltaTable.forPath(spark, __base_delta_path.format('dbs_communities'))

    updated_community = community_delta.toDF().join(updated_Set_ids, ['_id', 'bucket'])
    updated_community = updated_community.drop('bucket')

    
    users_delta = DeltaTable.forPath(spark, __base_delta_path.format('dbs_users')).toDF()
    users_delta = users_delta.withColumnRenamed('_id', 'userId')

    user_community_CDC = updated_community.join(users_delta, on = ['organiser_id', 'userId'], how = 'left')\
                    .withColumn('load_timestamp', unix_timestamp()).dropDuplicates()

    user_community_CDC.persist()

    # Update delta table
    user_community_table = DeltaTable.forPath(spark, __base_delta_path.format('dbs_user_community'))
    user_community_table.alias('base') \
                .merge(
                    user_community_CDC.alias('update'), 'base.organiser_id = update.organiser_id AND base.userId = update.userId AND base._id = update._id'
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()



    write_redshift(user_community_CDC, 'community_users' )
    
    user_community_CDC.unpersist()
    print("{}: User-Community CDC Processing ended".format(int(time.time())))

def process_stream(ctx_streamingDf: DataFrame, _):
    streaming_df = ctx_streamingDf.selectExpr('CAST(key AS STRING)', \
            'CAST(topic AS STRING)', \
            'CAST(partition AS INTEGER)', \
            'CAST(offset AS INTEGER)', \
            'CAST(timestamp AS STRING)', \
            'CAST(timestampType AS STRING)', \
            'CAST(value AS STRING)') \
                .withColumn('_value', from_json("value", DBZ_CDC.msg_schema)) \
                .withColumn('_filter', from_json("_value.payload.filter", DBZ_CDC.filter_schema)) \
                .withColumn('_patchSet', from_json("_value.payload.patch", DBZ_CDC.patch_schema)) \
                .withColumn('_source', from_json("_value.payload.source", DBZ_CDC.source_schema)) \
                    .select(
                        'topic',
                        '_source.db',
                        '_source.collection',
                        '_source.ts_ms',
                        '_filter._id',
                        '_value.payload.after',
                        '_patchSet.$v', 
                        '_patchSet.$set'
                        )
    
    streaming_df.persist()
    
    # 1. Process Hubilo Users
    __mongo_hubilousers(streaming_df.filter(col('collection') == 'hubilousers'))
    # 2. Process Community
    updated_Set_ids = __mongo_communities(streaming_df.filter(col('collection') == 'communities'))
    # 3. User-Community Join
    __user_communities(updated_Set_ids)

    streaming_df.unpersist()
    
def start_spark(app_name='roi_dbz-cdc_processing', master='local[*]'):
    spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_sess.sparkContext.setLogLevel("ERROR")
    
    return spark_sess

spark = start_spark()

ctx_streaming_df = spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", __CDC_Kafka_brokers)\
            .option("subscribePattern", __CDC_topic_pattern)\
            .option("startingOffsets", 'earliest')\
            .load()

query = ctx_streaming_df.writeStream.foreachBatch(process_stream)\
        .option("checkpointLocation", __base_delta_path.format('__checkpoint/cdc'))\
        .trigger(processingTime='5 minutes')\
        .start()

query.awaitTermination()



