import json
from logging import exception
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from schema import SfProspects, SyncCompleted, ROI_Notification
import time
from dateutil import parser
from pyspark.sql.window import Window

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

def time_parser(x):
    try:
        return parser.parse(x)
    except:
        None

def touchPointUdf(email, hubilo_email, isLoggedIn, Source):
    if email is not None and hubilo_email is not None:
        if (email.casefold() == hubilo_email.casefold()) and (isLoggedIn == True) and (Source == 'Hubilo'):
            return 1
        if (email.casefold() == hubilo_email.casefold()) and (isLoggedIn == True):
            return 2
    return 0
    
udf_touchPoint = udf(touchPointUdf, IntegerType())
time_udf = udf(time_parser, TimestampType())

def process_and_save_SFKafka_payload(streamingDf: DataFrame, _):
    print('*******process_and_save_SFKafka_payload****')
    print('*******************************************')
    ctx_streaming_df = streamingDf.selectExpr('CAST(key AS STRING)', \
            'CAST(topic AS STRING)', \
            'CAST(partition AS INTEGER)', \
            'CAST(offset AS INTEGER)', \
            'CAST(timestamp AS STRING)', \
            'CAST(timestampType AS STRING)', \
            'CAST(value AS STRING)') \
                .withColumn('len', length('value')) \
                .withColumn('_value', from_json("value", SfProspects.schema)) 
    
    sf_df = ctx_streaming_df.select(
        col('_value.data.organiserId').alias('organiser_Id').cast(LongType()),
        col('_value.data.opportunityDetails.Id').alias('opportunity_Id'),
        col('_value.data.opportunityDetails.Amount').alias('opportunity_Amount'),
        col('_value.data.opportunityDetails.CurrencyIsoCode').alias('opportunity_Currency'),
        col('_value.data.opportunityDetails.LastActivityDate').alias('opportunity_LastActivityDate'),
        col('_value.data.opportunityDetails.AccountId').alias('opportunity_AccountId'),
        col('_value.data.opportunityDetails.IsClosed').alias('opportunity_IsClosed'),
        col('_value.data.opportunityDetails.CloseDate').alias('opportunity_CloseDate'),
        col('_value.data.opportunityDetails.Closed_Amount__c').alias('opportunity_Closed_Amount'),
        col('_value.data.opportunityDetails.ContactId').alias('opportunity_ContactId'),
        col('_value.data.opportunityDetails.IsWon').alias('opportunity_IsWon'),
        col('_value.data.opportunityDetails.Loss_Amount__c').alias('opportunity_Loss_Amount'),
        time_udf(col('_value.data.opportunityDetails.CreatedDate')).alias('opportunity_CreatedDate'),
        col('_value.data.opportunityDetails.StageName').alias('opportunity_StageName'),
        col('_value.data.opportunityDetails.Name').alias('opportunity_Name'),
        col('_value.data.opportunityDetails.LeadSource').alias('opportunity_LeadSource'),
        time_udf(col('_value.data.opportunityDetails.LastModifiedDate')).alias('opportunity_LastModifiedDate'),
        col('_value.data.opportunityDetails.BaseCurrency').alias('base_currency'),
        col('_value.data.opportunityDetails.Type').alias('opportunity_type'),
        col('_value.data.opportunityHistory'),
        col('_value.data.leadDetails.Id').alias('lead_Id'),
        col('_value.data.leadDetails.Email').alias('lead_Email'),
        col('_value.data.leadDetails.LeadSource').alias('lead_LeadSource'),
        time_udf(col('_value.data.leadDetails.CreatedDate')).alias('lead_CreatedDate'),
        col('_value.data.leadDetails.ConvertedOpportunityId').alias('lead_ConvertedOpportunityId'),
        col('_value.data.leadDetails.ConvertedAccountId').alias('lead_ConvertedAccountId'),
        col('_value.data.leadDetails.ConvertedContactId').alias('lead_ConvertedContactId'),
        time_udf(col('_value.data.leadDetails.LastModifiedDate')).alias('lead_LastModifiedDate'),
        col('_value.data.leadDetails.ConvertedDate').alias('lead_ConvertedDate'),
        col('_value.data.leadDetails.IsConverted').alias('lead_IsConverted'),
        col('_value.data.leadDetails.Status').alias('lead_Status'),
        col('_value.data.leadDetails.Name').alias('lead_Name'),
        col('_value.data.leadDetails.Industry').alias('lead_Industry'),
        col('_value.data.leadDetails.Country').alias('lead_Country'),
        col('_value.data.contacts').alias('contacts'),
        col('_value.data.accountDetails.Id').alias('account_Id'),
        col('_value.data.accountDetails.Name').alias('account_Name'),
        col('_value.data.accountDetails.owner.Id').alias('account_OwnerId'),
        col('_value.data.accountDetails.owner.Name').alias('account_OwnerName'),
        col('_value.timeStamp').alias('record_timeStamp')
        )

    sf_df = sf_df.withColumn('__id', when(col('lead_Id').isNotNull(), lit(col('lead_Id'))).otherwise(lit(col('opportunity_Id'))))

    sf_df = sf_df.withColumn('opportunityHistory', expr('sort_array(transform(opportunityHistory, c-> struct(c.CreatedDate, c.StageName)), False)'))
    sf_df = sf_df.withColumn('latest_opportunityHistory', element_at("opportunityHistory", 1))
    sf_df = sf_df.withColumn('Opportunity_StageName', lit(col('latest_opportunityHistory.StageName')))\
                .withColumn('OpportunityCurrentStageDate', lit(col('latest_opportunityHistory.CreatedDate')))

    sf_df = sf_df.filter(col('organiser_Id').isNotNull())

    winspec = Window.partitionBy('__id').orderBy(col('record_timeStamp').desc())
    sf_df = sf_df.withColumn('row_num', row_number().over(winspec))
    sf_df = sf_df.filter(col('row_num') == 1)

    # append the unix time_stamp column
    sf_df = sf_df.withColumn('load_timestamp', unix_timestamp())
    sf_df = sf_df.dropDuplicates()
        
    # Store parsed payload with delta-table
    deltaTable_Sf = DeltaTable.forPath(spark, __base_delta_path.format('kafka_payload_salesforce'))
    deltaTable_Sf.alias('base') \
                .merge(
                    sf_df.alias('updates'), 'base.__id = updates.__id AND base.organiser_Id = updates.organiser_Id'
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    
    print("get_SFKafka_payload_v2 -- DONE")

def process_contact_leads_opportunity_eventAgg(df_orgIds):
    print("{}: Hubilo users SF Processing begin".format(int(time.time())))
    print('*******process_contact_leads_opportunity_eventAgg************')
    
    #
    # Load user_community
    #
    user_community_table = DeltaTable.forPath(spark, __base_delta_path.format('dbs_user_community')).toDF()
    user_community_table = user_community_table\
        .withColumn('email', lower('email'))\
        .withColumnRenamed('_id', 'communities_id')\
        .withColumnRenamed('source', 'community_source')\
        .withColumn('hubilo_email', col('email'))
    
    user_community_table = user_community_table.join(df_orgIds, 'organiser_id')

    print('*******user_community_table - reading DONE!! ************')
    
    #
    # Load sf_payload
    #
    sf_df = DeltaTable.forPath(spark, __base_delta_path.format('kafka_payload_salesforce')).toDF()
    sf_df = sf_df.join(df_orgIds, 'organiser_Id')

    sf_df.persist()

    print('*******SF - reading DONE!! ************')
    
    #
    # Prepare Leads
    #
    df_leads = sf_df.select(
                    col('lead_Id').alias('SF_Id'),
                    col('lead_Name').alias('Name'),
                    col('lead_Email').alias('email'),
                    col('organiser_Id'),
                    col('opportunity_Id'),
                    col('lead_ConvertedDate').alias('ConvertedDate'),
                    col('lead_LeadSource').alias('Source'),
                    col('lead_IsConverted').alias('IsLeadConverted'),
                    col('lead_Status').alias('LeadStatus'),
                    col('lead_Country').alias('Country'),
                    col('lead_Industry').alias('Industry'),
                    col('opportunity_LastModifiedDate'),
                    col('lead_LastModifiedDate'),
                    col('opportunity_IsWon').alias('IsWon'),
                    col('opportunity_IsClosed').alias('IsClosed'),
                    col('lead_CreatedDate').alias('record_created_date'),
                    col('lead_ConvertedOpportunityId'),
                    col('lead_ConvertedAccountId'),
                    col('lead_ConvertedContactId'),
                    col('account_Id'),
                    col('account_Name'),
                    col('Opportunity_StageName'),
                    col('OpportunityCurrentStageDate'),
                    col('opportunity_Amount'),
                    col('opportunity_CloseDate'),
                    col('opportunity_Currency'),
                    col('base_currency'),
                    ).withColumn('LastSFModifiedDate', greatest('opportunity_LastModifiedDate', 'lead_LastModifiedDate'))\
                    .withColumn('recordType', lit('LEAD'))

    df_contact = sf_df.withColumn('_contacts', explode('contacts')).select('organiser_Id', 
                     col('opportunity_Id'), 
                     col('opportunity_LastModifiedDate'),
                     col('lead_LastModifiedDate'), 
                     col('opportunity_IsWon').alias('IsWon'),
                     col('opportunity_IsClosed').alias('IsClosed'),
                     col('opportunity_Amount'),
                     col('_contacts.Id').alias('SF_Id'),
                     col('_contacts.Email').alias('email'),
                     col('_contacts.Name').alias('Name'),
                     col('account_Id'),
                     col('account_Name'),
                     col('Opportunity_StageName'),
                     col('OpportunityCurrentStageDate'),
                     col('opportunity_CloseDate'),
                     col('opportunity_Currency'),
                     col('base_currency'),
                     col('_contacts.LeadSource').alias('Source'),
                     col('opportunity_CreatedDate').alias('record_created_date')
                    ).withColumn('LastSFModifiedDate', greatest('opportunity_LastModifiedDate', 'lead_LastModifiedDate'))\
                        .withColumn('recordType', lit('CONTACT'))\
                        .withColumn('ConvertedDate', lit(None).cast(DateType()))\
                        .withColumn('IsLeadConverted', lit(None).cast(BooleanType()))\
                        .withColumn('LeadStatus', lit(None).cast(StringType()))\
                        .withColumn('Country', lit(None).cast(StringType()))\
                        .withColumn('Industry', lit(None).cast(StringType()))\
                        .withColumn('lead_ConvertedOpportunityId', lit(None).cast(StringType()))\
                        .withColumn('lead_ConvertedAccountId', lit(None).cast(StringType()))\
                        .withColumn('lead_ConvertedContactId', lit(None).cast(StringType()))


    df_lead_contact = df_contact.select(df_leads.columns).union(df_leads)
    df_lead_contact = df_lead_contact.withColumn('email', lower('email'))
    df_lead_contact = df_lead_contact.withColumn('record_created_timestamp', unix_timestamp('record_created_date'))
    df_lead_contact = df_lead_contact.withColumn('record_created_date', col('record_created_date').cast(DateType()))

    # Factor exchange rate 
    df_exchange_rates = read_redshift('exchange_rates')
    df_exchange_rates.persist()

    df_lead_contact = df_lead_contact.join(df_exchange_rates, 
                            [df_lead_contact.base_currency == df_exchange_rates.target_currency, 
                            df_lead_contact.opportunity_Currency == df_exchange_rates.base_currency], 'left').drop(df_exchange_rates.base_currency)

    # filling default conversion_rate
    df_lead_contact = df_lead_contact.fillna({'conversion_rate':'1'})

    df_lead_contact = df_lead_contact.withColumn('opportunity_Amount', col('opportunity_Amount')*col('conversion_rate'))

    sf_user_community = df_lead_contact.join(user_community_table, on = ['organiser_id', 'email'], how = 'left') 
    
    sf_user_community = sf_user_community.withColumn('touch_point', udf_touchPoint('email', 'hubilo_email', 'isLoggedIn', 'Source'))

    sf_user_community = sf_user_community.withColumn("initLoginAt",col("initLoginAt")/1000).withColumn('initLoginAt', col('initLoginAt').cast(LongType()))

    #sf_user_community = sf_user_community.withColumn('touchpoint_timestamp', when(col('touch_point') == 0, lit(col('record_created_timestamp'))).otherwise(lit(col('initLoginAt'))))
    sf_user_community = sf_user_community.withColumn('touchpoint_timestamp', lit(col('record_created_timestamp')))
    sf_user_community = sf_user_community.withColumn('touchpoint_date', from_unixtime('touchpoint_timestamp').cast(DateType()))
    # append the unix time_stamp column
    sf_user_community = sf_user_community.withColumn('load_timestamp', unix_timestamp())
    
    # Duplicate Ids Removal, 
    sf_user_community = sf_user_community.filter(col('SF_Id').isNotNull())
    winspec = Window.partitionBy('organiser_Id', 'SF_Id').orderBy(col('touchpoint_timestamp').desc())
    sf_user_community = sf_user_community.withColumn('row_num', row_number().over(winspec))
    sf_user_community = sf_user_community.filter(col('row_num') == 1)
    sf_user_community = sf_user_community.drop('row_num')

    print('*******sf_user_community - DF Creation DONE!! ************')

    sf_user_community.persist()
    
    # Contact_Leads Write Delta & Redshift    
    deltaTable_contact_lead = DeltaTable.forPath(spark, __base_delta_path.format('contact'))
    deltaTable_contact_lead.alias('base') \
                .merge(
                    sf_user_community.alias('updates'), 'base.sf_id = updates.sf_id AND base.organiser_Id = updates.organiser_Id'
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

    print('*******sf_user_community - Delta Write DONE!! ************')

    write_redshift(df = sf_user_community, table = 'contact')
    
    print('*******sf_user_community - Redshift Write DONE!! ************')

    #
    # Opportunity
    #
    df_opportunity = sf_df.select(
                col('organiser_Id'),
                    col('opportunity_Id'),
                    col('opportunity_Amount'),
                    col('opportunity_Currency'),
                    col('base_Currency'),
                    col('opportunity_LastActivityDate'),
                    col('opportunity_AccountId'),
                    col('opportunity_IsClosed'),
                    col('opportunity_CloseDate'),
                    col('opportunity_Closed_Amount'),
                    col('opportunity_IsWon'),
                    col('opportunity_Loss_Amount'),
                    col('opportunity_CreatedDate'),
                    col('opportunity_StageName'),
                    col('opportunity_Name'),
                    col('opportunity_LeadSource'),
                    col('opportunity_LastModifiedDate'),
                    col('opportunity_type'),
                    col('account_Id').alias('Opportunity_AccountId'),
                    col('account_Name').alias('Opportunity_AccountName'),
                    col('account_OwnerId').alias('Opportunity_AccountOwnerId'),
                    col('account_OwnerName').alias('Opportunity_AccountOwnerName')
        ).filter(col('opportunity_Id').isNotNull() & col('organiser_Id').isNotNull())

    df_opportunity = df_opportunity.withColumn('load_timestamp', unix_timestamp())
    
    winspec = Window.partitionBy('organiser_Id', 'opportunity_Id').orderBy(col('opportunity_LastModifiedDate').desc())
    df_opportunity = df_opportunity.withColumn('row_num', row_number().over(winspec))
    df_opportunity = df_opportunity.filter(col('row_num') == 1)
    df_opportunity = df_opportunity.drop('row_num')

    # Opportunity Write Delta & Redshift
    deltaTable_opportunity = DeltaTable.forPath(spark, __base_delta_path.format('opportunity'))
    deltaTable_opportunity.alias('base') \
                .merge(
                    df_opportunity.alias('updates'), 'base.opportunity_Id = updates.opportunity_Id AND base.organiser_Id = updates.organiser_Id'
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    
    print('******* Opportunity - Delta Write DONE!! ************')
    
    write_redshift(df = df_opportunity, table = 'opportunity')
    
    print('******* Opportunity - Redshift Write DONE!! ************')

    
    #
    # Event_AGG
    #
    contacts_lead = sf_user_community.select('SF_Id', 
                                         'organiser_Id', 
                                         'event_id', 
                                         'opportunity_Id', 
                                         'initLoginAt', 
                                         'IsClosed',
                                         'IsWon', 
                                         'touch_point',
                                         'opportunity_Amount',
                                        'recordType')

    contacts_lead = contacts_lead.withColumn('ClosedWon', when((col('IsClosed') == True) & (col('IsWon') == True), lit(1)).otherwise(lit(0)))

    contacts_lead = contacts_lead.withColumn('ClosedWon', when((col('IsClosed') == True) & (col('IsWon') == True), lit(1)).otherwise(lit(0)))
    winspec = Window.partitionBy('organiser_Id', 'event_id', 'opportunity_Id').orderBy(col("opportunity_Amount").desc())
    contacts_lead = contacts_lead.dropDuplicates()
    contacts_lead = contacts_lead.withColumn('row_num', row_number().over(winspec))

    output_sf = contacts_lead.filter(col('row_num') == 1).groupBy('organiser_Id', 'event_id', 'ClosedWon')\
            .agg(
                countDistinct('opportunity_Id').alias('cnt_opportunity'),
                sum('opportunity_Amount').alias('amt_opportunity'))\
            .groupBy('organiser_Id', 'event_id')\
            .pivot('ClosedWon')\
            .sum('cnt_opportunity', 'amt_opportunity')

    if ('0_sum(cnt_opportunity)' in  output_sf.columns) == False:
        output_sf =  output_sf.withColumn('0_sum(cnt_opportunity)', lit(0).cast(LongType()))
    if ('0_sum(amt_opportunity)' in  output_sf.columns) == False:
        output_sf =  output_sf.withColumn('0_sum(amt_opportunity)', lit(0).cast(LongType()))
    if ('1_sum(cnt_opportunity)' in  output_sf.columns) == False:
        output_sf =  output_sf.withColumn('1_sum(cnt_opportunity)', lit(0).cast(LongType()))
    if ('1_sum(amt_opportunity)' in  output_sf.columns) == False:
        output_sf =  output_sf.withColumn('1_sum(amt_opportunity)', lit(0).cast(LongType()))

    output_sf = output_sf.withColumnRenamed('0_sum(cnt_opportunity)', 'cnt_opportunity_0')\
            .withColumnRenamed('0_sum(amt_opportunity)', 'amt_opportunity_0')\
            .withColumnRenamed('1_sum(cnt_opportunity)', 'cnt_opportunity_1')\
            .withColumnRenamed('1_sum(amt_opportunity)', 'amt_opportunity_1')

    output_leads = contacts_lead.filter(col('recordType') == 'LEAD')\
            .groupBy('organiser_Id', 'event_id').agg(countDistinct('SF_Id'))\
            .withColumnRenamed('count(SF_Id)', 'cnt_leads')


    user_community_table = user_community_table.withColumn('atts', when(col('initLoginAt').isNotNull(), lit(1)).otherwise(lit('0')))
    
    output_hub_usr_comm = user_community_table\
            .groupBy('organiser_id', 'event_id', 'atts')\
            .agg(countDistinct('userId'))\
            .groupBy('organiser_id', 'event_id')\
            .pivot('atts').sum('count(userId)')

    if ('0' in output_hub_usr_comm.columns) == False:
        output_hub_usr_comm = output_hub_usr_comm.withColumn('0', lit(0).cast(LongType()))
    if ('1' in output_hub_usr_comm.columns) == False:
        output_hub_usr_comm = output_hub_usr_comm.withColumn('1', lit(0).cast(LongType()))

    output_hub_usr_comm = output_hub_usr_comm.withColumnRenamed('0', 'att_0')\
            .withColumnRenamed('1', 'att_1')

    org_events_df = output_sf.select('organiser_id', 'event_id').union(output_leads.select('organiser_id', 'event_id'))
    org_events_df = org_events_df.dropDuplicates()

    roi_event_wise_agg = org_events_df.join(output_sf,['organiser_id', 'event_id'], 'left')\
            .join(output_leads, ['organiser_id', 'event_id'], 'left')\
            .join(output_hub_usr_comm, ['organiser_id', 'event_id'], 'left')

    roi_event_wise_agg = roi_event_wise_agg.na.fill(0)

    roi_event_wise_agg = roi_event_wise_agg\
            .withColumn('regn', col('att_0') + col('att_1'))\
            .withColumnRenamed('att_1', 'atts')\
            .withColumn('Opportunity_Count', col('cnt_opportunity_0') + col('cnt_opportunity_1'))\
            .withColumnRenamed('cnt_opportunity_1', 'ClosedWon_Count')\
            .withColumn('Opportunity_Amount', col('amt_opportunity_0') + col('amt_opportunity_1'))\
            .withColumnRenamed('amt_opportunity_1', 'ClosedWon_Amount')\
            .withColumnRenamed('cnt_leads', 'leads_count').select(*['organiser_id',
             'event_id',
             'Opportunity_Count',
             'ClosedWon_Count',
             'Opportunity_Amount',
             'ClosedWon_Amount',
             'leads_count',
             'regn',
             'atts'
             ])

    roi_event_wise_agg = roi_event_wise_agg.withColumn('load_timestamp', unix_timestamp())
    
    # Event_AGG Write Delta & Redshift
    deltaTable_eventAgg = DeltaTable.forPath(spark, __base_delta_path.format('event_agg'))
    deltaTable_eventAgg.alias('base') \
                .merge(
                    roi_event_wise_agg.alias('updates'), 'base.event_id = updates.event_id AND base.organiser_Id = updates.organiser_Id'
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    
    print('******* deltaTable_eventAgg - Delta Write DONE!! ************')
    
    write_redshift(df = roi_event_wise_agg, table = 'roi_event_wise_agg')

    df_exchange_rates.unpersist()
    sf_user_community.unpersist()
    sf_df.unpersist()
    print('******* deltaTable_eventAgg - Redshifit Write DONE!! ************')

def get_SFKafka_payload_v2(df_orgIds):
    print('*******get_SFKafka_payload_v2************')
    sf_streaming_df = spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", __ksfka_sf_payload_broker)\
            .option("subscribe", 'gaa.roi.sfProspects.ingest')\
            .option("startingOffsets", 'earliest')\
            .load()

    query_sf = sf_streaming_df.writeStream.foreachBatch(process_and_save_SFKafka_payload)\
            .option("checkpointLocation", __base_delta_path.format('__checkpoint/sf_ingestion'))\
            .trigger(once=True)\
            .start()
            
    query_sf.awaitTermination()

    process_contact_leads_opportunity_eventAgg(df_orgIds)
    
def roi_notification(df):
        def map_func(org_id, timeStamp):
            return {'context': {
                        'oId': org_id,
                        'eId': 0,
                        'action': 'ORGANISER_SF_DATA_PROCESSING_COMPLETED',
                        'actor': 'SYSTEM'
                    },
                    'timeStamp':timeStamp
                    }

        map_udf = udf(map_func, ROI_Notification.schema)

        df = df.withColumn('organiser_Id', col('organiser_Id').cast(LongType())).withColumn('timeStamp', unix_timestamp())
        df = df.withColumn('data', map_udf('organiser_Id', 'timeStamp'))

        lst = df.select('data').toJSON().collect()
        payload = spark.createDataFrame(lst, StringType())
        return payload

def process_syncNotification(streamingDf: DataFrame, _):
    print('*******process_syncNotification************')
    SyncCompleted_df = streamingDf.selectExpr('CAST(key AS STRING)', \
            'CAST(topic AS STRING)', \
            'CAST(partition AS INTEGER)', \
            'CAST(offset AS INTEGER)', \
            'CAST(timestamp AS STRING)', \
            'CAST(timestampType AS STRING)', \
            'CAST(value AS STRING)') \
                .withColumn('len', length('value')) \
                .withColumn('_value', from_json("value", SyncCompleted.schema))
    
    df_del_orgIds = SyncCompleted_df.select('_value.context.*').filter(col('action') == 'ORGANISER_ROI_DISABLED')\
            .select(col('oId').alias('organiser_Id')).dropDuplicates()
    
    if df_del_orgIds.count() > 0:
        #Collect the list of organisers which we need to delete from delta-table
        org_ids = [org_id.organiser_id for org_id in df_del_orgIds.select('organiser_id').collect()]
        print('delete signel received: {}'.format(org_ids))
        #
        print('deleting kafka_payload_salesforce')
        DeltaTable.forPath(spark, __base_delta_path.format('kafka_payload_salesforce')).delete(col('organiser_id').isin(org_ids))
        print('deleting dbs_user_community')
        DeltaTable.forPath(spark, __base_delta_path.format('dbs_user_community')).delete(col('organiser_id').isin(org_ids))
        print('deleting contact')
        DeltaTable.forPath(spark, __base_delta_path.format('contact')).delete(col('organiser_id').isin(org_ids))
        print('deleting opportunity')
        DeltaTable.forPath(spark, __base_delta_path.format('opportunity')).delete(col('organiser_id').isin(org_ids))
        print('deleting event_agg')
        DeltaTable.forPath(spark, __base_delta_path.format('event_agg')).delete(col('organiser_id').isin(org_ids))
        print('all data deleted for asked orgaiser')



    df_orgIds = SyncCompleted_df.select('_value.context.*').filter(col('action') == 'ORGANISER_SF_DATA_FETCHING_COMPLETED')\
            .select(col('oId').alias('organiser_Id')).dropDuplicates()

    if df_orgIds.count() > 0:
        print(df_orgIds.show())
        get_SFKafka_payload_v2(df_orgIds)

        ## Processing completion Notification
        print('sending completion notification')
        notification_payload = roi_notification(df_orgIds)
        notification_payload.select('value').selectExpr("CAST(value AS STRING)").write.format("kafka")\
            .option("kafka.bootstrap.servers", __ksfka_sf_payload_broker)\
            .option("topic", "gaa.roi.sfOrganiserSync.events")\
            .save()

        print("{}: Hubilo users SF Processing ending".format(int(time.time())))

def start_spark(app_name='roi_salesforce_processing', master='local[*]'):
    spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_sess.sparkContext.setLogLevel("ERROR")
    
    return spark_sess

spark = start_spark()

sync_notification_df = spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", __ksfka_sf_payload_broker)\
            .option("subscribe", 'gaa.roi.sfOrganiserSync.events')\
            .option("startingOffsets", 'earliest')\
            .load()

query = sync_notification_df.writeStream.foreachBatch(process_syncNotification)\
            .option("checkpointLocation", __base_delta_path.format('__checkpoint/syncNotification'))\
            .trigger(processingTime='5 minutes')\
            .start()
            
query.awaitTermination()
