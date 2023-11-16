from pyspark.sql.functions import *
from pyspark.sql.types import *

class DBZ_CDC:
    msg_schema = StructType(fields=[
        StructField('schema', StringType(), False),
        StructField('payload', StructType(fields=[
            StructField('after', StringType(), False),
            StructField('patch', StringType(), False),
            StructField('filter', StringType(), False),
            StructField('source', StringType(), False)
            ]), False)
        ])

    filter_schema = StructType(fields=[
        StructField('_id', StringType(), False)
        ])

    patch_schema = StructType(fields=[
        StructField('$v', StringType(), False),
        StructField('$set', StringType(), False)
        ])

    source_schema = StructType(fields=[
        StructField('db', StringType(), False),
        StructField('collection', StringType(), False),
        StructField('ts_ms', LongType(), False)
        ])

    hubilousers_schema = StructType(fields=[
        StructField('_id', StructType(
            fields=[
                    StructField('$oid', StringType(), False),
                ]
            ), False),
        StructField('email', StringType(), False),
        StructField('organiser_id', StringType(), False),
        ])

    hubilousers_patch_schema = StructType(fields=[
    StructField('$oid', StringType(), False)
    ])

    hubilousers_patch_set_schema = StructType(fields=[
        StructField('email', StringType(), False),
        StructField('organiser_id', StringType(), False)
        ])

    community_schema = StructType(fields=[
        StructField('_id', StructType(
            fields=[
                    StructField('$oid', StringType(), False),
                ]
            ), False),
        StructField('event_id', LongType(), False),
        StructField('exhibitor_id', StringType(), False),
        StructField('firstName', StringType(), False),
        StructField('lastName', StringType(), False),
        StructField('immutable', StructType(
            fields=[
                    StructField('initLoginAt', StringType(), False),
                    StructField('isLoggedIn', StringType(), False)
                ]
            ), False),
        StructField('localCreatedAt', DoubleType(), False),
        StructField('organiser_id', StringType(), False),
        StructField('source', StringType(), False),
        StructField('speaker_id', LongType(), False),
        StructField('userId', StructType(
            fields=[
                    StructField('$oid', StringType(), False),
                ]
            ), False)
        ])
    
    community_patch_schema = StructType(fields=[
        StructField('$oid', StringType(), False)
        ])
    
    community_patch_set_schema = StructType(fields=[
        StructField('event_id', StringType(), False),
        StructField('organiser_id', StringType(), False),
        StructField('userId', StructType(
            fields=[
                    StructField('$oid', StringType(), False),
                ]
            ), False)
        ])

class SfProspects:
    schema = StructType(fields=[
        StructField('context', StructType(fields=[
            StructField('oId', StringType(), True),
            StructField('action', StringType(), True),
            StructField('actor', StringType(), True),
            StructField('client', StringType(), True)
            ])),
        StructField('meta', StructType(fields=[
            StructField('hubiloUserId', StringType(), True)
            ])),
        StructField('data', StructType(fields=[
            StructField('organiserId', LongType(), True),
            StructField('opportunityDetails', StructType(fields=[
                    StructField('Id', StringType(), True),
                    StructField('Amount', DoubleType(), True),
                    StructField('Currency__c', StringType(), True),
                    StructField('LastActivityDate', StringType(), True),
                    StructField('AccountId', StringType(), True),
                    StructField('IsClosed', BooleanType(), True),
                    StructField('CloseDate', DateType(), True),
                    StructField('Closed_Amount__c', DoubleType(), True),
                    StructField('ContactId', StringType(), True),
                    StructField('IsWon', BooleanType(), True),
                    StructField('Loss_Amount__c', DoubleType(), True),
                    StructField('CreatedDate', StringType(), True), #*
                    StructField('StageName', StringType(), True),
                    StructField('Name', StringType(), True),
                    StructField('LeadSource', StringType(), True),
                    StructField('LastModifiedDate', StringType(), True), #*
                    StructField('Type', StringType(), True),
                    StructField('BaseCurrency', StringType(), True),
                    StructField('CurrencyIsoCode', StringType(), True),
                ])),
            StructField('opportunityHistory', ArrayType(
                        StructType(fields=[
                            StructField('Amount', DoubleType(), True),
                            StructField('StageName', StringType(), True),
                            StructField('CreatedDate', DateType(), True),
                            StructField('OpportunityId', StringType(), True),
                        ])
                    )),
            StructField('leadDetails', StructType(fields=[
                    StructField('Id', StringType(), True),
                    StructField('Email', StringType(), True),
                    StructField('LeadSource', StringType(), True),
                    StructField('CreatedDate', StringType(), True), #*
                    StructField('ConvertedOpportunityId', StringType(), True),
                    StructField('ConvertedAccountId', StringType(), True),
                    StructField('ConvertedContactId', StringType(), True),
                    StructField('LastModifiedDate', StringType(), True), #*
                    StructField('ConvertedDate', DateType(), True),
                    StructField('IsConverted', BooleanType(), True),
                    StructField('Status', StringType(), True),
                    StructField('Name', StringType(), True),
                    StructField('Industry', StringType(), True),
                    StructField('Country', StringType(), True),
                ])),
            StructField('contacts', ArrayType(StructType(fields=[
                    StructField('Id', StringType(), True),
                    StructField('Email', StringType(), True),
                    StructField('Name', StringType(), True),
                    StructField('LeadSource', StringType(), True)
                ]))),
            
            StructField('accountDetails', StructType(fields=[
                    StructField('Name', StringType(), True),
                    StructField('Id', StringType(), True),
                    StructField('OwnerId', StringType(), True),
                    StructField('owner', StructType(fields=[
                        StructField('Name', StringType(), True),
                        StructField('Id', StringType(), True),
                    ]), True),
                ]))
            ])),
        StructField('timeStamp', LongType(), True),
        StructField('xnId', StringType(), True),
        ])


class SyncCompleted:
    schema = StructType(fields=[
        StructField('context', StructType(fields=[
            StructField('oId', StringType(), True),
            StructField('action', StringType(), True),
            StructField('actor', StringType(), True),
            StructField('client', StringType(), True)
            ]))])

class ROI_Notification:
    schema = StructType(
                [StructField('context', StructType([
                        StructField('oId', IntegerType()),
                        StructField('eId', IntegerType()),
                        StructField('action', StringType()),
                        StructField('actor', StringType()),

                    ])),
            StructField('timeStamp', LongType())
            ])