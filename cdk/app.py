#!/usr/bin/env python3
import os
import json
from constructs import Construct
from aws_cdk import (
    App,
    Environment,
    Stack,
    Duration,
    Size,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_source,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_sqs as sqs,
)


class OptimizelyE3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Retrieve account id from the stack
        account_id = Stack.of(self).account

        # IAM role for Glue to access S3
        glue_role = iam.Role(self, 'GlueRole',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
            ],
        )

        # IAM role for the Lambda function that collects object keys
        list_function_role = iam.Role(self, 'ListFunctionRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
            ],
        )

        # IAM role for the Lambda function that copies the object contents
        copy_function_role = iam.Role(self, 'CopyFunctionRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
            ],
        )

        # Dead letter queue for Import queue
        dead_letter_queue = sqs.Queue(
            self, 'DeadLetterQueue',
            visibility_timeout=Duration.minutes(10),
            enforce_ssl=True,
            retention_period=Duration.days(14),
        )

        # SQS queue to import list of objects
        import_queue = sqs.Queue(
            self, 'ImportQueue',
            visibility_timeout=Duration.minutes(15),
            enforce_ssl=True,
            retention_period=Duration.days(1),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=4,
                queue=dead_letter_queue,
            )
        )
        import_queue.grant_send_messages(list_function_role)
        import_queue.grant_consume_messages(copy_function_role)

        # SQS queue to send S3 notifications to Glue
        crawler_event_queue = sqs.Queue(
            self, 'CrawlerEventQueue',
            enforce_ssl=True,
            retention_period=Duration.days(1),
        )
        crawler_event_queue.grant(glue_role, 'sqs:*')

        # Lambda function that lists all objects
        list_function = lambda_.Function(
            self, 'ListFunction',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('src/list-objects'),
            handler='index.handler',
            role=list_function_role,
            memory_size=512,
            timeout=Duration.minutes(5),
            environment={
                'QUEUE_URL': import_queue.queue_url,
            }
        )

        # Bucket to store Parquet files
        input_bucket = s3.Bucket(self, 'Data',
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="auto-delete",
                    enabled=True,
                    expiration=Duration.days(7),
                )
            ]
        )
        input_bucket.grant_read_write(copy_function_role) # Allow copy function to write to this bucket
        input_bucket.grant_read(glue_role) # Allow glue to read from this bucket
        input_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, s3_notifications.SqsDestination(crawler_event_queue)
        )

        # Lambda function that copy over S3 objects
        copy_function = lambda_.Function(
            self, 'CopyFunction',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('src/copy-objects'),
            handler='index.handler',
            memory_size=1024,
            timeout=Duration.minutes(5),
            ephemeral_storage_size=Size.mebibytes(10240), # Request extra /tmp storage since copying
            role=copy_function_role,
            events=[
                lambda_event_source.SqsEventSource(
                    import_queue,
                    batch_size=1,
                    max_concurrency=5,
                )
            ],
            environment={
                'DESTINATION_BUCKET_NAME': input_bucket.bucket_name,
            }
        )

        # Bucket to upload query results from Athena
        results_bucket = s3.Bucket(self, 'Results',
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="auto-delete",
                    enabled=True,
                    expiration=Duration.days(7),
                )
            ]
        )

        # Glue database to store all data
        database = glue.CfnDatabase(self, 'Database',
            catalog_id=account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name='optimizely-e3',
            ),
        )

        # Crawler to index new files uploaded to S3 location
        cfn_incremental_crawler = glue.CfnCrawler(self, 'IncrementalCrawler',
            name='incremental-crawler',
            database_name=database.ref,
            role=glue_role.role_arn,
            configuration=json.dumps(
                {
                    'Version': 1.0,
                    'CrawlerOutput': {}
                }
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVENT_MODE',
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior='LOG',
                update_behavior='LOG',
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression='cron(0/15 * ? * MON-SAT *)'
            ),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path='s3://{}/'.format(input_bucket.bucket_name),
                        event_queue_arn=crawler_event_queue.queue_arn,
                        sample_size=1,
                    )
                ]
            ),
        )

        cfn_everything_crawler = glue.CfnCrawler(self, 'EverythingCrawler',
            name='everything-crawler',
            database_name=database.ref,
            role=glue_role.role_arn,
            configuration=json.dumps(
                {
                    'Version': 1.0,
                    'CrawlerOutput': {}
                }
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING',
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior='DELETE_FROM_DATABASE',
                update_behavior='UPDATE_IN_DATABASE',
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression='cron(0 4 ? * SUN *)'
            ),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path='s3://{}/'.format(input_bucket.bucket_name),
                        sample_size=1,
                    )
                ]
            ),
        )

        # Wait with creating the crawler until the queues are ready
        cfn_incremental_crawler.node.add_dependency(crawler_event_queue)
        cfn_everything_crawler.node.add_dependency(crawler_event_queue)

        # Custom Athena workgroup since the primary workgroup does not have an output location by default
        work_group = athena.CfnWorkGroup(self, 'Workgroup',
            name='optimizely-e3',
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location='s3://{}/'.format(results_bucket.bucket_name)
                )
            )
        )


app = App()
OptimizelyE3Stack(app, 'OptimizelyE3Athena',
    env=Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    ),
)
app.synth()