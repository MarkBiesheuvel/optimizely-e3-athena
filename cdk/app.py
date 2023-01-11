#!/usr/bin/env python3
import os
from constructs import Construct
from aws_cdk import (
    App,
    Environment,
    Stack,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
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

        # SQS queues to send S3 notifications to Glue
        event_queue = sqs.Queue(self, 'EventQueue')
        event_queue.grant_consume_messages(glue_role)
        event_queue.grant(glue_role, 'sqs:SetQueueAttributes')

        # SQS queue in case Glue can not process messages
        dl_queue = sqs.Queue(self, 'DeadLetterQueue')
        dl_queue.grant_consume_messages(glue_role)
        dl_queue.grant(glue_role, 'sqs:SetQueueAttributes')

        # Bucket to store Parquet files
        input_bucket = s3.Bucket(self, 'Input')
        input_bucket.grant_read(glue_role)
        input_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3_notifications.SqsDestination(event_queue))

        # Bucket to upload query results from Athena
        results_bucket = s3.Bucket(self, 'Results')

        # Glue database to store all data
        database = glue.CfnDatabase(self, 'Database',
            catalog_id=account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name='optimizely-e3',
            ),
        )

        # Crawler to index new files uploaded to S3 location
        cfn_crawler = glue.CfnCrawler(self, 'Crawler',
            name='e3-crawler',
            database_name=database.ref,
            role=glue_role.role_arn,
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVENT_MODE',
            ),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path='s3://{}/'.format(input_bucket.bucket_name),
                        event_queue_arn=event_queue.queue_arn,
                        dlq_event_queue_arn=dl_queue.queue_arn,
                    )
                ]
            ),
        )

        # Wait with creating the crawler until the queue is ready
        cfn_crawler.node.add_dependency(event_queue)

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