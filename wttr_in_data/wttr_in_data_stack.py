from aws_cdk import (
    Stack,
    aws_s3,
    aws_lambda,
    aws_events,
    aws_kms,
    RemovalPolicy,
    Duration,
    aws_events_targets,
    aws_glue_alpha,
    aws_glue,
    aws_iam,
    CfnParameter
)

from constructs import Construct


class WttrInDataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Stack parameters
        S3_INGEST_BUCKET_NAME = CfnParameter(
            self, 'S3IngestBucketName',
            type='String',
            description='Name of S3 bucket to store ingest data',
        )

        S3_INGEST_RAW_DATA_PATH = CfnParameter(
            self, 'S3IngestRawDataPath',
            type='String',
            description='Path to raw data in S3 ingest bucket',
            default='weather_data_raw'
        )

        S3_INGEST_PARQUET_DATA_PATH = CfnParameter(
            self, 'S3IngestParquetDataPath',
            type='String',
            description='Path to parquet data in S3 ingest bucket',
            default='weather_data_parquet'
        )

        LOCATION_QUERY_STRING = CfnParameter(
            self, 'LocationQueryString',
            type='String',
            description='Location query string for weather data',
            default='Melbourne VIC'
        )

        # Create KMS Key
        my_kms_key = aws_kms.Key(
            self, 'MyKMSKey',
            enable_key_rotation=True,
            pending_window=Duration.days(7),
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create S3 Ingest bucket with optionally provided bucket name.
        if S3_INGEST_BUCKET_NAME.value is not None:
            ingest_bucket = aws_s3.Bucket(
                self, "IngestBucket",
                bucket_name=S3_INGEST_BUCKET_NAME.value_as_string,
                versioned=True,
                removal_policy=RemovalPolicy.DESTROY,
                encryption=aws_s3.BucketEncryption.KMS,
                encryption_key=my_kms_key,
                enforce_ssl=True,
            )
        else:
            ingest_bucket = aws_s3.Bucket(
                self, "IngestBucket",
                versioned=True,
                removal_policy=RemovalPolicy.DESTROY,
                encryption=aws_s3.BucketEncryption.KMS,
                encryption_key=my_kms_key,
                enforce_ssl=True,
            )

        # Create lambda function to obtain data from wttr.in
        get_data_lambda = aws_lambda.Function(
            self, 'GetDataLambda',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            code=aws_lambda.Code.from_asset('lambda'),
            handler='get_data.handler',
            timeout=Duration.seconds(10),
            environment={
                'INGEST_BUCKET': ingest_bucket.bucket_name,
                'LOCATION_QUERY_STRING': LOCATION_QUERY_STRING.value_as_string,
                'RAW_DATA_PATH': S3_INGEST_RAW_DATA_PATH.value_as_string
            },
        )

        # Grant write permission to created lambda function
        ingest_bucket.grant_write(get_data_lambda)

        # Create event rule to trigger lambda function every hour
        extract_rule = aws_events.Rule(
            self, "HourlyRule",
            schedule=aws_events.Schedule.cron(
                minute='0',
                hour='*',
                week_day='*',
                month='*',
                year='*'
            )
        )

        # Add created hourly rule to lambda function
        extract_rule.add_target(
            aws_events_targets.LambdaFunction(get_data_lambda))

        # Create Glue database using aws_glue_alpha
        glue_database = aws_glue_alpha.Database(
            self, 'WttrDatabase',
            database_name='wttr_in_data')

        # Create Glue crawler's IAM role
        glue_crawler_role = aws_iam.Role(
            self, 'WttrCrawlerRole',
            assumed_by=aws_iam.ServicePrincipal(
                'glue.amazonaws.com'),
        )

        # Add managed policy to Glue crawler role
        glue_crawler_role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))

        # Grant Glue crawler's IAM role to Ingest bucket
        ingest_bucket.grant_read_write(glue_crawler_role)

        # Grant Encrypt and Decrypt to Glue crawler's IAM role
        my_kms_key.grant_encrypt_decrypt(glue_crawler_role)

        # Create Glue Crawler for raw data
        glue_crawler_raw = aws_glue.CfnCrawler(
            self, 'WttrRawCrawler',
            role=glue_crawler_role.role_arn,
            name='wttr_in_raw_data_crawler',
            targets=aws_glue.CfnCrawler.TargetsProperty(
                s3_targets=[aws_glue.CfnCrawler.S3TargetProperty(
                    path='s3://' + ingest_bucket.bucket_name +
                    f'/{S3_INGEST_RAW_DATA_PATH.value_as_string}/',
                )]
            ),
            database_name=glue_database.database_name,
            schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior='LOG',
                update_behavior='UPDATE_IN_DATABASE'
            ),
            recrawl_policy=aws_glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            )
        )

        # Create Glue Crawler for parquet data
        glue_crawler_parquet = aws_glue.CfnCrawler(
            self, 'WttrParquetCrawler',
            role=glue_crawler_role.role_arn,
            name='wttr_in_parquet_data_crawler',
            targets=aws_glue.CfnCrawler.TargetsProperty(
                s3_targets=[aws_glue.CfnCrawler.S3TargetProperty(
                    path='s3://' + ingest_bucket.bucket_name +
                    f'/{S3_INGEST_PARQUET_DATA_PATH.value_as_string}/',
                )]
            ),
            database_name=glue_database.database_name,
            schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior='LOG',
                update_behavior='UPDATE_IN_DATABASE'
            ),
            recrawl_policy=aws_glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            )
        )

        # Create Glue ETL job to flatten data and save to S3 as parquet files
        glue_etl_job = aws_glue_alpha.Job(
            self, 'WttrETLJob',
            executable=aws_glue_alpha.JobExecutable.python_etl(
                glue_version=aws_glue_alpha.GlueVersion.V3_0,
                python_version=aws_glue_alpha.PythonVersion.THREE,
                script=aws_glue_alpha.Code.from_asset(
                    'glue/job_script.py'),
            ),
            job_name='Wttr ETL Job',
            role=glue_crawler_role,
            worker_count=1,
            worker_type=aws_glue_alpha.WorkerType.STANDARD,
            default_arguments={
                '--glue_src_db': glue_database.database_name,
                '--glue_src_tbl': f'{S3_INGEST_RAW_DATA_PATH.value_as_string}',
                '--outputDir': 's3://' + ingest_bucket.bucket_name + f'/{S3_INGEST_PARQUET_DATA_PATH.value_as_string}/',
                '--tempDir': 's3://' + ingest_bucket.bucket_name + '/glue/temp/',
            }
        )

        # Create glue workflow
        glue_workflow = aws_glue.CfnWorkflow(
            self, 'WttrWorkflow',
            name='wttr_in_workflow',
            description='Workflow to crawl raw data, ETL and save to S3 as parquet files, and crawl parquet data'
        )

        # Create raw data crawler trigger
        crawl_raw_data_trigger = aws_glue.CfnTrigger(
            self, "CrawlRawDataTrigger",
            actions=[aws_glue.CfnTrigger.ActionProperty(
                crawler_name=glue_crawler_raw.name,
                timeout=300
            )],
            name=f'Run raw data crawler - {glue_crawler_raw.name}',
            description='Crawl raw data daily at 03:00',
            workflow_name=glue_workflow.name,
            type='SCHEDULED',
            schedule='cron(0 3 * * ? *)',
            start_on_creation=True,
        )

        crawl_raw_data_trigger.add_depends_on(glue_crawler_raw)
        crawl_raw_data_trigger.add_depends_on(glue_workflow)

        # Create etl job trigger
        glue_etl_job_trigger = aws_glue.CfnTrigger(
            self, 'WttrETLJobTrigger',
            actions=[aws_glue.CfnTrigger.ActionProperty(
                job_name=glue_etl_job.job_name,
                timeout=300
            )],
            name=f'Run ETL job - {glue_etl_job.job_name}',
            description='Run ETL job after raw data crawler',
            workflow_name=glue_workflow.name,
            type='CONDITIONAL',
            predicate=aws_glue.CfnTrigger.PredicateProperty(
                conditions=[aws_glue.CfnTrigger.ConditionProperty(
                    crawler_name=glue_crawler_raw.name,
                    logical_operator='EQUALS',
                    crawl_state='SUCCEEDED'
                )]
            ),
            start_on_creation=True,
        )

        glue_etl_job_trigger.add_depends_on(glue_workflow)
        # Since aws_glue_alpha is experimental, the dependency below is not added because it would return an error: Object of type @aws-cdk/aws-glue-alpha.Job is not convertible to aws-cdk-lib.CfnResource
        # glue_etl_job_trigger.add_depends_on(glue_etl_job)

        # Create parquet data crawler trigger
        crawl_parquet_data_trigger = aws_glue.CfnTrigger(
            self, "CrawlParquetDataTrigger",
            actions=[aws_glue.CfnTrigger.ActionProperty(
                crawler_name=glue_crawler_parquet.name,
                timeout=300
            )],
            name=f'Run parquet data crawler - {glue_crawler_parquet.name}',
            description='Crawl parquet data after ETL job',
            workflow_name=glue_workflow.name,
            type='CONDITIONAL',
            predicate=aws_glue.CfnTrigger.PredicateProperty(
                conditions=[aws_glue.CfnTrigger.ConditionProperty(
                    job_name=glue_etl_job.job_name,
                    logical_operator='EQUALS',
                    state='SUCCEEDED',
                )]
            ),
            start_on_creation=True,
        )

        crawl_parquet_data_trigger.add_depends_on(glue_crawler_parquet)
        crawl_parquet_data_trigger.add_depends_on(glue_workflow)
        crawl_parquet_data_trigger.add_depends_on(crawl_raw_data_trigger)
        crawl_parquet_data_trigger.add_depends_on(glue_etl_job_trigger)
