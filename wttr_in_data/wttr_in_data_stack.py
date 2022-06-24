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
    aws_iam
)

from constructs import Construct


class WttrInDataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create KMS Key
        my_kms_key = aws_kms.Key(
            self, 'MyKey',
            enable_key_rotation=True,
            pending_window=Duration.days(7),
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create S3 Ingest bucket
        ingest_bucket = aws_s3.Bucket(
            self, "IngestBucket",
            bucket_name="wttr-in-data-ingest",
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
                # Hardcoded for only Melbourne for now
                'LOCATION_QUERY_STRING': 'Melbourne VIC',
                'RAW_DATA_PATH': 'weather-data-raw'
            },
        )

        # Grant write permission to created lambda function
        ingest_bucket.grant_write(get_data_lambda)

        # Create event rule to trigger lambda function every hour
        extract_rule = aws_events.Rule(
            self, "hourlyRule",
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

        # Create Glue Crawler - Manual execution
        glue_crawler = aws_glue.CfnCrawler(
            self, 'WttrCrawler',
            role=glue_crawler_role.role_arn,
            name='wttr_in_data_crawler',
            targets=aws_glue.CfnCrawler.TargetsProperty(
                s3_targets=[aws_glue.CfnCrawler.S3TargetProperty(
                    path='s3://' + ingest_bucket.bucket_name + '/weather-data-raw/',
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
                '--glue_src_tbl': 'weather_data_raw',
                '--outputDir': 's3://' + ingest_bucket.bucket_name + '/weather-data-parquet/',
                '--tempDir': 's3://' + ingest_bucket.bucket_name + '/glue/temp/',
            }
        )
