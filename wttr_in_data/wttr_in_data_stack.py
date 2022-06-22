from aws_cdk import (
    Stack,
    aws_s3,
    aws_lambda,
    aws_events,
    RemovalPolicy,
    Duration,
    aws_events_targets
)
from constructs import Construct


class WttrInDataStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 Ingest bucket
        ingest_bucket = aws_s3.Bucket(
            self, "IngestBucket",
            bucket_name="wttr-in-data-ingest",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create lambda function to obtain data from wttr.in
        get_data_lambda = aws_lambda.Function(
            self, 'GetDataLambda',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            code=aws_lambda.Code.from_asset('lambda'),
            handler='get_data.handler',
            timeout=Duration.seconds(10),
            environment={
                'INGEST_BUCKET': ingest_bucket.bucket_name
            }
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

        extract_rule.add_target(
            aws_events_targets.LambdaFunction(get_data_lambda))
