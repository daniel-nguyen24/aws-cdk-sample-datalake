from aws_cdk import (
    Stack,
    aws_s3,
    assertions,
    App
)
from wttr_in_data.wttr_in_data_stack import WttrInDataStack
import pytest


def test_s3_ingest_bucket_created():
    app = App()

    test_stack = WttrInDataStack(app, "TestStack")

    template = assertions.Template.from_stack(test_stack)

    template.resource_count_is('AWS::S3::Bucket', 1)

    template.has_resource('AWS::S3::Bucket', {
        'DeletionPolicy': 'Delete',
        'UpdateReplacePolicy': 'Delete'
    })

    template.has_resource_properties('AWS::S3::Bucket', {
        'BucketName': 'wttr-in-data-ingest',
        'VersioningConfiguration': {
            'Status': 'Enabled'
        }
    })


def test_lambda_function_created():
    app = App()

    test_stack = WttrInDataStack(app, "TestStack")

    template = assertions.Template.from_stack(test_stack)

    template.resource_count_is('AWS::Lambda::Function', 1)

    env_capture = assertions.Capture()

    template.has_resource_properties('AWS::Lambda::Function',
                                     {
                                         'Handler': 'get_data.handler',
                                         'Runtime': 'python3.7',
                                         'Timeout': 10,
                                         'Environment': env_capture
                                     })

    assert env_capture.as_object() == {
        'Variables': {
            'INGEST_BUCKET': {'Ref': 'IngestBucket2B3522FA'}
        }
    }
