import aws_cdk as core
import aws_cdk.assertions as assertions

from wttr_in_data.wttr_in_data_stack import WttrInDataStack

# example tests. To run these tests, uncomment this file along with the example
# resource in wttr_in_data/wttr_in_data_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = WttrInDataStack(app, "wttr-in-data")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
