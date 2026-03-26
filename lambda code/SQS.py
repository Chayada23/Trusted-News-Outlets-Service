import json
import boto3
# รับ API → ส่ง SQS
sqs = boto3.client("sqs")

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/xxx/news-queue"

def lambda_handler(event, context):

    body = json.loads(event['body'])

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(body)
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "sent to queue"})
    }