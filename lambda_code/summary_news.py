import json
import boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Incident_Summary")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super().default(obj)

def lambda_handler(event, context):

    version = event.get("pathParameters", {}).get("version")

    if not version:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "Missing version in path (/summary/{version}/news)"
            })
        }

    response = table.query(
        IndexName="VersionIndex",
        KeyConditionExpression="version = :v",
        ExpressionAttributeValues={
            ":v": version
        }
    )

    items = response.get("Items", [])

    return {
        "statusCode": 200,
        "body": json.dumps(items, cls=DecimalEncoder, ensure_ascii=False)
    }