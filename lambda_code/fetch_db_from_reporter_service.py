import json
import boto3
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
REPORTER_TABLE = dynamodb.Table("Incident_Reporter")

# =========================================================
# BUILD REPORTER ITEM (ตาม schema ของคุณ)
# =========================================================

def build_reporter_item(data):

    return {
        "incident_id": data.get("incident_id"),

        "incidentId": data.get("incidentId", ""),

        "description": data.get("description", ""),

        "incidentType": data.get("incidentType", ""),

        "addressName": data.get("addressName", ""),

        "location": data.get("location", {
            "addressName": data.get("addressName", "")
        }),

        "reportChannel": data.get("reportChannel", ""),

        "reportCount": data.get("reportCount", 1),

        "reporterId": data.get("reporterId", ""),

        "severity": data.get("severity", ""),

        "status": data.get("status", "REPORTED"),

        "updatedAt": data.get(
            "updatedAt",
            datetime.utcnow().isoformat()
        )
    }

# =========================================================
# SAVE TO DYNAMODB
# =========================================================

def save_reporter(item):

    REPORTER_TABLE.put_item(Item=item)

# =========================================================
# LAMBDA (SQS TRIGGER ONLY)
# =========================================================

def lambda_handler(event, context):

    results = []

    # SQS ส่งมาเป็น batch
    for record in event["Records"]:

        # 1. รับข่าวจาก SQS
        body = json.loads(record["body"])

        # 2. แปลงตาม schema
        item = build_reporter_item(body)

        # 3. บันทึกลง DB
        save_reporter(item)

        results.append({
            "incident_id": item["incident_id"],
            "status": "saved"
        })

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }