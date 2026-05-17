import json
import boto3
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
REPORTER_TABLE = dynamodb.Table("Incident_Reporter")

def float_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    return obj

def build_reporter_item(data):
    incident_id = data.get("incidentId")

    return float_to_decimal({  # ← ครอบทั้ง item เลย ไม่ใช่แค่ location
        "incident_id":   incident_id,
        "incidentId":    incident_id,
        "description":   data.get("description", ""),
        "incidentType":  data.get("incidentType", ""),
        "addressName":   data.get("addressName", ""),
        "location":      data.get("location", {
                             "addressName": data.get("addressName", "")
                         }),
        "reportChannel": data.get("reportChannel", ""),
        "reportCount":   data.get("reportCount", 1),
        "reporterId":    data.get("reporterId", ""),
        "severity":      data.get("severity", ""),
        "status":        data.get("status", "REPORTED"),
        "updatedAt":     data.get("updatedAt", datetime.utcnow().isoformat()),
    })

def save_reporter(item):
    if not item.get("incident_id"):
        raise ValueError("incident_id is required and cannot be null")
    REPORTER_TABLE.put_item(Item=item)

def lambda_handler(event, context):
    results = []

    for record in event["Records"]:
        try:
            body = json.loads(record["body"])
            msg_type = body.get("Type")

            if msg_type != "Notification":
                print(f"Skipping type: {msg_type}")
                continue

            data = json.loads(body["Message"])

            # ป้องกัน severity เป็น None → DynamoDB ไม่รับ null string
            if data.get("severity") is None:
                data["severity"] = ""

            item = build_reporter_item(data)
            save_reporter(item)

            results.append({
                "incident_id": item["incident_id"],
                "status": "saved"
            })

        except Exception as e:
            print(f"[SKIP] {e} | messageId: {record.get('messageId')}")
            continue

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }