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

def remove_none(obj):
    if isinstance(obj, dict):
        return {k: remove_none(v) for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        return [remove_none(i) for i in obj if i is not None]
    return obj

def build_reporter_item(data):
    incident_id = data.get("incidentId") or data.get("incident_id")

    item = {
        "incident_id":   incident_id,
        "incidentId":    incident_id,
        "description":   data.get("description", ""),
        "incidentType":  data.get("incidentType") or data.get("incident_type", ""),
        "addressName":   data.get("addressName") or data.get("address_name", ""),
        "location":      data.get("location", {
                             "addressName": data.get("addressName") or data.get("address_name", "")
                         }),
        "reportChannel": data.get("reportChannel") or data.get("report_channel", ""),
        "reportCount":   data.get("reportCount") or data.get("report_count", 1),
        "reporterId":    data.get("reporterId") or data.get("reporter_id", ""),
        "severity":      data.get("severity"),
        "status":        data.get("status", "REPORTED"),
        "updatedAt":     data.get("updatedAt") or data.get("updated_at",
                             datetime.utcnow().isoformat()),
    }

    return float_to_decimal(remove_none(item))

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