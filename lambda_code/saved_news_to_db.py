import json
import boto3
import uuid
from datetime import datetime
from decimal import Decimal

# =========================
# DynamoDB
# =========================
dynamodb = boto3.resource("dynamodb")

REPORTER_TABLE = dynamodb.Table("Incident_Reporter")
SCRAPING_TABLE = dynamodb.Table("Disaster_Scraping")


# =========================
# ROUTING CONFIG
# =========================
REPORTER_SOURCE = "incidentreporterservice"

def convert_decimal(obj):

    if isinstance(obj, float):
        return Decimal(str(obj))

    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [convert_decimal(v) for v in obj]

    else:
        return obj

# =========================
# normalize event
# =========================
def normalize_event(body):

    # รองรับทั้ง string / dict
    if isinstance(body, dict):
        data = body
    else:
        data = json.loads(body)

    raw_source = data.get("source")

    # รองรับ SNS attribute / string / missing
    if isinstance(raw_source, dict):
        source = raw_source.get("Value")

    elif isinstance(raw_source, str):
        source = raw_source

    else:
        source = "disasterscrapingservice"

    return {
        "source": source,
        "incident_id": data.get("incident_id") or str(uuid.uuid4()),
        "incident_type": data.get("incident_type"),
        "description": data.get("description"),
        "message": data.get("message"),
        "status": data.get("status"),
        "location": data.get("location"),
        "affectedArea": data.get("affectedArea"),
        "severity": data.get("severity"),
        "reporter_id": data.get("reporter_id"),
        "updated_at": datetime.utcnow().isoformat()
    }


# =========================
# extract records safely
# =========================
def extract_records(event):

    if "Records" in event:
        return event["Records"]

    return [event]


# =========================
# routing resolver
# =========================
def resolve_route(source):

    source = (source or "").lower().strip()

    if source == REPORTER_SOURCE:
        return "reporter"

    return "scraping"


# =========================
# main processing
# =========================
def route_and_write(records):

    for record in records:

        if "body" in record:
            body = record["body"]
        else:
            body = record

        item = normalize_event(body)

        route = resolve_route(item["source"])

        # =========================
        # REPORTER TABLE
        # =========================
        if route == "reporter":

            item_db = convert_decimal({
                "incident_id": item["incident_id"],
                "incident_type": item["incident_type"],
                "description": item["description"],
                "updated_at": item["updated_at"],
                "status": item["status"],
                "location": item["location"],
                "severity": item["severity"],
                "reporter_id": item["reporter_id"]
            })

            REPORTER_TABLE.put_item(Item=item_db)

        # =========================
        # SCRAPING TABLE
        # =========================
        else:

            item_db = convert_decimal({
                "incident_id": item["incident_id"],
                "incident_type": item["incident_type"],
                "message": item["message"],
                "updated_at": item["updated_at"],
                "affectedArea": item["affectedArea"],
                "severity": item["severity"]
            })

            SCRAPING_TABLE.put_item(Item=item_db)

    return {
        "statusCode": 200,
        "body": json.dumps("OK")
    }
def lambda_handler(event, context):

    records = extract_records(event)

    return route_and_write(records)