import json
import boto3
import math
import re
from decimal import Decimal
from datetime import datetime

# =========================================================
# AWS
# =========================================================

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

REPORTER_TABLE = dynamodb.Table("Incident_Reporter")
SCRAPING_TABLE = dynamodb.Table("Disaster_Scraping")
SUMMARY_TABLE = dynamodb.Table("Summary_In")

SNS_VERIFIED = "arn:aws:sns:us-east-1:767398101278:incident-prioritization-topic"
SNS_REJECTED = "arn:aws:sns:us-east-1:767398101278:status-changed-rejected-topic"

THRESHOLD = 0.15
OPERATOR_ID = "Trusted_News_Outlets_Service"


# =========================================================
# VECTOR
# =========================================================

def simple_vector(text):
    words = re.findall(r'[\w\u0E00-\u0E7F]+', text.lower())
    vector = {}
    for w in words:
        vector[w] = vector.get(w, 0) + 1
    return vector


def cosine(v1, v2):
    common = set(v1.keys()) | set(v2.keys())

    dot = sum(v1.get(k, 0) * v2.get(k, 0) for k in common)

    mag1 = math.sqrt(sum(v * v for v in v1.values()))
    mag2 = math.sqrt(sum(v * v for v in v2.values()))

    if mag1 == 0 or mag2 == 0:
        return 0

    return dot / (mag1 * mag2)


# =========================================================
# FETCH REPORTERS
# =========================================================

def fetch_reporters():
    items = []
    response = REPORTER_TABLE.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = REPORTER_TABLE.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        items.extend(response.get("Items", []))

    return items


# =========================================================
# TEXT MAPPING
# =========================================================

def get_reporter_text(item):
    return " ".join([
        item.get("description", ""),
        item.get("incidentType", ""),
        item.get("addressName", "")
    ])


def get_scraping_text(item):
    location_text = " ".join(item.get("location_id", []))

    return " ".join([
        item.get("description", ""),
        item.get("incident_type", ""),
        location_text
    ])


# =========================================================
# SCRAPING PARSER
# =========================================================

def parse_scraping_item(data):
    return {
        "incident_id": data.get("incident_id", ""),
        "created_at": data.get("created_at", ""),
        "description": data.get("description", ""),
        "incident_start": data.get("incident_start", ""),
        "incident_type": data.get("incident_type", ""),
        "location_id": data.get("location_id", []),
        "severity": data.get("severity", ""),
        "status": data.get("status", ""),
        "reporter_id": data.get("reporter_id", "")
    }


# =========================================================
# MATCHING
# =========================================================

def match(scraping_item, reporter_items):

    scraping_vector = simple_vector(get_scraping_text(scraping_item))

    best_score = -1
    best_match = None

    for reporter in reporter_items:

        reporter_vector = simple_vector(get_reporter_text(reporter))

        score = cosine(scraping_vector, reporter_vector)

        if score > best_score:
            best_score = score
            best_match = reporter

    return {
        "incident_id": scraping_item["incident_id"],
        "score": best_score,
        "reporter": best_match
    }


# =========================================================
# SAVE SUMMARY
# =========================================================

def save_result(result, status):

    SUMMARY_TABLE.put_item(
        Item={
            "incident_id": result["incident_id"],
            "match_id": result["match_id"],
            "score": Decimal(str(result["score"])),
            "status": status,
            "matched_text": result["matched_text"],
            "created_at": datetime.utcnow().isoformat(),
            "operatorId": OPERATOR_ID
        }
    )


# =========================================================
# DELETE SCRAPING
# =========================================================

def delete_scraping_news(incident_id):
    SCRAPING_TABLE.delete_item(
        Key={"incident_id": incident_id}
    )


# =========================================================
# ROUTER (FINAL FIX)
# =========================================================

def route(result):

    status = "verified" if result["score"] >= THRESHOLD else "rejected"
    now = datetime.utcnow().isoformat()

    # =====================================================
    # VERIFIED → ส่ง reporter record เป็น payload หลัก
    # =====================================================
    if status == "verified":

        payload = result["reporter"]

        payload["status"] = "verified"
        payload["updated_at"] = now
        #payload["score"] = result["score"]
        payload["operatorId"] = OPERATOR_ID

        topic = SNS_VERIFIED

    # =====================================================
    # REJECTED → ส่งแค่ minimal info ตามที่คุณต้องการ
    # =====================================================
    else:

        payload = {
            "operatorId": OPERATOR_ID,
            "status": "rejected",
            "updated_at": now
        }

        topic = SNS_REJECTED

    sns.publish(
        TopicArn=topic,
        Message=json.dumps(payload, default=str)
    )

    save_result(
        {
            "incident_id": result["incident_id"],
            "match_id": result["reporter"]["incident_id"] if result["reporter"] else "NO_MATCH",
            "score": result["score"],
            "matched_text": get_reporter_text(result["reporter"]) if result["reporter"] else ""
        },
        status
    )

    delete_scraping_news(result["incident_id"])

    return payload


# =========================================================
# HANDLER
# =========================================================

def lambda_handler(event, context):

    reporter_items = fetch_reporters()

    results = []

    for record in event["Records"]:

        scraping_raw = json.loads(record["body"])
        scraping_item = parse_scraping_item(scraping_raw)

        matched = match(scraping_item, reporter_items)

        routed = route(matched)

        results.append(routed)

    return {
        "statusCode": 200,
        "body": json.dumps(results, default=str)
    }