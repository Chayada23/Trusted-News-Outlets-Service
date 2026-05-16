import json
import boto3
import math
import re
from decimal import Decimal
from datetime import datetime

# =========================================================
# AWS CLIENTS
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
    vec = {}
    for w in words:
        vec[w] = vec.get(w, 0) + 1
    return vec


def cosine(v1, v2):
    common = set(v1.keys()) & set(v2.keys())

    dot = sum(v1.get(k, 0) * v2.get(k, 0) for k in common)

    mag1 = math.sqrt(sum(v * v for v in v1.values()))
    mag2 = math.sqrt(sum(v * v for v in v2.values()))

    if mag1 == 0 or mag2 == 0:
        return 0

    return dot / (mag1 * mag2)

# =========================================================
# FETCH REPORTERS (REFERENCE DATA)
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
# PARSE SQS SCRAPING EVENT
# =========================================================

def parse_scraping_item(data):
    return {
        "incident_id": data.get("incident_id", ""),
        "description": data.get("description", ""),
        "incident_type": data.get("incident_type", ""),
        "location_id": data.get("location_id", []),
        "severity": data.get("severity", ""),
        "status": data.get("status", "")
    }

# =========================================================
# MATCHING
# =========================================================

def match(scraping_item, reporter_items):

    s_vec = simple_vector(get_scraping_text(scraping_item))

    best_score = 0
    best_match = None

    for r in reporter_items:

        r_vec = simple_vector(get_reporter_text(r))
        score = cosine(s_vec, r_vec)

        if score > best_score:
            best_score = score
            best_match = r

    return {
        "incident_id": scraping_item["incident_id"],
        "score": best_score,
        "reporter": best_match
    }

# =========================================================
# DELETE FUNCTIONS (IMPORTANT)
# =========================================================

def delete_scraping(incident_id):
    SCRAPING_TABLE.delete_item(
        Key={"incident_id": incident_id}
    )


def delete_reporter(incident_id):
    REPORTER_TABLE.delete_item(
        Key={"incident_id": incident_id}
    )

# =========================================================
# SAVE SUMMARY
# =========================================================

def save_result(result, status, reporter):

    SUMMARY_TABLE.put_item(
        Item={
            "incident_id": result["incident_id"],
            "match_id": reporter["incident_id"] if reporter else "NO_MATCH",
            "score": Decimal(str(result["score"])),
            "status": status,
            "matched_text": get_reporter_text(reporter) if reporter else "",
            "created_at": datetime.utcnow().isoformat(),
            "operatorId": OPERATOR_ID
        }
    )

# =========================================================
# ROUTE SNS + DELETE BOTH
# =========================================================

def route(result):
    status = "verified" if result["score"] >= THRESHOLD else "rejected"
    now = datetime.utcnow().isoformat()
    reporter = result["reporter"]

    # ---------------- VERIFIED ----------------
    if status == "verified":
        if not reporter:
            
            status = "rejected"
            payload = {
                "incident_id": result["incident_id"],
                "status": "rejected",
                "updated_at": now,
                "operatorId": OPERATOR_ID
            }
            topic = SNS_REJECTED
        else:
            payload = dict(reporter)  # copy ไม่ mutate
            payload["status"] = "verified"
            payload["updated_at"] = now
            payload["operatorId"] = OPERATOR_ID
            topic = SNS_VERIFIED

    # ---------------- REJECTED ----------------
    else:
        payload = {
            "incident_id": result["incident_id"],
            "status": "rejected",
            "updated_at": now,
            "operatorId": OPERATOR_ID
        }
        topic = SNS_REJECTED

    sns.publish(
        TopicArn=topic,
        Message=json.dumps(payload, default=str)
    )

    save_result(result, status, reporter)

    delete_scraping(result["incident_id"])

    if status == "verified" and reporter:
        delete_reporter(reporter["incident_id"])

    return payload

# =========================================================
# LAMBDA HANDLER (SQS ONLY)
# =========================================================

def lambda_handler(event, context):

    reporter_items = fetch_reporters()

    responses = []

    for record in event["Records"]:

        scraping_raw = json.loads(record["body"])
        scraping_item = parse_scraping_item(scraping_raw)

        result = match(scraping_item, reporter_items)

        responses.append(route(result))
         # delete reporter are only if matched
        if result["reporter"]:
            matched_id = result["reporter"]["incident_id"]
            reporter_items = [
                r for r in reporter_items
                if r["incident_id"] != matched_id
            ]

    return {
        "statusCode": 200,
        "body": json.dumps(responses, default=str)
    }