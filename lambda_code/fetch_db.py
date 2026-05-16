import json
import boto3
from datetime import datetime
from decimal import Decimal
import re
import math

# ===== AWS =====
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

REPORTER_TABLE = dynamodb.Table("Incident_Reporter")
SCRAPING_TABLE = dynamodb.Table("Disaster_Scraping")
SUMMARY_TABLE = dynamodb.Table("Summary_In")

# ===== SNS =====
SNS_VERIFIED = "arn:aws:sns:us-east-1:767398101278:incident-prioritization-topic"
SNS_REJECTED = "arn:aws:sns:us-east-1:767398101278:status-changed-rejected-topic"

# ===== CONFIG =====
THRESHOLD = 0.15
OPERATOR_ID = "Trusted_News_Outlets_Service"

# =========================================================
# TEXT VECTOR
# =========================================================

def simple_vector(text):

    words = re.findall(r'[\w\u0E00-\u0E7F]+', text.lower())

    vector = {}

    for w in words:
        vector[w] = vector.get(w, 0) + 1

    return vector


# =========================================================
# COSINE SIMILARITY
# =========================================================

def cosine(v1, v2):

    common = set(v1.keys()) | set(v2.keys())

    dot = sum(v1.get(k, 0) * v2.get(k, 0) for k in common)

    mag1 = math.sqrt(sum(v * v for v in v1.values()))
    mag2 = math.sqrt(sum(v * v for v in v2.values()))

    if mag1 == 0 or mag2 == 0:
        return 0

    return dot / (mag1 * mag2)


# =========================================================
# FETCH ALL
# =========================================================

def fetch_all(table):

    items = []

    response = table.scan()

    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:

        response = table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )

        items.extend(response.get("Items", []))

    return items


# =========================================================
# REPORTER TEXT
# schema:
# description
# incidentType
# addressName
# =========================================================

def get_reporter_text(item):

    description = item.get("description", "")
    incident_type = item.get("incidentType", "")
    address = item.get("addressName", "")

    return f"{description} {incident_type} {address}"


# =========================================================
# SCRAPING TEXT
# schema:
# description
# incident_type
# location_id
# =========================================================

def get_scraping_text(item):

    description = item.get("description", "")
    incident_type = item.get("incident_type", "")

    location_list = item.get("location_id", [])

    if not isinstance(location_list, list):
        location_list = []

    locations = " ".join(location_list)

    return f"{description} {incident_type} {locations}"


# =========================================================
# MATCHING
# =========================================================

def match(reporter_data, scraping_data):

    results = []

    for reporter in reporter_data:

        reporter_vector = simple_vector(
            get_reporter_text(reporter)
        )

        best_score = -1
        best_match = None

        for scraping in scraping_data:

            scraping_vector = simple_vector(
                get_scraping_text(scraping)
            )

            score = cosine(
                reporter_vector,
                scraping_vector
            )

            if score > best_score:

                best_score = score
                best_match = scraping

        results.append({
            "incident_id": reporter.get("incident_id"),
            "match_id": best_match.get("incident_id") if best_match else "NO_MATCH",
            "score": best_score,
            "matched_text": get_scraping_text(best_match) if best_match else ""
        })

    return results


# =========================================================
# SAFE DECIMAL
# =========================================================

def safe_decimal(value):

    if isinstance(value, float):
        return Decimal(str(value))

    return value


# =========================================================
# SAVE RESULT
# =========================================================

def save_result(result, status, matched_text):

    now = datetime.utcnow().isoformat()

    SUMMARY_TABLE.put_item(
        Item={
            "incident_id": result["incident_id"],
            "match_id": result["match_id"],
            "score": safe_decimal(result["score"]),
            "status": status,
            "matched_text": matched_text,
            "created_at": now,
            "operatorId": OPERATOR_ID
        }
    )


# =========================================================
# ROUTE SNS
# =========================================================

def route(result):

    now = datetime.utcnow().isoformat()

    status = (
        "verified"
        if result["score"] >= THRESHOLD
        else "rejected"
    )

    topic = (
        SNS_VERIFIED
        if status == "verified"
        else SNS_REJECTED
    )

    payload = {
        "incident_id": result["incident_id"],
        "status": status,
        "updated_at": now,
        "operatorId": OPERATOR_ID
    }

    sns.publish(
        TopicArn=topic,
        Message=json.dumps(payload)
    )

    save_result(
        result,
        status,
        result.get("matched_text", "")
    )

    return payload


# =========================================================
# LAMBDA HANDLER
# =========================================================

def lambda_handler(event, context):

    reporter_data = fetch_all(REPORTER_TABLE)

    scraping_data = fetch_all(SCRAPING_TABLE)

    matched_results = match(
        reporter_data,
        scraping_data
    )

    routed = []

    for result in matched_results:
        routed.append(route(result))

    return {
        "statusCode": 200,
        "body": json.dumps(routed)
    }