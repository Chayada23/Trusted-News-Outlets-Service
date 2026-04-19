import os
from unittest import result
from urllib import response
import uuid
from datetime import datetime, timezone
import boto3
import json
import requests
#command to download required packages
#python -m pip install requests

API_KEY = os.getenv("GROQ_API_KEY")

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

table_a = dynamodb.Table("Incident_News")
table_b = dynamodb.Table("Incident_News2")
summary_table = dynamodb.Table("Incident_Summary")

API_URL = "https://api.groq.com/openai/v1/chat/completions"

def get_next_version(incident_id):

    response = summary_table.query(
        KeyConditionExpression="incident_id = :id",
        ExpressionAttributeValues={
            ":id": incident_id
        }
    )

    items = response.get("Items", [])

    if not items:
        return 1

    return max(i.get("version", 0) for i in items) + 1

def save_summary_to_db(item, summary_text, verification_status="pass"):

    trace_id = "trace_" + str(uuid.uuid4())[:8]

    version = get_next_version(item["incident_id"])

    data = {
        "incident_id": item["incident_id"],
        "incident_type": item["incident_type"],
        "severity": item["severity"],
        "status": item["status"],
        "location": item["location"],
        "address_name": item["address_name"],
        "incident_start": item["incident_start"],
        "ended_time": item["ended_time"],
        "reporter_id": item["reporter_id"],

        "created_at": item["created_at"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "version": version,
        "trace_id": trace_id,
        "verification": verification_status,

        "description": summary_text
    }
    print("saving to DynamoDB:", data)
    summary_table.put_item(Item=data)
    print("DynamoDB response:", response)
    return trace_id

def process_and_summarize(items_a, items_b):

    results = []

    for a in items_a:
        for b in items_b:

            # if the news items match
            if a["incident_id"] != b["incident_id"]:
                continue
            #if the news don't match
            if a["incident_type"] != b["incident_type"]:
                continue

            # รวมข่าว
            news_data = [
                a["description"],
                b["description"]
            ]

            combined_text = "\n\n---\n\n".join(news_data)

            #  ส่งเข้า LLM
            payload = {
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {
                        "role": "user",
                        "content": f"""
คุณคือ AI นักวิเคราะห์ข่าว

มีข่าว 2 ชิ้นด้านล่าง:
{combined_text}

งานของคุณ:
1. สรุปข่าวแต่ละข่าวสั้น ๆ
2. เปรียบเทียบว่าเหมือนกันตรงไหน
3. ต่างกันตรงไหน
4. สรุปภาพรวมของเหตุการณ์
"""
                    }
                ]
            }

            response = requests.post(
                API_URL,
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json"
                },
                json=payload
            )

            try:
                result = response.json()
                summary = result["choices"][0]["message"]["content"]

                # SAVE เข้า DynamoDB ตรงนี้ (ถูก scope)
                save_summary_to_db(
                    item=a,
                    summary_text=summary,
                    verification_status="pass"
                )

                # return result
                results.append({
                    "incident_id": a["incident_id"],
                    "type": a["incident_type"],
                    "summary": summary
                })

            except Exception as e:
                print("ERROR:", e)
                print(response.text)

    return results


def lambda_handler(event, context):

    try:
        response_a = table_a.scan()
        response_b = table_b.scan()

        items_a = response_a["Items"]
        items_b = response_b["Items"]

        result = process_and_summarize(items_a, items_b)

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False)
        }

    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "Verification failed",
                "reason": str(e)
            })
        }
    