import json
import boto3
import uuid
from datetime import datetime
import requests   # ✅ เพิ่มตัวนี้

# DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("UpdateReport")

# 🔥 ใส่ URL ของ API (ฝั่ง Bedrock)
API_URL = "https://abc123.execute-api.us-east-1.amazonaws.com/analyze"

# ---------------------------
# RULE-BASED ANALYSIS
# ---------------------------
def analyze_description(description):

    description = description.lower()

    keywords = {
        "impact": ["เสียชีวิต","บาดเจ็บ","เสียหาย","สูญหาย"],
        "area": ["จังหวัด","อำเภอ","พื้นที่","ใกล้เคียง","รอบๆ","บริเวณ","แถวๆ","ใกล้","รอบ","บริเวณใกล้เคียง"],
        "severity": ["รุนแรง","ระดับ","หนัก","วิกฤต","ร้ายแรง","อันตราย","รุนแรงมาก","รุนแรงที่สุด","รุนแรงมากๆ","ไม่มาก","ไม่รุนแรง","ไม่หนัก","ไม่วิกฤต","ไม่ร้ายแรง","ไม่อันตราย","ไม่ได้ส่งผลกระทบมาก","ไม่ได้ส่งผลกระทบรุนแรง","ไม่ส่งผลกระทบ","ไม่ส่งผลกระทบรุนแรง"],
        "response": ["ช่วยเหลือ","กู้ภัย","เจ้าหน้าที่","หน่วยงาน","ตำรวจ","ทหาร","ดับเพลิง","อาสาสมัคร","หน่วยกู้ภัย","เจ้าหน้าที่กู้ภัย","เจ้าหน้าที่ช่วยเหลือ"],
        "advice": ["หลีกเลี่ยง","เตือนภัย","โปรดระวัง","ขอให้ระวัง","ขอให้หลีกเลี่ยง","ขอให้เตือนภัย","ขอให้โปรดระวัง","ขอให้ระวังภัย","ขอให้ระวังอันตราย","ขอให้ระวังความเสี่ยง","ขอให้ระวังความเสียหาย","ขอให้ระวังความสูญเสีย","ขอให้ระวังความรุนแรง","ขอให้ระวังความวิกฤต","ขอให้ระวังความอันตราย","ขอเตือนภัย","ขอให้ระวังภัย"]
    }

    score = 0

    for words in keywords.values():
        if any(word in description for word in words):
            score += 1

    if score >= 4:
        level = "HIGH"
    elif score >= 2:
        level = "MEDIUM"
    else:
        level = "LOW"

    return score, level

# ---------------------------
# 🔥 CALL API (แทน Bedrock)
# ---------------------------
def call_ai_api(description):

    response = requests.post(
        API_URL,
        json={"description": description},
        timeout=5
    )

    return response.json()

# ---------------------------
# MAIN LAMBDA (SQS WORKER)
# ---------------------------
def lambda_handler(event, context):

    print("start SQS worker")

    results = []

    for record in event['Records']:

        try:
            data = json.loads(record['body'])
            description = data.get("description", "")

            if not description:
                raise Exception("No description")

            # RULE
            score, level = analyze_description(description)

            # 🔥 เรียก API แทน Bedrock
            ai_result = call_ai_api(description)
            ai_level = ai_result.get("credibility", "LOW")

            print("RULE:", level, "AI:", ai_level)

            # เงื่อนไข
            if level in ["MEDIUM", "HIGH"] or ai_level in ["MEDIUM", "HIGH"]:

                item = {
                    "id": str(uuid.uuid4()),
                    "incident_id": data.get("incident_id"),
                    "description": description,
                    "rule_score": score,
                    "rule_level": level,
                    "ai_credibility": ai_level,
                    "created_at": datetime.utcnow().isoformat()
                }

                table.put_item(Item=item)
                results.append(item)

            else:
                print("SKIP (LOW):", description)

        except Exception as e:
            print("ERROR:", str(e))
            raise e  # SQS retry → DLQ

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
