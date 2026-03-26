import json
import boto3
import uuid
from datetime import datetime

# Bedrock
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

# DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("UpdateReport")

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
# BEDROCK AI
# ---------------------------
def analyze_with_ai(description):

    prompt = f"""
    วิเคราะห์ข้อความนี้:
    {description}

    ให้ตอบเป็น JSON:
    {{
      "credibility": "LOW/MEDIUM/HIGH"
    }}
    """

    response = bedrock.invoke_model(
        modelId="amazon.nova-lite-v1:0",
        body=json.dumps({"inputText": prompt}),
        contentType="application/json",
        accept="application/json"
    )

    result = json.loads(response['body'].read())
    return result

# ---------------------------
# MAIN LAMBDA (SQS WORKER)
# ---------------------------
def lambda_handler(event, context):

    print("start SQS worker")

    results = []

    # 🔥 ต้อง loop ทุก record (สำคัญมาก)
    for record in event['Records']:

        try:
            data = json.loads(record['body'])
            description = data.get("description", "")

            # ❗ ถ้าไม่มี description → fail เพื่อ retry
            if not description:
                raise Exception("No description")

            # วิเคราะห์
            score, level = analyze_description(description)
            ai_result = analyze_with_ai(description)

            ai_level = ai_result.get("credibility", "LOW")

            # 🔥 เงื่อนไขใหม่ (สำคัญ)
            if level in ["MEDIUM", "HIGH"] or ai_level in ["MEDIUM", "HIGH"]:

            # สร้าง item
                item = {
                    "id": str(uuid.uuid4()),
                    "incident_id": data.get("incident_id"),
                    "description": description,
                    "rule_score": score,
                    "rule_level": level,
                    "ai_credibility": ai_result.get("credibility"),
                    "created_at": datetime.utcnow().isoformat()
                }

                # บันทึก
                table.put_item(Item=item)

                results.append(item)

            else:
                print("SKIP (LOW):", description)
            
        except Exception as e:
            print("ERROR:", str(e))

            # ❗ สำคัญ: raise เพื่อให้ SQS retry
            raise e

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }