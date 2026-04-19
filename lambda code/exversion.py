import json
import pymysql
import os
import socket

print("network test")

s = socket.socket()
s.settimeout(3)

# try:
#     s.connect((os.environ['DB_HOST'],3306))
#     print("NETWORK OK")
# except Exception as e:
#     print("NETWORK ERROR:", e)


# def get_connection():
#     return pymysql.connect(
#         host=os.environ['DB_HOST'],
#         user=os.environ['DB_USER'],
#         password=os.environ['DB_PASSWORD'],
#         database=os.environ['DB_NAME'],
#         port=3306,
#         connect_timeout=5
#     )


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

    for category, words in keywords.items():
        if any(word in description for word in words):
            score += 1

    if score >= 4:
        level = "HIGH"
    elif score >= 2:
        level = "MEDIUM"
    else:
        level = "LOW"

    return score, level


def lambda_handler(event, context):

    print("start lambda")

    # ถ้า event มาจาก SQS
    if 'Records' in event:
        print("event from SQS")
        record = event['Records'][0]

        receive_count = int(record['attributes']['ApproximateReceiveCount'])
        print("Receive Count:", receive_count)

        message = record['body']
        data = json.loads(message)

    else:
        # ใช้ตอน Test ใน Lambda console
        print("event from test")
        data = event

    description = data.get("description", "")

    score, level = analyze_description(description)

    status_report = level

    # connection = get_connection()

    print("connected db")

    # try:
    #     with connection.cursor() as cursor:

    #         sql = """
    #         INSERT INTO UpdateReport
    #         (incident_id, incident_type, severity, status, location, address_name,
    #         incident_start, ended_time, description, reporter_id, status_report,
    #         created_at, updated_at)
    #         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
    #         """

    #         cursor.execute(sql, (
    #             data.get("incident_id"),
    #             data.get("incident_type"),
    #             data.get("severity"),
    #             data.get("status"),
    #             data.get("location"),
    #             data.get("address_name"),
    #             data.get("incident_start"),
    #             data.get("ended_time"),
    #             description,
    #             data.get("reporter_id"),
    #             status_report
    #         ))

    #         connection.commit()

    # finally:
    #     connection.close()

    # return {
    #     "statusCode": 200,
    #     "body": json.dumps({
    #         "message": "Incident saved",
    #         "credibility_score": score,
    #         "credibility_level": level,
    #         "status_report": status_report
    #     })
    # }