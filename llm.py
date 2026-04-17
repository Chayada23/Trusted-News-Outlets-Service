import os
import requests
#หาtagsจากHTML
from bs4 import BeautifulSoup 

# 1. API KEY
API_KEY = os.getenv("GROQ_API_KEY")
# If API_KEY is not set, print error and exit
if not API_KEY:
    print("GROQ_API_KEY is not set")
    exit()

# 2. ดึงข่าว
url = "https://www.bbc.com/news/articles/c4gvkpj0024o"
res = requests.get(url)
#ใช้ BeautifulSoup แกะ HTML
soup = BeautifulSoup(res.text, "html.parser")
#ดึง title + content
title = soup.title.text if soup.title else "No title"
#ดึงทุก <p> (paragraph)
content = " ".join([p.text for p in soup.find_all("p")])

print("TITLE:", title)

# เตรียมเรียก Groq API
API_URL = "https://api.groq.com/openai/v1/chat/completions"
#ตั้ง header
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}
#สร้าง prompt ส่งให้ AI
payload = {
    "model": "llama-3.1-8b-instant",
    "messages": [
        {
            "role": "user",
            "content": f"สรุปข่าวนี้แบบสั้น:\n{content[:3000]}"
        }
    ]
}
#ส่ง request ไป Groq
response = requests.post(API_URL, headers=headers, json=payload)

# DEBUG ตรงนี้สำคัญมาก
print("STATUS CODE:", response.status_code)
print("RAW RESPONSE:", response.text)
# 200 = สำเร็จ
# 400/401 = error เช่น key ผิด
# หรือ model ผิด

# 4. parse JSON แบบปลอดภัย
# พยายามแปลงข้อมูลที่ได้จาก API ให้เป็น JSON
try:
    result = response.json()
except Exception:
    print("Response is not JSON")
    exit()

# 5. check error จาก API
if "error" in result:
    print("API ERROR:", result["error"]["message"])
    exit()
# หลังจากได้ JSON แล้ว
# เช็คว่า API ส่ง error มาหรือเปล่า

# 6. print result แบบปลอดภัย
print("\n===== SUMMARY =====\n")
print(result["choices"][0]["message"]["content"])
#print("API KEY (for debugging):", os.getenv("GROQ_API_KEY"))