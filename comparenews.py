import os
import requests
from bs4 import BeautifulSoup

API_KEY = os.getenv("GROQ_API_KEY")

if not API_KEY:
    print("GROQ_API_KEY is not set")
    exit()

# 🔗 ข่าว 2 ลิงก์
urls = [
    "https://www.bbc.com/news/articles/c4gvkpj0024o",
    "https://www.nation.com.pk/13-Apr-2026/iran-war-lands-triple-blow-flood-ravaged-sri-lankans"  # เปลี่ยนเป็นข่าวจริง
]

def get_content(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    content = " ".join([p.text for p in soup.find_all("p")])
    title = soup.title.text if soup.title else "No title"
    return title, content

news_data = []
for url in urls:
    title, content = get_content(url)
    news_data.append(f"TITLE: {title}\nCONTENT:\n{content[:2500]}")

combined_text = "\n\n---\n\n".join(news_data)

API_URL = "https://api.groq.com/openai/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

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
4. สรุป “ภาพรวมรวมของทั้งสองข่าว” แบบเข้าใจง่าย
"""
        }
    ]
}

response = requests.post(API_URL, headers=headers, json=payload)

print("STATUS CODE:", response.status_code)
print("RAW RESPONSE:", response.text)

try:
    result = response.json()
except Exception:
    print("Response is not JSON")
    exit()

if "error" in result:
    print("API ERROR:", result["error"]["message"])
    exit()

print("\n===== ANALYSIS RESULT =====\n")
print(result["choices"][0]["message"]["content"])