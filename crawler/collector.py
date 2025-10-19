import os, time, requests, json, mariadb

DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]
CRAWL4AI_URL = "http://crawl4ai:11235/v1/crawl"

def store_data(entries):
    conn = mariadb.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS findings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url TEXT,
            snippet TEXT,
            title TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for e in entries:
        cur.execute(
            "INSERT INTO findings (url, snippet, title) VALUES (?, ?, ?)",
            (e.get("url"), e.get("snippet"), e.get("title")),
        )
    conn.commit()
    conn.close()

def run_crawl(query="chemical element market price"):
    payload = {
        "query": query,
        "max_results": 15,
        "mode": "focused"
    }
    response = requests.post(CRAWL4AI_URL, json=payload)
    response.raise_for_status()
    data = response.json()
    results = [
        {
            "url": r.get("url"),
            "title": r.get("meta", {}).get("title", "Unknown"),
            "snippet": r.get("text", "")[:200],
        }
        for r in data.get("results", [])
    ]
    store_data(results)

if __name__ == "__main__":
    while True:
        print("Running POET collector cycle...")
        try:
            run_crawl()
        except Exception as e:
            print("Error during crawl:", e)
        time.sleep(7200)  # alle 2 Stunden
