import sqlite3
topics = [
    ("why your salary will never make you rich", "money", 80, "manual"),
    ("the debt trap nobody talks about", "money", 80, "manual"),
    ("why saving money keeps you poor", "money", 80, "manual"),
    ("why your boss will always earn more than you", "money", 80, "manual"),
    ("the biggest lie about becoming successful", "success", 80, "manual"),
    ("why talented people stay broke", "success", 80, "manual"),
    ("why loyal employees get paid the least", "career", 80, "manual"),
    ("the salary negotiation secret nobody teaches", "career", 80, "manual"),
]
conn = sqlite3.connect("data/processed/channel_forge.db")
c = conn.cursor()
c.executemany("INSERT OR IGNORE INTO scored_topics (keyword, category, score, source) VALUES (?,?,?,?)", topics)
conn.commit()
print("Topics inserted:", c.rowcount)
c.execute("SELECT COUNT(*) FROM scored_topics")
print("Total topics now:", c.fetchone()[0])
conn.close()
