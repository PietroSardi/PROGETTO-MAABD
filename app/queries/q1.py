# Q1 — Neo4j. Transfer uscenti da un conto fra due date.
from app.db.neo4j import get_driver
from app.queries._common import date_to_ms, ms_to_iso, timed


CYPHER = """
MATCH (a:Account {id:$accountId})-[t:TRANSFER]->(b:Account)
WHERE t.timestamp >= $startMs AND t.timestamp < $endMs
RETURN b.id AS dstAccount, t.amount AS amount, t.timestamp AS ts
ORDER BY t.timestamp
LIMIT 100
"""


@timed
def run(params):
    acc = int(params["accountId"])
    start_ms = date_to_ms(params["startDate"])
    end_ms   = date_to_ms(params["endDate"])
    with get_driver().session() as s:
        res = s.run(CYPHER, accountId=acc, startMs=start_ms, endMs=end_ms)
        return [
            {"dstAccount": r["dstAccount"],
             "amount":     r["amount"],
             "timestamp":  ms_to_iso(r["ts"])}
            for r in res
        ]
