# Q4 — Neo4j. Cicli A->B->C->A con importo >= soglia e finestra temporale.
from app.db.neo4j import get_driver
from app.queries._common import ms_to_iso, timed


CYPHER = """
MATCH (a:Account)-[t1:TRANSFER]->(b:Account)-[t2:TRANSFER]->(c:Account)-[t3:TRANSFER]->(a)
WHERE t1.amount >= $minAmount AND t2.amount >= $minAmount AND t3.amount >= $minAmount
  AND t1.timestamp < t2.timestamp AND t2.timestamp < t3.timestamp
  AND t3.timestamp - t1.timestamp <= $windowMs
RETURN a.id AS a, b.id AS b, c.id AS c,
       t1.amount AS amt1, t2.amount AS amt2, t3.amount AS amt3,
       t1.timestamp AS ts1, t3.timestamp AS ts3
LIMIT 50
"""


@timed
def run(params):
    min_amount  = float(params["minAmount"])
    window_days = int(params["windowDays"])
    window_ms   = window_days * 24 * 60 * 60 * 1000

    with get_driver().session() as s:
        res = s.run(CYPHER, minAmount=min_amount, windowMs=window_ms)
        return [
            {
                "a": r["a"], "b": r["b"], "c": r["c"],
                "amt1": r["amt1"], "amt2": r["amt2"], "amt3": r["amt3"],
                "start": ms_to_iso(r["ts1"]),
                "end":   ms_to_iso(r["ts3"]),
            }
            for r in res
        ]
