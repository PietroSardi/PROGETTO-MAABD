# Q3 — cross-DB. Scheda conto + 10 conti vicini per traffico transfer.
#
# Flusso (tre passi, due motori):
#   1. MongoDB : metadati del conto e profilo dell'intestatario (persona o azienda)
#   2. Neo4j   : i 10 conti con cui ha scambiato piu' TRANSFER (in entrambe le direzioni)
#   3. MongoDB : metadati dei 10 vicini, in una sola find con $in
# Ogni motore fa quello in cui e' piu' comodo: il documento per le anagrafiche,
# il grafo per il vicinato.
from app.db.mongo import get_db
from app.db.neo4j import get_driver
from app.queries._common import ms_to_iso, owners_of, timed


CYPHER_NEIGHBORS = """
MATCH (a:Account {id:$accountId})-[t:TRANSFER]-(b:Account)
RETURN b.id AS neighbor, count(t) AS transfers, sum(t.amount) AS totalAmount
ORDER BY transfers DESC
LIMIT 10
"""


@timed
def run(params):
    acc = int(params["accountId"])
    db = get_db()

    account = db.account.find_one({"_id": acc})
    if not account:
        return []
    account["creationDate"] = ms_to_iso(account.get("creationDate"))
    owner = owners_of([acc]).get(acc)

    with get_driver().session() as s:
        rows = s.run(CYPHER_NEIGHBORS, accountId=acc).data()

    # metadati dei vicini in un colpo solo (se non ci sono vicini ids e' vuoto)
    ids  = [r["neighbor"] for r in rows]
    meta = {d["_id"]: d for d in db.account.find({"_id": {"$in": ids}})}

    neighbors = []
    for r in rows:
        m = meta.get(r["neighbor"], {})
        neighbors.append({
            "accountId":   r["neighbor"],
            "type":        m.get("type"),
            "isBlocked":   m.get("isBlocked"),
            "transfers":   r["transfers"],
            "totalAmount": r["totalAmount"],
        })

    return [{"account": account, "owner": owner, "neighbors": neighbors}]
