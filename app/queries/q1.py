# Q1 — Neo4j. Transfer uscenti da un conto fra due date.
#
# Lookup "locale": si parte da un nodo noto (Account per id, trovato via
# constraint/indice) e si seguono solo i suoi archi uscenti. Il costo dipende
# dal grado del conto, non dalla dimensione del grafo: e' il caso ideale per
# un database a grafo.
from app.db.neo4j import get_driver
from app.queries._common import date_to_ms, ms_to_iso, timed


# - pattern direzionato (->): solo i transfer in USCITA
# - intervallo semi-aperto [start, end): la data di fine e' esclusa, cosi'
#   "fino al 2023-01-01" vuol dire "tutto il 2022" e due finestre consecutive
#   non si sovrappongono
# - LIMIT 100 per non far esplodere la pagina
CYPHER = """
MATCH (a:Account {id:$accountId})-[t:TRANSFER]->(b:Account)
WHERE t.timestamp >= $startMs AND t.timestamp < $endMs
RETURN b.id AS dstAccount, t.amount AS amount, t.timestamp AS ts
ORDER BY t.timestamp
LIMIT 100
"""


@timed
def run(params):
    # i parametri arrivano dal form come stringhe: converto subito.
    # se la conversione fallisce parte un ValueError che app.py mostra come
    # "parametro non valido"
    acc = int(params["accountId"])
    start_ms = date_to_ms(params["startDate"])
    end_ms   = date_to_ms(params["endDate"])
    with get_driver().session() as s:
        # query parametrica ($accountId ecc.): niente concatenazione di stringhe,
        # quindi niente injection e Neo4j riusa il piano compilato
        res = s.run(CYPHER, accountId=acc, startMs=start_ms, endMs=end_ms)
        return [
            {"dstAccount": r["dstAccount"],
             "amount":     r["amount"],
             "timestamp":  ms_to_iso(r["ts"])}   # ms -> ISO leggibile
            for r in res
        ]
