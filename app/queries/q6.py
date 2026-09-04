# Q6 — cross-DB. Hub detection su Neo4j + profilo intestatario da MongoDB.
#
# Un "hub" e' un conto con degree (transfer in entrata + uscita) alto dentro la
# finestra temporale. Due criteri, scelti dall'utente:
#   - soglia assoluta (minDegree): tiene i conti con degree >= soglia. E' il
#     criterio operativo antifrode ("segnala chi supera N movimenti").
#   - top x% (topPct, opzionale): tiene la frazione piu' attiva della
#     distribuzione. Analisi relativa, utile per confronto.
# In entrambi i casi Neo4j restituisce al massimo 20 hub, piu' quanti hub totali
# soddisfano il criterio (nHubs) e il degree del piu' "piccolo" fra loro
# (cutoffDegree), cosi' i due criteri sono confrontabili a colpo d'occhio.
# MongoDB arricchisce poi ogni hub con tipo conto, data apertura e profilo
# dell'intestatario (persona o azienda).
from app.db.mongo import get_db
from app.db.neo4j import get_driver
from app.queries._common import date_to_ms, ms_to_iso, owners_of, timed


# Soglia assoluta: il filtro WHERE scarta subito quasi tutti i conti, quindi la
# collect() finale raccoglie solo gli hub (poche decine/centinaia di elementi).
CYPHER_THRESHOLD = """
MATCH (a:Account)-[t:TRANSFER]-()
WHERE t.timestamp >= $startMs AND t.timestamp < $endMs
WITH a, count(t) AS degree, sum(t.amount) AS totalAmount
WHERE degree >= $minDegree
ORDER BY degree DESC
WITH collect({accountId: a.id, degree: degree, totalAmount: totalAmount}) AS hubs
UNWIND hubs[0..20] AS h
RETURN h.accountId AS accountId, h.degree AS degree, h.totalAmount AS totalAmount,
       size(hubs) AS nHubs, hubs[-1].degree AS cutoffDegree
"""

# Top x%: per sapere dove cade il taglio serve l'intera classifica dei conti
# attivi (~200k elementi, stanno in memoria), quindi e' un po' piu' lenta.
CYPHER_TOP_PCT = """
MATCH (a:Account)-[t:TRANSFER]-()
WHERE t.timestamp >= $startMs AND t.timestamp < $endMs
WITH a, count(t) AS degree, sum(t.amount) AS totalAmount
ORDER BY degree DESC
WITH collect({accountId: a.id, degree: degree, totalAmount: totalAmount}) AS ranked
WITH ranked[0..toInteger(ceil(size(ranked) * $topPct / 100.0))] AS hubs
UNWIND hubs[0..20] AS h
RETURN h.accountId AS accountId, h.degree AS degree, h.totalAmount AS totalAmount,
       size(hubs) AS nHubs, hubs[-1].degree AS cutoffDegree
"""


@timed
def run(params):
    start_ms   = date_to_ms(params["startDate"])
    end_ms     = date_to_ms(params["endDate"])
    min_degree = int(params["minDegree"])
    top_pct    = float(params["topPct"]) if params.get("topPct", "").strip() else None
    if top_pct is not None and not 0 < top_pct <= 100:
        raise ValueError("topPct deve essere fra 0 e 100")

    with get_driver().session() as s:
        if top_pct is None:
            hubs = s.run(CYPHER_THRESHOLD, startMs=start_ms, endMs=end_ms, minDegree=min_degree).data()
        else:
            hubs = s.run(CYPHER_TOP_PCT, startMs=start_ms, endMs=end_ms, topPct=top_pct).data()

    if not hubs:
        return []

    # arricchimento da Mongo: metadati conto + intestatario (persona o azienda)
    ids    = [h["accountId"] for h in hubs]
    meta   = {d["_id"]: d for d in get_db().account.find({"_id": {"$in": ids}})}
    owners = owners_of(ids)

    out = []
    for h in hubs:
        m = meta.get(h["accountId"], {})
        o = owners.get(h["accountId"], {})
        out.append({
            "accountId":    h["accountId"],
            "degree":       h["degree"],
            "totalAmount":  h["totalAmount"],
            "accountType":  m.get("type"),
            "openedAt":     ms_to_iso(m.get("creationDate")),
            "ownerType":    o.get("ownerType"),
            "ownerId":      o.get("_id"),
            "ownerName":    o.get("name"),
            "ownerCountry": o.get("country"),
            "ownerBlocked": o.get("isBlocked"),
            "nHubs":        h["nHubs"],
            "cutoffDegree": h["cutoffDegree"],
        })
    return out
