import time
from datetime import datetime, timezone

from app.db.mongo import get_db


# Funzioni condivise dalle sei query: conversione date, misura dei tempi,
# risoluzione dell'intestatario di un conto.


def date_to_ms(s):
    # "YYYY-MM-DD" (dal form) -> epoch ms UTC (come sono salvati nei DB).
    # UTC esplicito: altrimenti Python userebbe il fuso del Mac e i confini
    # della finestra slitterebbero di un'ora o due.
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_iso(ms):
    # epoch ms -> stringa ISO per la tabella; None resta None
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def timed(fn):
    # decoratore: misura i ms dell'intera run (per le cross-DB: entrambi i
    # motori + il Python in mezzo, escluso il rendering HTML) e incapsula il
    # risultato nella forma {rows, count, elapsed_ms} che il template si aspetta
    def wrap(params):
        t0 = time.perf_counter()
        rows = fn(params)
        return {"rows": rows, "count": len(rows), "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2)}
    return wrap


def owners_of(account_ids):
    """Intestatario di ciascun conto, letto da MongoDB.

    In FinBench un conto appartiene o a una Person o a una Company, tramite due
    collezioni-link distinte (personOwnAccount / companyOwnAccount). Qui le
    interroghiamo entrambe e restituiamo {accountId: profilo}, dove il profilo
    e' il documento anagrafico con in piu' il campo "ownerType" (person|company).
    Usata da Q3 e Q6, che sono le due query cross-database.
    """
    db = get_db()
    out = {}
    for kind, link_coll, key in (("person",  "personOwnAccount",  "personId"),
                                 ("company", "companyOwnAccount", "companyId")):
        links = {x["accountId"]: x[key] for x in db[link_coll].find({"accountId": {"$in": account_ids}})}
        docs  = {d["_id"]: d for d in db[kind].find({"_id": {"$in": list(links.values())}})}
        for acc, oid in links.items():
            if oid in docs:
                out[acc] = {"ownerType": kind, **docs[oid]}
    return out
