import json, random, traceback
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

from app.db.mongo import get_db
from app.db.neo4j import get_driver
from app.queries import q1, q2, q3, q4, q5, q6


app = Flask(__name__, template_folder="templates", static_folder="static")

DAY_MS = 24 * 60 * 60 * 1000


# config delle 6 query. i default degli id sono valori "veri" (ho controllato
# che abbiano dati nel dataset SF 1) così al primo caricamento si vede qualcosa.
QUERIES = {
    "q1": {
        "short": "Transfer uscenti",
        "name": "Q1 — Transfer uscenti in una finestra temporale",
        "db": "Neo4j",
        "desc": "Dato un conto, mostra fino a 100 transazioni uscenti fra due date.",
        "fields": [
            {"name": "accountId", "label": "Account ID",  "type": "number", "value": "4895977119294693626"},
            {"name": "startDate", "label": "Data inizio", "type": "date",   "value": "2020-01-01"},
            {"name": "endDate",   "label": "Data fine",   "type": "date",   "value": "2023-01-01"},
        ],
        "fn": q1.run,
    },
    "q2": {
        "short": "Profilo persona",
        "name": "Q2 — Profilo completo di una persona",
        "db": "MongoDB",
        "desc": "Anagrafica + conti posseduti + prestiti richiesti, in un solo documento.",
        "fields": [
            {"name": "personId", "label": "Person ID", "type": "number", "value": "13194139557822"},
        ],
        "fn": q2.run,
    },
    "q3": {
        "short": "Scheda conto + vicini",
        "name": "Q3 — Scheda conto con top-10 vicini",
        "db": "Cross-DB",
        "desc": "Metadati conto (Mongo) + 10 conti più connessi per transfer (Neo4j).",
        "fields": [
            {"name": "accountId", "label": "Account ID", "type": "number", "value": "4895977119294693626"},
        ],
        "fn": q3.run,
    },
    "q4": {
        "short": "Cicli A→B→C→A",
        "name": "Q4 — Cicli di transfer a tre passi",
        "db": "Neo4j",
        "desc": "Cicli chiusi A→B→C→A con importi ≥ soglia entro una finestra di giorni.",
        "fields": [
            {"name": "minAmount",  "label": "Importo minimo (€)", "type": "number", "value": "10000", "step": "0.01"},
            {"name": "windowDays", "label": "Finestra (giorni)",  "type": "number", "value": "30"},
        ],
        "fn": q4.run,
    },
    "q5": {
        "short": "Top volume mensile",
        "name": "Q5 — Top-10 account per volume trasferito in un mese",
        "db": "MongoDB",
        "desc": "Aggregazione mensile sui transfer con join sull'anagrafica conto.",
        "fields": [
            {"name": "year",  "label": "Anno",        "type": "number", "value": "2020"},
            {"name": "month", "label": "Mese (1-12)", "type": "number", "value": "6"},
        ],
        "fn": q5.run,
    },
    "q6": {
        "short": "Hub detection",
        "name": "Q6 — Hub detection + profilo intestatario",
        "db": "Cross-DB",
        "desc": "Account con degree ≥ soglia, oppure nel top x% (Neo4j), arricchiti col profilo intestatario (Mongo).",
        "fields": [
            {"name": "startDate", "label": "Data inizio",   "type": "date",   "value": "2020-01-01"},
            {"name": "endDate",   "label": "Data fine",     "type": "date",   "value": "2023-01-01"},
            {"name": "minDegree", "label": "Soglia degree", "type": "number", "value": "500"},
            # variante richiesta dal docente: se valorizzato sostituisce la soglia
            {"name": "topPct", "label": "Top x% (opzionale, sostituisce la soglia)", "type": "number",
             "value": "", "step": "0.01", "min": "0.01", "max": "100"},
        ],
        "fn": q6.run,
    },
}

# campi su cui il bottone "dadino" inline ha senso (ID o date)
RANDOMIZABLE = {"accountId", "personId", "startDate", "endDate"}


# cache dei sample per /api/random. la popolo al primo uso perché
# fare 2 query al DB all'avvio di Flask rallenta l'hot-reload in debug.
_samples = None


def _fmt_date(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def get_samples():
    global _samples
    if _samples is not None:
        return _samples

    accounts = []
    tmin = tmax = None
    with get_driver().session() as s:
        # conti con almeno 10 transfer uscenti (altrimenti Q1 torna vuota)
        rows = s.run("""
            MATCH (a:Account)-[t:TRANSFER]->()
            WITH a.id AS id, count(t) AS n
            WHERE n >= 10
            RETURN id ORDER BY n DESC LIMIT 500
        """).data()
        accounts = [r["id"] for r in rows]
        r = s.run("MATCH ()-[t:TRANSFER]->() "
                  "RETURN min(t.timestamp) AS mn, max(t.timestamp) AS mx").single()
        tmin, tmax = r["mn"], r["mx"]

    persons = [d["_id"] for d in get_db().personOwnAccount.aggregate([
        {"$group": {"_id": "$personId", "n": {"$sum": 1}}},
        {"$sample": {"size": 500}},
    ])]

    _samples = {"accounts": accounts, "persons": persons, "tmin": tmin, "tmax": tmax}
    return _samples


@app.route("/")
def index():
    return render_template("index.html", queries=QUERIES, randomizable=RANDOMIZABLE)


@app.route("/api/random/<qid>")
def api_random(qid):
    # pesca valori coerenti col dataset: l'idea è che dopo il random la query non
    # debba tornare zero righe, altrimenti l'utente pensa che sia rotto.
    if qid not in QUERIES:
        return jsonify({"error": "query sconosciuta"}), 404
    try:
        smp = get_samples()
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": "database non raggiungibile: " + str(e)}), 503

    fields = {f["name"] for f in QUERIES[qid]["fields"]}
    tmin, tmax = smp.get("tmin"), smp.get("tmax")
    out = {}

    # se servono accountId + date insieme (Q1), le agganciamo al range del
    # conto pescato — così dentro quella finestra ci sono per forza transfer.
    if "accountId" in fields:
        if not smp["accounts"]:
            return jsonify({"error": "nessun account disponibile nel dataset"}), 503
        acc = random.choice(smp["accounts"])
        out["accountId"] = str(acc)
        if "startDate" in fields and "endDate" in fields:
            try:
                with get_driver().session() as sess:
                    r = sess.run(
                        "MATCH (:Account {id:$id})-[t:TRANSFER]->() "
                        "RETURN min(t.timestamp) AS mn, max(t.timestamp) AS mx",
                        id=acc).single()
                if r and r["mn"] is not None:
                    out["startDate"] = _fmt_date(r["mn"])
                    out["endDate"]   = _fmt_date(r["mx"] + DAY_MS)
            except Exception:
                traceback.print_exc()

    if "personId" in fields:
        if not smp["persons"]:
            return jsonify({"error": "nessuna persona disponibile nel dataset"}), 503
        out["personId"] = str(random.choice(smp["persons"]))

    # date indipendenti (Q6): finestra ampia dentro al range reale
    if "startDate" in fields and "endDate" in fields and "startDate" not in out:
        if tmin is None or tmax is None or tmax <= tmin:
            return jsonify({"error": "range temporale non disponibile"}), 503
        span = max(tmax - tmin - 365 * DAY_MS, DAY_MS)
        t1 = random.randint(tmin, tmin + span)
        t2 = min(t1 + random.randint(180 * DAY_MS, 365 * DAY_MS), tmax)
        out["startDate"] = _fmt_date(t1)
        out["endDate"]   = _fmt_date(t2)

    if "minAmount" in fields:
        out["minAmount"]  = str(random.choice([100, 500, 1000, 2000, 5000]))
    if "windowDays" in fields:
        out["windowDays"] = str(random.choice([7, 14, 30, 60]))

    # anno/mese presi da un timestamp reale del dataset
    if "year" in fields and "month" in fields:
        if tmin is not None and tmax is not None:
            dt = datetime.fromtimestamp(random.randint(tmin, tmax) / 1000, tz=timezone.utc)
            out["year"]  = str(dt.year)
            out["month"] = str(dt.month)
        else:
            out["year"]  = "2021"
            out["month"] = str(random.randint(1, 12))

    if "minDegree" in fields:
        # soglia bassa, altrimenti gli hub sono troppo pochi e si vede poco
        out["minDegree"] = str(random.choice([50, 100, 200]))

    return jsonify(out)


@app.route("/query/<qid>", methods=["POST"])
def run_query(qid):
    if qid not in QUERIES:
        return render_template("results.html",
                               error="query sconosciuta: " + qid,
                               query=None, result=None), 404

    q = QUERIES[qid]
    params = {f["name"]: request.form.get(f["name"], "") for f in q["fields"]}
    labels = {f["name"]: f["label"] for f in q["fields"]}

    try:
        result = q["fn"](params)
    except ValueError as e:
        # parametro malformato (es. id non numerico, data vuota): niente traceback
        return render_template("results.html",
                               query={"id": qid, **q}, params=params, labels=labels,
                               error="Parametro non valido: " + str(e), result=None), 400
    except Exception as e:
        traceback.print_exc()
        return render_template("results.html",
                               query={"id": qid, **q}, params=params, labels=labels,
                               error=str(e), result=None)

    rows_json = json.dumps(result["rows"], default=str, indent=2, ensure_ascii=False)
    return render_template("results.html",
                           query={"id": qid, **q}, params=params, labels=labels,
                           result=result, rows_json=rows_json, error=None)


if __name__ == "__main__":
    # porto 5050: su macOS recente il 5000 è occupato dal ricevitore AirPlay
    app.run(host="0.0.0.0", port=5050, debug=True)
