# Smoke test: esegue le 6 query con i parametri di default della UI e verifica
# che ognuna torni almeno una riga senza errori. Richiede i DB Docker attivi.
#
#   .venv/bin/python test_smoke.py
import sys

from app.app import QUERIES
from app.queries import q3, q6


def main():
    ok = True
    for qid, q in QUERIES.items():
        params = {f["name"]: f["value"] for f in q["fields"]}
        r = q["fn"](params)
        status = "ok " if r["count"] > 0 else "VUOTA"
        ok &= r["count"] > 0
        print(f"{qid} {status} rows={r['count']:>3} {r['elapsed_ms']:>8.1f} ms")

    # Q3 su un conto intestato a un'azienda: l'intestatario deve arrivare da companyOwnAccount
    r = q3.run({"accountId": "237846905076329218"})["rows"][0]
    assert r["owner"] and r["owner"]["ownerType"] == "company", r["owner"]
    print("q3 company owner ok:", r["owner"]["name"])

    # Q6 variante top x%: nHubs deve essere ~1% dei conti attivi e ordinata per degree
    rows = q6.run({"startDate": "2020-01-01", "endDate": "2023-01-01",
                   "minDegree": "0", "topPct": "1"})["rows"]
    assert rows and rows[0]["nHubs"] > 20, rows[:1]
    assert rows[0]["degree"] >= rows[-1]["degree"] >= rows[0]["cutoffDegree"]
    assert all(r["ownerType"] in ("person", "company") for r in rows), "hub senza intestatario"
    print(f"q6 top 1% ok: nHubs={rows[0]['nHubs']} cutoffDegree={rows[0]['cutoffDegree']}")

    print("TUTTO OK" if ok else "QUALCHE QUERY VUOTA")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
