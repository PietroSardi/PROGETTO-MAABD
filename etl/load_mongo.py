# carica i CSV di LDBC FinBench dentro MongoDB
import csv
import glob
import os
import sys

from pymongo import MongoClient, ASCENDING


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
DB_NAME = os.environ.get("MONGO_DB", "findetect")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "bulk-load"))
BATCH = 10000


def to_ms(s):
    # createTime e' un epoch in millisecondi; -1 o vuoto vuol dire "data assente"
    s = (s or "").strip()
    if s in ("", "-1", "null"):
        return None
    return int(s)


def to_float(x):
    # Spark scrive importi con virgola decimale (locale it_IT). Normalizzo.
    if x is None or x == "":
        return 0.0
    return float(str(x).replace(",", "."))


def find_csv(name):
    # lo spark del datagen scrive dir/part-*.csv, ma a volte capita un file piatto
    pats = [
        os.path.join(DATA_DIR, "**", name, "*.csv"),
        os.path.join(DATA_DIR, "**", name + ".csv"),
        os.path.join(DATA_DIR, name, "*.csv"),
        os.path.join(DATA_DIR, name + ".csv"),
    ]
    out = []
    for p in pats:
        out += glob.glob(p, recursive=True)
    return sorted(set(out))


def rows_of(name):
    files = find_csv(name)
    if not files:
        print("  [skip] nessun CSV per", name)
        return
    for f in files:
        with open(f, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh, delimiter="|"):
                yield row


def load(db, coll_name, csv_name, mapper):
    # carica una collezione a blocchi da BATCH documenti: un insert_one per riga
    # sarebbero 1,4M round-trip per i soli transfer, con insert_many sono 138.
    # ordered=False: Mongo puo' inserire il blocco in parallelo e non si ferma
    # al primo errore. Il mapper trasforma la riga CSV nel documento.
    coll = db[coll_name]
    coll.drop()   # ricarico da zero: rilanciare lo script e' idempotente
    buf = []
    n = 0
    for r in rows_of(csv_name):
        doc = mapper(r)
        if doc is None:
            continue
        buf.append(doc)
        if len(buf) >= BATCH:
            coll.insert_many(buf, ordered=False)
            n += len(buf)
            buf = []
            print("   ", coll_name, n)
    if buf:
        coll.insert_many(buf, ordered=False)
        n += len(buf)
    print("  totale", coll_name, "=", n)
    return n


# lo schema dei CSV (datagen v0.1.0) usa colonne camelCase e createTime come epoch ms.
# Uso l'id FinBench come _id del documento: cosi' la ricerca per chiave sfrutta
# l'indice automatico e Neo4j e Mongo condividono gli stessi identificativi
# (indispensabile per le query cross-database). I booleani arrivano come
# stringhe "true"/"false".
def map_person(r):
    return {
        "_id": int(r["id"]),
        "name": r.get("name"),
        "isBlocked": r.get("isBlocked") == "true",
        "gender": r.get("gender"),
        "birthday": r.get("birthday"),
        "country": r.get("country"),
        "city": r.get("city"),
        "creationDate": to_ms(r.get("createTime", "")),
    }


def map_company(r):
    return {
        "_id": int(r["id"]),
        "name": r.get("name"),
        "isBlocked": r.get("isBlocked") == "true",
        "country": r.get("country"),
        "city": r.get("city"),
        "business": r.get("business"),
        "url": r.get("url"),
        "creationDate": to_ms(r.get("createTime", "")),
    }


def map_account(r):
    return {
        "_id": int(r["id"]),
        "type": r.get("type"),
        "nickname": r.get("nickname"),
        "isBlocked": r.get("isBlocked") == "true",
        "accountLevel": r.get("accountLevel"),
        "ownerType": r.get("Owner"),
        "creationDate": to_ms(r.get("createTime", "")),
    }


def map_loan(r):
    return {
        "_id": int(r["id"]),
        "amount": to_float(r.get("loanAmount")),
        "balance": to_float(r.get("balance")),
        "usage": r.get("usage"),
        "interestRate": to_float(r.get("interestRate")),
        "creationDate": to_ms(r.get("createTime", "")),
    }


def map_medium(r):
    # nota: il CSV medium ha 'type' al posto di 'name'
    return {
        "_id": int(r["id"]),
        "name": r.get("type"),
        "isBlocked": r.get("isBlocked") == "true",
        "riskLevel": r.get("riskLevel"),
        "creationDate": to_ms(r.get("createTime", "")),
    }


def map_transfer(r):
    # tengo solo quello che serve a Q5 (aggregazione mensile)
    return {
        "accountSrc": int(r["fromId"]),
        "accountDst": int(r["toId"]),
        "amount": to_float(r.get("amount")),
        "timestamp": to_ms(r.get("createTime", "")),
        "orderNum": r.get("orderNum"),
        "payType": r.get("payType"),
    }


def map_own_person(r):
    return {"personId": int(r["personId"]), "accountId": int(r["accountId"])}


def map_own_company(r):
    # un conto e' intestato o a una persona o a un'azienda: serve anche questo
    # link per risolvere l'intestatario dei conti aziendali (Q3 e Q6)
    return {"companyId": int(r["companyId"]), "accountId": int(r["accountId"])}


def map_apply_person(r):
    return {
        "personId": int(r["personId"]),
        "loanId": int(r["loanId"]),
        "loanAmount": to_float(r.get("loanAmount")),
        "creationDate": to_ms(r.get("createTime", "")),
    }


# collezione Mongo -> (nome CSV, mapper). Le ultime tre servono per Q2, Q3 e Q6
# (join persona/azienda <-> conto, persona <-> prestito).
COLLECTIONS = {
    "person":            ("person",            map_person),
    "company":           ("company",           map_company),
    "account":           ("account",           map_account),
    "loan":              ("loan",              map_loan),
    "medium":            ("medium",            map_medium),
    "transfer":          ("transfer",          map_transfer),
    "personOwnAccount":  ("personOwnAccount",  map_own_person),
    "companyOwnAccount": ("companyOwnAccount", map_own_company),
    "personApplyLoan":   ("personApplyLoan",   map_apply_person),
}


def main():
    print("Mongo:", MONGO_URI, " DB:", DB_NAME)
    print("Data :", os.path.abspath(DATA_DIR))
    db = MongoClient(MONGO_URI)[DB_NAME]

    # senza argomenti carica tutto; con argomenti solo le collezioni indicate
    # (es. `python -m etl.load_mongo companyOwnAccount` per aggiungerne una)
    wanted = sys.argv[1:] or list(COLLECTIONS)
    unknown = set(wanted) - set(COLLECTIONS)
    if unknown:
        sys.exit("collezioni sconosciute: %s (valide: %s)" % (", ".join(unknown), ", ".join(COLLECTIONS)))
    for name in wanted:
        csv_name, mapper = COLLECTIONS[name]
        load(db, name, csv_name, mapper)

    # indici DOPO il caricamento: mantenere un B-tree aggiornato durante 1,4M di
    # insert costa, costruirlo alla fine in un colpo e' piu' rapido. (In Neo4j
    # facciamo il contrario perche' li' il caricamento degli archi fa MATCH sui
    # nodi e senza indice sarebbe una scansione per ogni arco.)
    # A cosa servono: transfer.timestamp -> $match di Q5; personOwnAccount.personId
    # e personApplyLoan.personId -> $lookup di Q2; *.accountId -> intestatario in Q3/Q6.
    # Idempotenti: create_index non fa nulla se l'indice esiste gia'.
    print("creo indici...")
    db.transfer.create_index([("accountSrc", ASCENDING)])
    db.transfer.create_index([("accountDst", ASCENDING)])
    db.transfer.create_index([("timestamp", ASCENDING)])
    db.transfer.create_index([("accountSrc", ASCENDING), ("timestamp", ASCENDING)])
    db.personOwnAccount.create_index([("personId", ASCENDING)])
    db.personOwnAccount.create_index([("accountId", ASCENDING)])
    db.companyOwnAccount.create_index([("accountId", ASCENDING)])
    db.personApplyLoan.create_index([("personId", ASCENDING)])
    print("ok")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERRORE:", e, file=sys.stderr)
        sys.exit(1)
