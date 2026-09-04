# carica i CSV di LDBC FinBench dentro Neo4j
import csv
import glob
import os
import sys

from neo4j import GraphDatabase

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass


NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASS", "neo4j")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "bulk-load"))
BATCH = 2000


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


def run_batch(session, cypher, csv_name, mapper):
    buf = []
    n = 0
    for r in rows_of(csv_name):
        m = mapper(r)
        if m is None:
            continue
        buf.append(m)
        if len(buf) >= BATCH:
            session.run(cypher, rows=buf)
            n += len(buf)
            buf = []
            print("    +", BATCH, "(tot", n, ")")
    if buf:
        session.run(cypher, rows=buf)
        n += len(buf)
    print("  totale:", n)
    return n


# cypher per i nodi. Pattern comune: $rows e' una lista di BATCH dizionari
# passata come parametro, UNWIND la srotola in una riga per elemento -> una
# sola chiamata di rete e una sola transazione per blocco.
# MERGE ("trova o crea") per i nodi: idempotente, rilanciare l'ETL non duplica.
Q_PERSON ="UNWIND $rows AS r MERGE (p:Person {id:r.id}) SET p.name=r.name, p.isBlocked=r.isBlocked"
Q_COMPANY = "UNWIND $rows AS r MERGE (c:Company {id:r.id}) SET c.name=r.name, c.isBlocked=r.isBlocked"
Q_ACCOUNT = "UNWIND $rows AS r MERGE (a:Account {id:r.id}) SET a.type=r.type, a.isBlocked=r.isBlocked"
Q_LOAN = "UNWIND $rows AS r MERGE (l:Loan {id:r.id}) SET l.amount=r.amount, l.balance=r.balance"
Q_MEDIUM = "UNWIND $rows AS r MERGE (m:Medium {id:r.id}) SET m.name=r.name, m.riskLevel=r.riskLevel"

# cypher per gli archi. Prima MATCH dei due estremi (veloce grazie al constraint
# su id), poi:
# - CREATE per TRANSFER/WITHDRAW/DEPOSIT/REPAY/SIGNIN: fra la stessa coppia di
#   nodi ci possono essere MOLTI eventi diversi, ogni riga CSV deve diventare un
#   arco nuovo (MERGE li fonderebbe in uno e falserebbe conteggi e somme)
# - MERGE per OWN/APPLY/GUARANTEE/INVEST: relazioni uniche per coppia
Q_TRANSFER = """
UNWIND $rows AS r
MATCH (a:Account {id:r.src}), (b:Account {id:r.dst})
CREATE (a)-[:TRANSFER {amount:r.amount, timestamp:r.ts}]->(b)
"""
Q_OWN_P = """
UNWIND $rows AS r
MATCH (p:Person {id:r.personId}), (a:Account {id:r.accountId})
MERGE (p)-[:OWN]->(a)
"""
Q_OWN_C = """
UNWIND $rows AS r
MATCH (c:Company {id:r.companyId}), (a:Account {id:r.accountId})
MERGE (c)-[:OWN]->(a)
"""
Q_DEPOSIT = """
UNWIND $rows AS r
MATCH (l:Loan {id:r.loanId}), (a:Account {id:r.accountId})
CREATE (l)-[:DEPOSIT {amount:r.amount, timestamp:r.ts}]->(a)
"""
Q_WITHDRAW = """
UNWIND $rows AS r
MATCH (a:Account {id:r.src}), (b:Account {id:r.dst})
CREATE (a)-[:WITHDRAW {amount:r.amount, timestamp:r.ts}]->(b)
"""
Q_REPAY = """
UNWIND $rows AS r
MATCH (a:Account {id:r.accountId}), (l:Loan {id:r.loanId})
CREATE (a)-[:REPAY {amount:r.amount, timestamp:r.ts}]->(l)
"""
# NB: MERGE non accetta null nelle proprietà del pattern. Sposto le
# proprietà in ON CREATE SET per tollerare timestamp/amount mancanti.
Q_APPLY_P = """
UNWIND $rows AS r
MATCH (p:Person {id:r.personId}), (l:Loan {id:r.loanId})
MERGE (p)-[rel:APPLY]->(l)
ON CREATE SET rel.amount=r.amount, rel.timestamp=r.ts
"""
Q_APPLY_C = """
UNWIND $rows AS r
MATCH (c:Company {id:r.companyId}), (l:Loan {id:r.loanId})
MERGE (c)-[rel:APPLY]->(l)
ON CREATE SET rel.amount=r.amount, rel.timestamp=r.ts
"""
Q_GUAR_P = """
UNWIND $rows AS r
MATCH (a:Person {id:r.fromId}), (b:Person {id:r.toId})
MERGE (a)-[rel:GUARANTEE]->(b)
ON CREATE SET rel.timestamp=r.ts
"""
Q_GUAR_C = """
UNWIND $rows AS r
MATCH (a:Company {id:r.fromId}), (b:Company {id:r.toId})
MERGE (a)-[rel:GUARANTEE]->(b)
ON CREATE SET rel.timestamp=r.ts
"""
Q_INV_P = """
UNWIND $rows AS r
MATCH (p:Person {id:r.personId}), (c:Company {id:r.companyId})
MERGE (p)-[rel:INVEST]->(c)
ON CREATE SET rel.ratio=r.ratio, rel.timestamp=r.ts
"""
Q_INV_C = """
UNWIND $rows AS r
MATCH (a:Company {id:r.fromId}), (b:Company {id:r.toId})
MERGE (a)-[rel:INVEST]->(b)
ON CREATE SET rel.ratio=r.ratio, rel.timestamp=r.ts
"""
Q_SIGNIN = """
UNWIND $rows AS r
MATCH (m:Medium {id:r.mediumId}), (a:Account {id:r.accountId})
CREATE (m)-[:SIGNIN {timestamp:r.ts}]->(a)
"""


# mapper CSV -> dict (schema v0.1.0: id/createTime/fromId/toId/personId/…)
def m_person(r):   return {"id": int(r["id"]), "name": r.get("name"), "isBlocked": r.get("isBlocked") == "true"}
def m_company(r):  return {"id": int(r["id"]), "name": r.get("name"), "isBlocked": r.get("isBlocked") == "true"}
def m_account(r):  return {"id": int(r["id"]), "type": r.get("type"), "isBlocked": r.get("isBlocked") == "true"}
def m_loan(r):     return {"id": int(r["id"]), "amount": to_float(r.get("loanAmount")), "balance": to_float(r.get("balance"))}
def m_medium(r):   return {"id": int(r["id"]), "name": r.get("type"), "riskLevel": r.get("riskLevel")}

def m_transfer(r):      return {"src": int(r["fromId"]), "dst": int(r["toId"]), "amount": to_float(r.get("amount")), "ts": to_ms(r.get("createTime", ""))}
def m_own_p(r):         return {"personId": int(r["personId"]), "accountId": int(r["accountId"])}
def m_own_c(r):         return {"companyId": int(r["companyId"]), "accountId": int(r["accountId"])}
def m_deposit(r):       return {"loanId": int(r["loanId"]), "accountId": int(r["accountId"]), "amount": to_float(r.get("amount")), "ts": to_ms(r.get("createTime", ""))}
def m_withdraw(r):      return {"src": int(r["fromId"]), "dst": int(r["toId"]), "amount": to_float(r.get("amount")), "ts": to_ms(r.get("createTime", ""))}
def m_repay(r):         return {"accountId": int(r["accountId"]), "loanId": int(r["loanId"]), "amount": to_float(r.get("amount")), "ts": to_ms(r.get("createTime", ""))}
def m_apply_p(r):       return {"personId": int(r["personId"]), "loanId": int(r["loanId"]), "amount": to_float(r.get("loanAmount")), "ts": to_ms(r.get("createTime", ""))}
def m_apply_c(r):       return {"companyId": int(r["companyId"]), "loanId": int(r["loanId"]), "amount": to_float(r.get("loanAmount")), "ts": to_ms(r.get("createTime", ""))}
def m_guar_p(r):        return {"fromId": int(r["fromId"]), "toId": int(r["toId"]), "ts": to_ms(r.get("createTime", ""))}
def m_guar_c(r):        return {"fromId": int(r["fromId"]), "toId": int(r["toId"]), "ts": to_ms(r.get("createTime", ""))}
# personInvest: investor è una persona; companyInvest: investor è una company
def m_inv_p(r):         return {"personId": int(r["investorId"]), "companyId": int(r["companyId"]), "ratio": to_float(r.get("ratio")), "ts": to_ms(r.get("createTime", ""))}
def m_inv_c(r):         return {"fromId": int(r["investorId"]), "toId": int(r["companyId"]), "ratio": to_float(r.get("ratio")), "ts": to_ms(r.get("createTime", ""))}
def m_signin(r):        return {"mediumId": int(r["mediumId"]), "accountId": int(r["accountId"]), "ts": to_ms(r.get("createTime", ""))}


def main():
    print("Neo4j:", NEO4J_URI)
    print("Data :", os.path.abspath(DATA_DIR))
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    with driver.session() as s:
        # DETACH DELETE cancella nodi e archi collegati; spezzato in transazioni
        # da 5000 nodi perche' Neo4j tiene lo stato della transazione in heap e
        # 6M di elementi in una sola andrebbero in out-of-memory
        print("pulisco il DB (chunked, evita OOM)...")
        s.run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS")

        # constraint di unicita' su id = anche indice: i MATCH degli archi
        # diventano ricerche O(log n) invece di scansioni di 264k nodi. Vanno
        # creati PRIMA degli archi (5M archi x 2 MATCH senza indice = ingestibile).
        # L'indice su TRANSFER.timestamp serve ai filtri temporali di Q1 e Q6.
        print("constraint e indici...")
        for lbl in ("Person", "Company", "Account", "Loan", "Medium"):
            s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE n.id IS UNIQUE" % lbl)
        s.run("CREATE INDEX IF NOT EXISTS FOR ()-[t:TRANSFER]-() ON (t.timestamp)")

        # prima i nodi, poi gli archi (sennò MATCH non trova i nodi)
        print("Person");   run_batch(s, Q_PERSON,  "person",  m_person)
        print("Company");  run_batch(s, Q_COMPANY, "company", m_company)
        print("Account");  run_batch(s, Q_ACCOUNT, "account", m_account)
        print("Loan");     run_batch(s, Q_LOAN,    "loan",    m_loan)
        print("Medium");   run_batch(s, Q_MEDIUM,  "medium",  m_medium)

        print("OWN (persona)");     run_batch(s, Q_OWN_P,    "personOwnAccount",  m_own_p)
        print("OWN (azienda)");     run_batch(s, Q_OWN_C,    "companyOwnAccount", m_own_c)
        print("APPLY (persona)");   run_batch(s, Q_APPLY_P,  "personApplyLoan",   m_apply_p)
        print("APPLY (azienda)");   run_batch(s, Q_APPLY_C,  "companyApplyLoan",  m_apply_c)
        print("DEPOSIT");           run_batch(s, Q_DEPOSIT,  "deposit",           m_deposit)
        print("WITHDRAW");          run_batch(s, Q_WITHDRAW, "withdraw",          m_withdraw)
        print("REPAY");             run_batch(s, Q_REPAY,    "repay",             m_repay)
        print("GUARANTEE (pers.)"); run_batch(s, Q_GUAR_P,   "personGuarantee",   m_guar_p)
        print("GUARANTEE (az.)");   run_batch(s, Q_GUAR_C,   "companyGuarantee",  m_guar_c)
        print("INVEST (pers.)");    run_batch(s, Q_INV_P,    "personInvest",      m_inv_p)
        print("INVEST (az.)");      run_batch(s, Q_INV_C,    "companyInvest",     m_inv_c)
        print("SIGNIN");            run_batch(s, Q_SIGNIN,   "signIn",            m_signin)
        print("TRANSFER");          run_batch(s, Q_TRANSFER, "transfer",          m_transfer)

    driver.close()
    print("fatto.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERRORE:", e, file=sys.stderr)
        sys.exit(1)
