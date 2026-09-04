# Q2 — MongoDB. Profilo completo di una persona (anagrafica + conti + prestiti).
#
# E' la "scheda aggregata": un solo documento in uscita con dentro gli array dei
# conti e dei prestiti. Le collezioni in Mongo sono piatte (person, account,
# loan + due collezioni-link), quindi la scheda viene ricomposta con una
# aggregation pipeline a catena di $lookup.
from app.db.mongo import get_db
from app.queries._common import timed


@timed
def run(params):
    pid = int(params["personId"])
    db = get_db()

    # 4 lookup a catena: persona -> ownAccount -> account
    #                    persona -> applyLoan  -> loan
    # $lookup = left outer join: per ogni documento cerca in "from" quelli con
    # foreignField == localField e li mette nell'array "as" (vuoto se non trova).
    # Nel 2° e 4° lookup localField punta DENTRO un array (ownLinks.accountId):
    # Mongo lo legge come "uno qualsiasi degli elementi", quindi recupera tutti
    # i conti in una sola operazione.
    pipeline = [
        {"$match": {"_id": pid}},                    # primo stadio: 1 documento, indice su _id
        {"$lookup": {"from": "personOwnAccount", "localField": "_id",
                     "foreignField": "personId", "as": "ownLinks"}},
        {"$lookup": {"from": "account",          "localField": "ownLinks.accountId",
                     "foreignField": "_id",       "as": "accounts"}},
        {"$lookup": {"from": "personApplyLoan",  "localField": "_id",
                     "foreignField": "personId", "as": "applyLinks"}},
        {"$lookup": {"from": "loan",             "localField": "applyLinks.loanId",
                     "foreignField": "_id",       "as": "loans"}},
        # tengo solo i campi utili, anche dentro gli array (i link spariscono)
        {"$project": {
            "_id": 1, "name": 1, "gender": 1, "country": 1, "city": 1, "isBlocked": 1,
            "accounts": {"_id": 1, "type": 1, "isBlocked": 1},
            "loans":    {"_id": 1, "amount": 1, "balance": 1, "usage": 1},
        }},
    ]

    # il cursore ha 0 o 1 documenti; ritorno sempre una lista come le altre query
    doc = next(db.person.aggregate(pipeline), None)
    return [doc] if doc else []
