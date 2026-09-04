# Q2 — MongoDB. Profilo completo di una persona (anagrafica + conti + prestiti).
from app.db.mongo import get_db
from app.queries._common import timed


@timed
def run(params):
    pid = int(params["personId"])
    db = get_db()

    # 4 lookup a catena: persona -> ownAccount -> account
    #                    persona -> applyLoan  -> loan
    pipeline = [
        {"$match": {"_id": pid}},
        {"$lookup": {"from": "personOwnAccount", "localField": "_id",
                     "foreignField": "personId", "as": "ownLinks"}},
        {"$lookup": {"from": "account",          "localField": "ownLinks.accountId",
                     "foreignField": "_id",       "as": "accounts"}},
        {"$lookup": {"from": "personApplyLoan",  "localField": "_id",
                     "foreignField": "personId", "as": "applyLinks"}},
        {"$lookup": {"from": "loan",             "localField": "applyLinks.loanId",
                     "foreignField": "_id",       "as": "loans"}},
        {"$project": {
            "_id": 1, "name": 1, "gender": 1, "country": 1, "city": 1, "isBlocked": 1,
            "accounts": {"_id": 1, "type": 1, "isBlocked": 1},
            "loans":    {"_id": 1, "amount": 1, "balance": 1, "usage": 1},
        }},
    ]

    doc = next(db.person.aggregate(pipeline), None)
    return [doc] if doc else []
