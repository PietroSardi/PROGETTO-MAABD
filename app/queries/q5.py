# Q5 — MongoDB. Top-10 account per volume trasferito in un dato mese.
from datetime import datetime, timezone

from app.db.mongo import get_db
from app.queries._common import timed


def month_range_ms(year, month):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    # mese successivo: dicembre -> gennaio anno+1
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


@timed
def run(params):
    year  = int(params["year"])
    month = int(params["month"])
    start_ms, end_ms = month_range_ms(year, month)

    pipeline = [
        {"$match":  {"timestamp": {"$gte": start_ms, "$lt": end_ms}}},
        {"$group":  {"_id": "$accountSrc",
                     "totalAmount":  {"$sum": "$amount"},
                     "numTransfers": {"$sum": 1}}},
        {"$sort":   {"totalAmount": -1}},
        {"$limit":  10},
        {"$lookup": {"from": "account",
                     "localField": "_id",
                     "foreignField": "_id",
                     "as": "account"}},
        {"$unwind": {"path": "$account", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "accountId":    "$_id",
            "totalAmount":  1,
            "numTransfers": 1,
            "accountType":  "$account.type",
            "isBlocked":    "$account.isBlocked",
        }},
    ]
    return list(get_db().transfer.aggregate(pipeline))
