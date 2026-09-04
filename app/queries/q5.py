# Q5 — MongoDB. Top-10 account per volume trasferito in un dato mese.
#
# Analitica ma veloce: il $match sul mese (indice su transfer.timestamp) taglia
# subito 1,38M transfer a qualche decina di migliaia, e il resto della pipeline
# lavora solo su quelli. L'equivalente SQL sarebbe
#   SELECT src, SUM(amount) ... WHERE ts BETWEEN ... GROUP BY src ORDER BY 2 DESC LIMIT 10
# piu' un join sull'anagrafica del conto.
from datetime import datetime, timezone

from app.db.mongo import get_db
from app.queries._common import timed


def month_range_ms(year, month):
    # [primo giorno del mese, primo giorno del mese dopo) in epoch ms UTC.
    # datetime() solleva ValueError per mesi fuori da 1-12: e' la nostra validazione.
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
        # 1) solo i transfer del mese (usa l'indice su timestamp)
        {"$match":  {"timestamp": {"$gte": start_ms, "$lt": end_ms}}},
        # 2) somma per conto MITTENTE: "volume trasferito" = uscite
        {"$group":  {"_id": "$accountSrc",
                     "totalAmount":  {"$sum": "$amount"},
                     "numTransfers": {"$sum": 1}}},
        # 3) $sort + $limit: Mongo li fonde in un top-k in memoria
        {"$sort":   {"totalAmount": -1}},
        {"$limit":  10},
        # 4) join con l'anagrafica DOPO il limit: 10 lookup invece di migliaia
        {"$lookup": {"from": "account",
                     "localField": "_id",
                     "foreignField": "_id",
                     "as": "account"}},
        # l'array "account" ha 0 o 1 elementi: lo appiattisco senza perdere la
        # riga se per caso il conto non fosse in anagrafica
        {"$unwind": {"path": "$account", "preserveNullAndEmptyArrays": True}},
        # 5) rinomino _id -> accountId e tengo solo le colonne per la tabella
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
