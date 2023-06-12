from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
# Replace the placeholder with your Atlas connection string
uri = "mongodb://localhost:27017/"
# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))
                          
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
# 03/01/2023 - 10/01/2023 770000 > 771500         inicio de alta ok
# 10/03/2023 - 12/03/2023 780000 > 780600         inicio de alta ok
# 06/10/202 - 16/10/2022 757451 > 759000         lateralização ok
# 04/06/2022 09/06/2022 - 739200  -> 740000                queda 1  ok
# 01/05/2022 - 05/05/2022   -> 734400  -> 735100           queda 2  ok     
# 02/11/2022 -> 07/11/2022    761300 > 762100      queda 3 ok
# 30/03/2022 -> 04/04/2022    729700 > 730450      queda  4 ok
# Selecionar o banco de dados
dic_total = {}

async def consulta_assincrona(highorlow):

    db = client['btc']
    # Selecionar a coleção
    collection = db[highorlow]
    # Buscar todos os documentos na coleção
    pipeline = [
    {
        '$group': {
            '_id': '$snd',
            'count_total': {'$sum': '$count'},
            'repeticoes': {'$sum': 1},
            'amount': {'$sum': '$total'},
            'total_control': {'$sum': '$total_control'},
            'count_control': {'$sum': '$count_control'},
        }
    },
    {
        '$match': {'repeticoes': {'$gt': 3}}
    },
    {
        '$sort': {'sats': -1}
    },
   {
        '$match': {'count_total': {'$lt': 15}}
    }
    ]
    resultado_high = list(collection.aggregate(pipeline))
    # print(resultado_high)
    for item in resultado_high:
        snd = item['_id']
        count_total = item['count_total']
        count_control = item['count_control']
        amount_control = item['total_control']
        sats = item['amount']
        repeticoes = item['repeticoes']
        if sats > amount_control*2:
            if snd in dic_total:
                # print(snd)
                dic_total[snd].append( {highorlow : { "count_total": count_total, "repeticoes": repeticoes, "sats": sats, "amount_control": amount_control, "count_control": count_control}})
            else:
                dic_total[snd] = [{highorlow : {"count_total": count_total, "repeticoes": repeticoes, "sats": sats, "amount_control": amount_control, "count_control": count_control}}]

            # print(f"SND: {snd}, count_total: {count_total}, repeticoes: {repeticoes}, sats: {sats}, amount_control: {amount_control}, count_control: {count_control} ")
    # resultado = list(collection.aggregate(pipeline))
    # print("Número de ocorrências de idade 34:", resultado)
    # documentos = list(documents_cursor)
    # print(dic_total)

loop = asyncio.get_event_loop()
# loop.run_until_complete(consulta_assincrona("raw_high_trend"))
loop.run_until_complete(consulta_assincrona("raw_low_trend"))
print(dic_total)
