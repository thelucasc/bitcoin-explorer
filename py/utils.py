from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import asyncio
from datetime import datetime
import pytz
from motor.motor_asyncio import AsyncIOMotorClient
# Replace the placeholder with your Atlas connection string
uri = "mongodb://localhost:27017/"
# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. Volume")
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

def search_documents(start_time, end_time):

    db = client['btc']
    # Selecionar a coleção
    collection = db["blocks"]
    # Buscar todos os documentos na coleção
    # Connect to MongoDB


    # Convert string to datetime object
    # start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.000Z")
    # end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.000Z")

    # Ensure the dates are in UTC
    # start_time = pytz.UTC.localize(start_time)
    # end_time = pytz.UTC.localize(end_time)

    # Create a cursor object using a query
    cursor = collection.find({'time': {'$gte': start_time, '$lt': end_time}})

    # Create a dictionary to store the results by day
    results_by_day = {}

    # Iterate through the cursor
    for document in cursor:
        # Extract the day from the time
        day = document['time'][:10]  # Extract the date part of the ISO string

        # If the day is not in the results yet, add it
        if day not in results_by_day:
            results_by_day[day] = []

        # Append the document to the results for that day
        results_by_day[day].append(document)

    # Return the results
    return results_by_day


# Define the start and end times
start_time = "2022-03-30T00:00:00.000Z"
end_time = "2022-04-03T00:00:00.000Z"

# Call the function
results = search_documents(start_time, end_time)
print(results)
# Print the results
for day in results:
    print(f"Documents for {day}:")
    for document in results[day]:
        print(document)