import faust
import time
import pandas as pd
import pickle
from datetime import datetime
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

mongo_client = AsyncIOMotorClient('mongodb://mongo_data:27017')
db = mongo_client['f1_data']
collection = db['real_time_simulations']

# Define your Faust app
app = faust.App('data_app', 
                broker='kafka://kafka:9092',
                broker_auto_create_topics=True)

# Define a Kafka topic
data_topic = app.topic('data_topic')
print("created topic")


# List to store processed data
# data_to_send = []

async def insert_record(record):
    result = await collection.insert_one(record)
    print(f'Record inserted with id: {result.inserted_id}')


# # Agent to consume and process records
@app.agent(data_topic)
async def consume_data(records):
    print("starting processing records")
    async for record in records:
        print("processing record")
        processed_record = {
            "timestamp": datetime.now(),
            "speed": record["SpeedI1"],
            "driver": record["Driver"],
            "laptime": record["Sector2Time_scaled"]
        }
        uploading = await collection.insert_one(processed_record)
        print("uploaded: ", uploading.inserted_id)
        # print("created object: ", obj_to_show)
        # data_to_send.append(obj_to_show)
        # print("number of processed records: ", len(data_to_send))



if __name__ == '__main__':
    app.main()
