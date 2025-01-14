# import faust
# import os


import faust

import time
import pandas as pd 
import pickle
# from pymongo import MongoClient
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

mongo_client = AsyncIOMotorClient('mongodb://mongo_data:27017')
db = mongo_client['f1_data']
collection = db['real_time_simulations']

app = faust.App('data_producer', broker='kafka://kafka:9092')

data_topic = app.topic('data_topic', value_type=dict)
print("created topic")

full_data = pd.read_csv("processed_data_for_training.csv")
print("shape of full data: ", full_data.shape)


async def send_simulation_info():
    print("sent initial record")
    await data_topic.send(key="test_1", value= {"simulation_start_time":datetime.now()})


async def produce_data(argument):

    for _, record in full_data.iterrows():
        # Convert pandas Series to a dictionary and serialize if necessary
        record_dict = record.to_dict()
        
        print("attempt to send this record: ", record_dict)
        key = str(record_dict.get('Circuit', '_default'))  
        try:
            await data_topic.send(key=key, value=record_dict)
            print(f"Produced message with key: {key}, value: {record_dict}")
        except Exception as e:
            print(f"Failed to produce message: {e}")
        
        await asyncio.sleep(5)
        

@app.page('/simulate_gp')
async def start_simulation(self, request):
    print("starting simulation")
    argument = "Test arg"
    asyncio.create_task(produce_data(argument))
    last_record = await collection.find_one(
        sort=[('_id', -1)],
        projection={'_id': 0}  # Excludes the _id field
    )
    response_data = {
        'latest_message': "received message",
        'timestamp': str(datetime.now()),
        'last_record': last_record
    }
    return self.json(response_data)
    # return self.json({
    #     'latest_message': "received message",
    #     'timestamp': str(datetime.now())
    # })


if __name__=="__main__":
    app.main()
    asyncio.run(send_simulation_info)