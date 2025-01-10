# import faust
# import os


import faust

import time
import pandas as pd 
import pickle
# from pymongo import MongoClient
from datetime import datetime
import asyncio
# from fastapi import FastAPI
from pydantic import BaseModel

# Define your Faust app
app = faust.App('data_consumer', broker='kafka://kafka:9092')

## utilizzando una table si  mantiene lo stato tra i messaggi senza perderli
##  
# Define a Kafka topic
data_topic = app.topic('data_topic')
# last_message = app.Table('last_message', default=str)

# Define a Faust Table
# data_table = app.Table(
#     'data_table',
#     default=dict,  # Default value for table entries
#     partitions=1,  # Number of Kafka partitions
# )
# Stream processor to consume from the topic and populate the table

# @app.on_ready()
# async def on_ready():
#     try:
#         # Try to check if the topic exists or if we can connect
#         logger.info("Checking connection to Kafka and the topic...")
#         # A dummy record to test the connection
#         record = await data_topic.get(key='test', default=None)
#         if record is None:
#             logger.error(f"Failed to fetch record from topic '{data_topic.name}'")
#         else:
#             logger.info(f"Successfully connected to Kafka and found a record: {record}")
#     except Exception as e:
#         logger.error(f"Error connecting to Kafka or topic '{data_topic.name}': {e}")




# Define your Faust app
app = faust.App('data_producer', broker='kafka://kafka:9092')

# Define a Kafka topic
data_topic = app.topic('data_topic', value_type=dict)
print("created topic")

full_data = pd.read_csv("processed_data_for_training.csv")
print("shape of full data: ", full_data.shape)

class SimulationRequest(BaseModel):
    duration: int
    intensity: str

## produce data in data_topic
@app.timer(interval=1.0)
async def produce_data(argument = "None"):
    for _, record in full_data.iterrows():
        # Convert pandas Series to a dictionary and serialize if necessary
        record_dict = record.to_dict()
        print("argument for subsetting data sent from http: ",argument )
        print("attempt to send this record: ", record_dict)
        key = str(record_dict.get('Circuit', '_default'))  
        try:
            await data_topic.send(key=key, value=record_dict)
            print(f"Produced message with key: {key}, value: {record_dict}")
        except Exception as e:
            print(f"Failed to produce message: {e}")
        
        await asyncio.sleep(5)





new_messages = []
@app.agent(data_topic) ## consuma messaggi 
async def process_data(stream):
    print("input stream: ", stream)
    async for record in stream:
        print("record recived in consumer type and data: ", type(record), "\n", record)
        new_messages.append(record)
        print("processed record", record)
        if len(new_messages) > 100:  # Limit to the latest 100 messages
            new_messages.pop(0)
        print("processed record", record)

@app.page('/driver_data')
async def latest_message(self,request):

    latest = new_messages
    return self.json({'latest_message': latest,
                     'timestamp'      : str(datetime.now())})



# # Endpoint to query the table
# @app.page('/data')
# # @app.table_route(table=data_table, query_param='key')
# async def get_data(self, request):
    
#     # async for record in data_topic.stream():
#     #     data = f'last message from topic_test: {last_message_from_topic[0]} | '
#     #     data += f'Server Time : {datetime.now()}'
#     #     print(data)
#     data = {
#         "last message" : last_message_from_topic[0],
#         "time"         : datetime.now().isoformat()
#     }
#     return self.json(data)
#     # return self.json(results)


if __name__ == '__main__':
    app.main()