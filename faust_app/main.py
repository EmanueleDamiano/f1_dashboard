# import faust
# import os


import faust

import time
import pandas as pd 
import pickle
# from pymongo import MongoClient
import asyncio
from datetime import datetime
import logging

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

@app.agent(data_topic) ## consuma messaggi 
async def process_data(stream):
    print("input stream: ", stream)
    async for record in stream:
        print("record recived in consumer type and data: ", type(record), "\n", record)
        # last_message["data"] = record
        # print("processed record", record)
        
@app.page('/latest')
async def latest_message(request):
    latest = last_message['latest']
    return app.json({'latest_message': latest})



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