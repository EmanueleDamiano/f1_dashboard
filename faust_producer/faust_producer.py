# import faust
# import os


import faust

import time
import pandas as pd 
import pickle
# from pymongo import MongoClient
import asyncio
from datetime import datetime
# from aiohttp_sse import sse_response

# # Define Faust app
# app = faust.App('local-simulation', broker='kafka://kafka:9092')


# Define your Faust app
app = faust.App('data_producer', broker='kafka://kafka:9092')

# Define a Kafka topic
data_topic = app.topic('data_topic', value_type=dict)
print("created topic")

full_data = pd.read_csv("processed_data_for_training.csv")
print("shape of full data: ", full_data.shape)




# object_to_send = [
#     {'driver_number': 16, 'team_name': 'Ferrari', 'full_name': 'Charles LECLERC', 'name_acronym': 'LEC', 'session_key': 9153, 'circuit_short_name': 'Monza', 'session_name': 'Qualifying'},
#     {'driver_number': 16, 'team_name': 'Ferrari', 'full_name': 'Charles LECLERC', 'name_acronym': 'LEC', 'session_key': 9157, 'circuit_short_name': 'Monza', 'session_name': 'Race'},
#     {'driver_number': 16, 'team_name': 'Ferrari', 'full_name': 'Charles LECLERC', 'name_acronym': 'LEC', 'session_key': 9511, 'circuit_short_name': 'Imola', 'session_name': 'Qualifying'}
# ]

## produce data in data_topic
@app.timer(interval=1.0)
async def produce_data():
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

# @app.timer(interval=5.0)
# async def produce_data():
#     for record in object_to_send:
#         # Convert pandas Series to a dictionary and serialize if necessary
        
#         print("attempt to send this record: ", record)
#         # key = str(record.get('team_name', '_default'))  
#         try:
#             await data_topic.send(key="driver_record_number", value=record)
#             print(f"Produced message with value: {record}")
#             print("type of produced message: ", type(record))
#         except Exception as e:
#             print(f"Failed to produce message: {e}")
        
#         await asyncio.sleep(5)



if __name__=="__main__":
    app.main()  