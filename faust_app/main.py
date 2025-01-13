# import faust
# import os


import faust

import time
import pandas as pd 
import pickle
# from pymongo import MongoClient
from datetime import datetime
import asyncio
from faust.web import Response
# from fastapi import FastAPI
# from pydantic import BaseModel

# Define your Faust app
app = faust.App('data_consumer', 
                broker='kafka://kafka:9092',
                broker_auto_create_topics=True,
                
            )
## utilizzando una table si  mantiene lo stato tra i messaggi senza perderli
##  
# Define a Kafka topic
data_topic = app.topic('data_topic')

app = faust.App('data_producer', broker='kafka://kafka:9092')

# Define a Kafka topic
data_topic = app.topic('data_topic')
print("created topic")

full_data = pd.read_csv("processed_data_for_training.csv")
print("shape of full data: ", full_data.shape)

## produce data in data_topic
# @app.timer(interval=1.0)

async def produce_data(argument = "None"):
    print("recived argument: ", argument)
    for _, record in full_data.iterrows():
        # Convert pandas Series to a dictionary and serialize if necessary
        record_dict = record.to_dict()
        print("argument for subsetting data sent from http: ",argument )
        # print("attempt to send this record: ", record_dict)
        key = str(record_dict.get('Circuit', '_default'))  
        try:
            await data_topic.send(key=key, value=record_dict)
            print(f"Produced message with key: {key}, value: {record_dict}")
        except Exception as e:
            print(f"Failed to produce message: {e}")
        
        await asyncio.sleep(5)




data_to_send = []

# @app.agent(data_topic)
async def process_record(stream):
    print("input stream: ", stream)
    async for record in stream:
        # print("record recived in consumer type and data: ", type(record), "\n", record)
        print("processing record")
        obj_to_show = {
            "timestamp" : datetime.now(),
            "speed"     : record["SpeedI1"],
            "driver"    : record["Driver"],
            "laptime"   : record["Sector2Time_scaled"] 
        }
        data_to_send.append(obj_to_show)
        print("shape of object of processed record: ", len(data_to_send))


@app.agent(data_topic)
async def consume_data(records):
    while True:
        try:
            async for record in records:
                process_record(record)
            break
        except Exception as e:
            print(f"Error consuming data: {e}, retrying...")
            await asyncio.sleep(5)  # Retry after a delay







@app.page('/simulate_gp')
async def start_simulation(self, request):
    # Send the payload to the Kafka topic
    print("starting simulation")
    # duration = request.query.get('duration', 'default_duration')
    # intensity = request.query.get('intensity', 'default_intensity')
    
    argument = "Test arg"
    ## coroutine in background, non blocca l'esecuzione
    asyncio.create_task(produce_data(argument))
    # await produce_data(argument)
    # await asyncio.sleep(5)
    # await data_topic.send(value=request.dict())
    # try:
    #     response = data_to_send[-1]
    #     response["status"] = 200
    # except:
    #     response = {
    #         "message" : "not enough data to conusme",
    #         "time"    : datetime.now,
    #         "status" : 200
    #     }
    response = {
            "message" : "not enough data to conusme",
            "time"    : datetime.now,
            "status" : 200
        }


    return self.json({'latest_message': "recived message",
                     'timestamp'      : str(datetime.now())})





# @app.page('/driver_data')
# async def latest_message(self,request):

#     latest = new_messages
#     return self.json({'latest_message': latest,
#                      'timestamp'      : str(datetime.now())})



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