from kafka import KafkaProducer
from json import dumps
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

pinterest_data_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="pinterest_data_producer",
    value_serializer=lambda inputmessage: dumps(inputmessage).encode("ascii"),
)

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    pinterest_data_producer.send(topic="PinterestData", value=data)
    print(data)
    return item


if __name__ == '__main__':
    uvicorn.run("kafka_producer:app", host="localhost", port=8000)
