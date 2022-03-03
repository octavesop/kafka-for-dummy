# from kafka import KafkaProducer
from kafka import KafkaProducer
from json import dumps
import json

topic = 'test'
producer = KafkaProducer(
        acks=0, 
        compression_type='gzip', 
        bootstrap_servers='localhost:9094',
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

f = open('./dummy-data/dummy.json', 'r')
line = f.read()
json = json.loads(line)

for data in json: 
    print(data)
    producer.send(topic, value=data)
    producer.flush()

print('::: 작업 완료 ::: ')
f.close()