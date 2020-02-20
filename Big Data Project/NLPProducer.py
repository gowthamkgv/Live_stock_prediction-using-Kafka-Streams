import time
from kafka import KafkaConsumer, KafkaProducer
import json
import requests
import datetime

producer = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
topic_name = 'NLP'

def get_data():
	res_BTC = requests.get("https://min-api.cryptocompare.com/data/v2/news/?categories=BTC&lang=EN")
	res_ETH = requests.get("https://min-api.cryptocompare.com/data/v2/news/?categories=ETH&lang=EN")
	a = res_BTC.content
	b = res_ETH.content
#print(a)
	y = json.loads(a)
	z = json.loads(b)

ticks = value['published_on']
converted_timeStamp = datetime.datetime.now() + datetime.timedelta(microseconds = ticks)
#print(converted_timeStamp)
for value in y['Data']:
    doc_BTC = 'BTC'+ ',' + str(converted_timeStamp)+','+ str(value['body'])
    #print(doc_BTC)
	producer.send(topic_name,doc_BTC)
for value in z['Data']:
    doc_ETH = 'ETH'+ ',' + str(converted_timeStamp)+','+ str(value['body'])
    #print(doc_ETH)
	producer.send(topic_name,doc_ETH)
def periodic_work(interval):
	while True:
		get_data()
		time.sleep(interval)
periodic_work(1)


