import tweepy                                                                                                                                                           
import time                                                                                                                                                             
from kafka import KafkaConsumer, KafkaProducer                                                                                                                          
import json                                                                                                                                                             
import requests                                                                                                                                                         
import datetime                                                                                                                                                         
producer = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')                                                                                          
topic_name = 'crypt'                                                                                                                                                    
def get_data():                                                                                                                                                         
        res = requests.get("https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC,ETH&tsyms=USD,EUR")                                                         
        a=res.content                                                                                                                                                   
        y=json.loads(a.decode('utf8'))                                                                                                                                  
        ticks = y['RAW']['BTC']['USD']['LASTUPDATE']                                                                                                                    
        for key,value in y.items():                                                                                                                                     
                #doc = {'Currency':key, 'Price_USD': y[key]['USD'], 'Price_EUR':y[key]['EUR']}                                                                          
                converted_timeStamp = datetime.datetime.now() + datetime.timedelta(microseconds = ticks)                                                                
                doc_BTC = 'BTC' +','+ str(y['RAW']['BTC']['USD']['PRICE']) + ',' + str(y['RAW']['BTC']['EUR']['PRICE'])+ ',' + str(converted_timeStamp)                 
                doc_ETH = 'ETH'+ ',' + str(y['RAW']['ETH']['USD']['PRICE']) + ','+ str(y['RAW']['ETH']['EUR']['PRICE']) + ',' + str(converted_timeStamp)                
                #record = json.dumps(doc)                                                                                                                               
                producer.send(topic_name,doc_BTC)                                                                                                                       
                producer.send(topic_name,doc_ETH)                                                                                                                       
def periodic_work(interval):                                                                                                                                            
        while True:                                                                                                                                                     
                get_data()                                                                                                                                              
 #interval should be an integer, the number of seconds to wait                                                                                                          
                time.sleep(interval)                                                                                                                                    
periodic_work(60)   