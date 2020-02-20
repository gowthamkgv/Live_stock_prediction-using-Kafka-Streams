OUTLINE:

We have ran the cryptproducer and NLP producer for 20 hours in one day(5th Aug) and ran consumer after 20th hour to make sure there is no resource issue 

Cryptproducer.py

This code gets the crypto data from the API for BTC and ETH and puts in the topic "crypt"

NLPproducer.py

This code gets the latest news articles from the API and stores in topic "NLP"

Structuredstreaming.py

Entering into pyspark terminal with required packages and executing the consumer code to directly stream data into HDFS to the specified path
I have merged both the consumer code line in to one file, as we executed them manually not through spark submit(as it we faced a little issue doing that way)