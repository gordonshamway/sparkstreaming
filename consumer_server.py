from kafka import KafkaConsumer
import json
import time

class ConsumerServer(KafkaConsumer):
    
    
    ## check parameters
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['calls'])

        while True:
            for message in consumer:
                print(message.value)
                if message is None:
                    break
        consumer.close()
        
if __name__ == "__main__":
    cons = ConsumerServer()
    cons.run()