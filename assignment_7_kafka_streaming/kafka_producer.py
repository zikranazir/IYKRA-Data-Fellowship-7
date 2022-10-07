import json
import argparse
import time
import os
from tweepy.streaming import Stream
from confluent_kafka import Producer
from dotenv import load_dotenv
from lib import logger

logger = logger.get_logger("twit.stream")
load_dotenv()


kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})
logger.info("Kafka Producer Was Initiated")

def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {}'.format(msg.topic())
        logger.info(message)
#######################################################


class TwitterStreamListener(Stream):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    """
    def create_topic(self, topic):
        self._topic = topic

    def on_data(self, raw_data):
        try:
            data = json.loads(raw_data)
            kafka_producer.poll(1.0)
            kafka_producer.produce(self._topic, value=raw_data, callback=receipt)
            info  = "\n--------------------------------------------------------"
            info += "\ntweet id: " + str(data['id_str'])
            info += "\ntweet date: " + data['created_at']
            info += "\nusername: " + data['user']['name']
            info += "\ntext: " + data['text'][:100].replace("\n", "\\n")
            info += "\n-------------------------------------------------------\n"
            logger.info(info)
            kafka_producer.flush()
            time.sleep(3)
        except Exception as e:
            logger.error(e)

    def on_error(self, status_code):
        raise Exception(status_code)

#######################################################

def main():
    #parser = argparse.ArgumentParser()
    #parser.add_argument("keywords", help="List keyword splitting by comma")
    args = "pssi" #parser.parse_args()
    topic = "bola"

    consumer_key = os.environ.get('CONSUMER_KEY')
    consumer_secret = os.environ.get('CONSUMER_SECRET')
    access_token = os.environ.get('ACCESS_TOKEN')
    secret_token = os.environ.get('SECRET_TOKEN')

    try:
        # run stream twitter
        stream = TwitterStreamListener(consumer_key, consumer_secret,
                                       access_token, secret_token)
        if args is not None and topic is not None:
            # stream data by keywords
            stream.create_topic(topic)
            stream.filter(track=[args])
        else:
            logger.warning("Keywords Not Found")
    except Exception as e:
        logger.error("Streamer error: {}".format(e))
        if e == 420:
            logger.info("Need a rest")
            time.sleep(15 * 60)
        else:
            return False


if __name__ == '__main__':
    main()
