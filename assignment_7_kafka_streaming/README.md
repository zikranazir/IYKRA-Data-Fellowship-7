# Twitter Streaming With Kafka

### How to use:

1. Create Python Virtual Environment

    - Use `virtualenv <env_name>` on your working directory
    - Activate virtual environment with command `source /bin/activate`


2. Install requirements dependencies on requirements.txt
   
    - `pip install -r requirements.txt`


3. Change .env.example to .env

    - `cp .env.example .env`


4. Run Docker Compose (it will take a long time if you never running the docker-compose before)
    
    - `docker-compose up`
   

5. Run kafka_producer.py 
    
    - `python kafka_producer.py <filter/keyword> -t <topic_name>`
    or
    - `python kafka_producer.py <filter/keyword> --topic <topic_name>`


6. Check messages on kafka_consumer.py

    - `python kafka_consumer.py`


7. Shutdown docker-compose

    - `docker-compose down`
