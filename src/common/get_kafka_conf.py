import logging

from confluent_kafka import Producer

from .settings import settings


def get_kafka_conf():
    host = settings.KAFKA_HOST
    port = settings.KAFKA_PORT
    conf = {"bootstrap.servers": f"{host}:{port}"}
    return conf


def create_kafka_producer() -> Producer:
    try:
        conf = get_kafka_conf()
        producer = Producer(conf)
        logging.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logging.error(f"Error creating kafka producer : {e}")
