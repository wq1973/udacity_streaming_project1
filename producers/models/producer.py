"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer


logger = logging.getLogger(__name__)

# localhost
BOOTSTRAP_SERVERS = "PLAINTEXT://kafka0:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081/"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "bootstrap-server": "PLAINTEXT://localhost:9092",
            "broker_id": 0,
            "schema.registry.url": "http://localhost:8081",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.avro.AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

    def topic_exsists(self, client, topic_name):
        """Checks if the given topic exists"""
        # ref: https://skuchkula.github.io/managing-kafka-topics/#method-1-use-confluent_kafka--confluents-python-client-for-apache-kafka
        topic_metadata = client.list_topics(timeout=5)
        for t in topic_metadata.topics.values():
            if t.topic == self.topic_name:
                return True
        return False

    def list_all_topics(self, client):
        """Lists all the available topics"""
        topic_metadata = client.list_topics(timeout=5)
        for t in topic_metadata.topics.values():
            print(t.topic)

    def create_topic(self, client, topic_name):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.

        client = AdminClient(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
        )

        # list all available topics
        list_all_topics(client)

        if topic_exsists is True:
            logger.info(f"Topic {self.topic_name} exsists. Will not create")
            return

        if topic_exsists is False:

            logger.info(f"Creating topic: {self.topic_name}")

            futures = client.create_topics(
                [
                    NewTopic(
                        topic=topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas,
                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    print("topic created")
                except Exception as e:
                    logger.info("topic creation kafka integration incomplete", topic, e)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        # TODO: Write cleanup code for the Producer here
        if self.producer is None:
            return

        logger.info("producer close incomplete - skipping")
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
