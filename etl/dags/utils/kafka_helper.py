from kafka import KafkaProducer, KafkaConsumer
import json


class KafkaProducerHelper:
    def __init__(self, bootstrap_servers='kafka:9092', topic='f1.dim.drivers'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )

    def send_all(self, rows, key_field=None):
        """
        Send list of dicts to Kafka topic.
        key_field: optional field name to use as Kafka message key (for partitioning).
        """
        for row in rows:
            key = str(row[key_field]) if key_field and key_field in row else None
            self.producer.send(self.topic, value=row, key=key)
        self.producer.flush()


class KafkaConsumerHelper:
    """
    Batch Kafka consumer with manual offset commit.

    Reads all available messages from subscribed topics.
    After processing, call commit() to mark messages as consumed.
    Next run of the same consumer group will start from the committed offset,
    so only new messages are read (incremental behaviour).
    """

    def __init__(self, bootstrap_servers='kafka:9092', topics=None,
                 group_id='f1-incremental-consumers', consumer_timeout_ms=15000):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',   # first run: read from beginning
            enable_auto_commit=False,        # manual commit after processing
            consumer_timeout_ms=consumer_timeout_ms  # stop when no new messages
        )

    def consume_batch(self):
        """
        Read all available messages from subscribed topics.
        Returns dict: { topic_name: [record_dict, ...] }
        Stops after consumer_timeout_ms milliseconds with no new messages.
        """
        messages = {}
        for message in self.consumer:
            topic = message.topic
            if topic not in messages:
                messages[topic] = []
            messages[topic].append(message.value)
        return messages

    def commit(self):
        """Commit offsets - next run will start from here."""
        self.consumer.commit()

    def close(self):
        self.consumer.close()
