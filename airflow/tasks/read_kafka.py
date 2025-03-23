from airflow.decorators import task, branch_task
from kafka import KafkaConsumer, TopicPartition
import json
from datetime import timedelta, datetime
import time
from kafka.errors import KafkaError
from airflow.models import TaskInstance, DagRun

@task
def read_kafka_logs(topic: str, broker: str, minutes: int, backup_broker=None):
    """Reads Kafka logs from a specific time range.

    Args:
        topic (str): The Kafka topic to be subscribed to.
        broker (str): The default broker to be read from.
        minutes (int): The number of minutes to go back in time.
        output_dict (dictionary): The dictionary that the returned logs are saved to.
        backup_broker: An optional backup broker for fault tolerance.
    """
    
    brokers = [broker] if backup_broker is None else [broker, backup_broker]
    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        auto_offset_reset="latest", 
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        print(f"Topic '{topic}' does not exist.")
        return []

    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    timestamp = int((datetime.utcnow() - timedelta(minutes=minutes)).timestamp() * 1000)
    offsets = consumer.offsets_for_times({tp: timestamp for tp in topic_partitions})

    for tp, offset_data in offsets.items():
        if offset_data:
            consumer.assign([tp])
            consumer.seek(tp, offset_data.offset)

    logs = []

    start_time = time.time()
    while time.time() - start_time < 10: 
        raw_messages = consumer.poll(timeout_ms=1000)
        for tp, messages in raw_messages.items():
            for message in messages:
                message.value["source"] = topic
                message.value["aggregation_period"] = minutes
                logs.append(message.value)

    consumer.close()
    return logs

@task.branch
def select_extract_tasks(topic: str, **kwargs):
    """Chooses which of the aggregation tasks to run: The last 10, 60 minutes of 24 hour option.
    
    Args:
        topic (string): The topic that should be used to determine the task ids to call.
        kwargs: Airflow's context dictionary.
    """
    dag_id = kwargs["dag"].dag_id
    num_runs = len(DagRun.find(dag_id=dag_id))
    if num_runs % 144 == 0:
        return [f"Extract_Data.Extract_{topic}_Data.read_logs_back_one_day",
                f"Extract_Data.Extract_{topic}_Data.read_logs_back_one_hour",
                f"Extract_Data.Extract_{topic}_Data.read_logs_back_ten_minutes"]
    elif num_runs % 6 == 0:
        return [f"Extract_Data.Extract_{topic}_Data.read_logs_back_one_hour",
                f"Extract_Data.Extract_{topic}_Data.read_logs_back_ten_minutes"]
    else: 
        return f"Extract_Data.Extract_{topic}_Data.read_logs_back_ten_minutes"
    