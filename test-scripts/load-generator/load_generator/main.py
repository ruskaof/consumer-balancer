import os
from kafka import KafkaProducer
import random
import threading
import time
from datetime import datetime, timedelta

from .prometheus_client import PrometheusClient
from .plotting import plot_test_results

BOOSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "test-topic")
PARTITIONS_COUNT = int(os.getenv("PARTITIONS_COUNT", "32"))
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "localhost:9090")
GRAPH_OUTPUT_DIR = os.getenv("GRAPH_OUTPUT_DIR", "test-out")
producer = KafkaProducer(bootstrap_servers=BOOSTRAP_SERVERS)
HIGH_TRAFFIC_PARTITIONS_COUNT = PARTITIONS_COUNT // 5
MEDIUM_TRAFFIC_PARTITIONS_COUNT = PARTITIONS_COUNT // 3
LOW_TRAFFIC_PARTITION_COUNT = PARTITIONS_COUNT - \
    HIGH_TRAFFIC_PARTITIONS_COUNT - MEDIUM_TRAFFIC_PARTITIONS_COUNT
ITERATION_DURATION_SECONDS = 300
ITERATIONS_COUNT = 2

prometheus_client = PrometheusClient(PROMETHEUS_URL)


def run_test_iteration():
    partitions = set(range(PARTITIONS_COUNT))
    high_traffic_partitions = set(random.sample(
        list(partitions), HIGH_TRAFFIC_PARTITIONS_COUNT))
    medium_traffic_partitions = set(random.sample(
        list(partitions - high_traffic_partitions), MEDIUM_TRAFFIC_PARTITIONS_COUNT))
    low_traffic_partitions = partitions - \
        medium_traffic_partitions - high_traffic_partitions

    threads: list[threading.Thread] = []
    threads.append(threading.Thread(target=generate_data, kwargs={
                   "topic": TOPIC,
                   "partitions": high_traffic_partitions,
                   "frequency_seconds": 0.05,
                   "duration_seconds": ITERATION_DURATION_SECONDS}))
    threads.append(threading.Thread(target=generate_data, kwargs={
                   "topic": TOPIC,
                   "partitions": medium_traffic_partitions,
                   "frequency_seconds": 0.2,
                   "duration_seconds": ITERATION_DURATION_SECONDS}))
    threads.append(threading.Thread(target=generate_data, kwargs={
                   "topic": TOPIC,
                   "partitions": low_traffic_partitions,
                   "frequency_seconds": 0.5,
                   "duration_seconds": ITERATION_DURATION_SECONDS}))

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def generate_data(topic, partitions, frequency_seconds, duration_seconds):
    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        for partition in partitions:
            producer.send(topic, value="test".encode(), partition=partition)
        time.sleep(frequency_seconds)
    producer.flush()


def plot_prometheus_cpu_usage():
    end_time = datetime.now()
    start_time = end_time - \
        timedelta(seconds=ITERATIONS_COUNT*ITERATION_DURATION_SECONDS - 10)

    end_ts = end_time.timestamp()
    start_ts = start_time.timestamp()

    eps_by_job = {
        "listener-default": prometheus_client.get_record_consumed_rates("listener-default", start_ts, end_ts),
        "listener-balanced": prometheus_client.get_record_consumed_rates("listener-balanced", start_ts, end_ts),
    }
    rebalance_timestamps = prometheus_client.get_rebalance_timestamps("listener-default", start_ts, end_ts)
    plot_test_results(GRAPH_OUTPUT_DIR, eps_by_job, rebalance_timestamps)


if __name__ == "__main__":
    for i in range(ITERATIONS_COUNT):
        print(f"Running iteration {i+1}/{ITERATIONS_COUNT}")
        run_test_iteration()
    plot_prometheus_cpu_usage()
    producer.close()
