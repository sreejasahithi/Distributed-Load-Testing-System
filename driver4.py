from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading
import uuid
import requests
import sys
# from shared_variable import responses_sent

bootstrap_servers = sys.argv[2]

class DriverNode:
    def __init__(self):
        self.node_id = -1
        self.nodeip = sys.argv[1]
        self.message_request = 10
        self.is_producer = True
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.consumer = KafkaConsumer("register_topic", bootstrap_servers=bootstrap_servers)
        self.test_consumer = KafkaConsumer("test_config", bootstrap_servers=bootstrap_servers)
        self.trigger_consumer = KafkaConsumer("trigger_topic", group_id="driver_group_4", bootstrap_servers=bootstrap_servers)
        self.metrics={}
        self.config = {}
        self.triggermessage = {}
        self.delay = 0
        self.interval = 1  # Set the default interval to 1 second
        self.terminate_threads = False

    def send_message(self, topic, message):
        self.producer.send(topic, value=message.encode('utf-8'))

    def switch_to_consumer(self):
        self.is_producer = False
        self.producer.close()
        print("Switched to consumer mode")
        # Create a new KafkaProducer instance
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def run_producer(self):
        message_to_send = {"node_id": -1, "node_ip": self.nodeip, "message_type": "DRIVER_NODE_REGISTER"}
        self.send_message("register_topic", json.dumps(message_to_send))
        print("Message sent:", message_to_send)
        time.sleep(1)
        self.switch_to_consumer()

    def run_consumer(self):
        for msg in self.consumer:
            message = ''
            if msg is None:
                continue

            try:
                message = json.loads(msg.value.decode('utf-8'))
            except json.JSONDecodeError:
                print("Error decoding JSON:", msg.value())
                continue
            if message:
                if message["node_ip"] == self.nodeip:
                    print(message)
                    self.node_id = message["node_id"]
                    self.node_ip = message["node_ip"]
                    self.consumer.close()
                    print(self.node_id)

    def run_configconsumer(self):
        for msg in self.test_consumer:
            message = ''
            if msg is None:
                continue

            try:
                message = json.loads(msg.value.decode('utf-8'))
            except json.JSONDecodeError:
                print("Error decoding JSON:", msg.value())
                continue
            if message:
                self.config = message
                if message["test_type"] == "AVALANCHE":
                    num_message = message["message_count_per_driver"]
                    self.message_request = num_message
                    print(message)
                    # Code for avalanche from driver to server
                elif message["test_type"] == "TSUNAMI":
                    num_message = message["message_count_per_driver"]
                    self.message_request = num_message
                    self.delay = message["test_message_delay"]
                    print(message)
                    # Code for Tsunami from driver to server

    def process_trigger_message(self, trigger_message):
        print("Received Trigger:", trigger_message)

    def run_trigger_consumer(self):
        semaphore = threading.Semaphore()
        for msg in self.trigger_consumer:
            message = ''
            if msg is None:
                continue
            received_message = msg.value.decode('utf-8')
            print("Received (Trigger):", received_message)

            if received_message:
                self.triggermessage = json.loads(received_message)
                try:
                    # Start a new thread for sending metrics periodically
                    metrics_thread = threading.Thread(target=self.send_metrics_periodically)
                    metrics_thread.start()

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

    def send_metrics_periodically(self):
        j=0
        try:
                latencies = []
                for i in range(self.message_request):
                
                    url = "http://localhost:8000/ping"
                    start_time = time.time()  # Record the start time
                    response = requests.get(url)
                    end_time = time.time()  # Record the end time

                    # Update shared_variable
                    # self.increment_responses_sent()

                    latency = (end_time - start_time) * 1000  # Convert to milliseconds
                    print(response, latency)
                    latencies.append(latency)
                    

                    if latencies:
                        metrics = {
                            "mean_latency": sum(latencies) / len(latencies),
                            "median_latency": (sorted(latencies)[len(latencies) // 2 - 1] +
                                            sorted(latencies)[len(latencies) // 2]) / 2 if len(
                                latencies) % 2 == 0
                                            else sorted(latencies)[len(latencies) // 2],
                            "min_latency": min(latencies),
                            "max_latency": max(latencies),
                            "no_requests":i+1
                            
                        }

                        metrics_message = {
                            "node_id": self.node_id,
                            "test_id": self.triggermessage["test_id"],
                            "interval_id": str(uuid.uuid4()),  # Generate a unique ID for each interval
                            "metrics": metrics,
                            "ip":sys.argv[1]
                        }
                        j+=1
                        self.metrics=metrics_message
                        # Send metrics for every interval
                        if (i==0):
                            print(i,"(((((((((((((((((((((((())))))))))))))))))))))))")
                            # self.send_message("metrics_topic", json.dumps(metrics_message))
                            run_to_orch=threading.Thread(target=driver_node.start_send)
                            run_to_orch.start()
                        time.sleep(self.delay)

        except Exception as e:
                print(f"Error sending metrics: {e}")

            
            # time.sleep(self.interval * self.message_request)
    def start_send(self):

        print("HERE IN SEND ORCH")
        j=0
        while True:
            self.send_message("metrics_topic",json.dumps(self.metrics))
            time.sleep(1)
    def send_to_orch(self):
        print("HERE IN SEND ORCH")
        j=0
        while True:
            self.send_message("metrics_topic",json.dumps(self.metrics))
            time.sleep(2)
        # self.send_message("metrics_topic",json.dumps(self.metrics))
        # send_to_orch.start()
    def send_heartbeat(self):
        while True:
            heartbeat_message = {
                "node_id": self.node_id,
                "heartbeat": "YES"
            }

            try:
                # Check if the producer is closed
                if self.producer is None or self.producer._closed:
                    # If closed, create a new KafkaProducer instance
                    self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

                # Send the heartbeat message
                self.producer.send("heartbeat_topic", value=json.dumps(heartbeat_message).encode('utf-8'))

            except Exception as e:
                print(f"Error sending heartbeat message: {e}")

            time.sleep(3)  # Adjust the interval as needed

    def run_heartbeat_producer(self):
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.start()

if __name__ == "__main__":
    driver_node = DriverNode()

    producer_thread = threading.Thread(target=driver_node.run_producer)
    consumer_thread = threading.Thread(target=driver_node.run_consumer)
    consumer_config_thread = threading.Thread(target=driver_node.run_configconsumer)
    trigger_consumer_thread = threading.Thread(target=driver_node.run_trigger_consumer)
    heartbeat_producer_thread = threading.Thread(target=driver_node.run_heartbeat_producer)
    start_send=threading.Thread(target=driver_node.start_send)
    producer_thread.start()
    consumer_thread.start()
    consumer_config_thread.start()
    trigger_consumer_thread.start()
    heartbeat_producer_thread.start()
    start_send.start()

    # try:
    #     # ... (existing code)
    #     while True:
    #         time.sleep(1)  # Adjust as needed
    # except KeyboardInterrupt:
    #     print("KeyboardInterrupt: Terminating threads...")
    #     driver_node.terminate_threads = True

    producer_thread.join()
    consumer_thread.join()
    consumer_config_thread.join()
    trigger_consumer_thread.join()
    heartbeat_producer_thread.join()
    start_send.join()
    run_to_orch.join()
    # metrics_thread.join()

