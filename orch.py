

from flask import Flask, jsonify, request,render_template,redirect
import requests
import os
import sys
app = Flask(__name__)
template_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_folder)

from kafka import KafkaConsumer, KafkaProducer
import json
import threading
import uuid
import time

import sqlite3
import pandas as pd
#from flask_socketio import SocketIO

# Connect to SQLite database (or create a new one if it doesn't exist)
global conn
conn = sqlite3.connect('bd_3.db')

# Create a cursor object to execute SQL queries
# cursor = conn.cursor()

# # Create a table to store node metrics
# cursor.execute('''
#     CREATE TABLE IF NOT EXISTS node_metrics(
#         node_id TEXT PRIMARY KEY,
#         mean_latency REAL,
#         min_latency REAL,
#         max_latency REAL
#     )
# ''')

# # Sample metrics list
# # metrics_list = [
# #     {'node_id': '019232a8-c11b-42fb-b28b-ac373ef060fb', 'metrics': {'mean_latency': 33.4, 'min_latency': 25.1, 'max_latency': 40.0}},
# #     {'node_id': '12345678-1234-5678-1234-567812345678', 'metrics': {'mean_latency': 36.8, 'min_latency': 30.5, 'max_latency': 42.3}},
# #     # Add more metrics dictionaries as needed
# # ]
# metrics_list=[]
# # Insert data into the node_metrics table
# for metrics in metrics_list:
#     node_id = metrics['node_id']
#     mean_latency = metrics['metrics']['mean_latency']
#     min_latency = metrics['metrics']['min_latency']
#     max_latency = metrics['metrics']['max_latency']

#     cursor.execute('''
#         INSERT OR REPLACE INTO node_metrics (node_id, mean_latency, min_latency, max_latency)
#         VALUES (?, ?, ?, ?)
#     ''', (node_id, mean_latency, min_latency, max_latency))

# # Commit the changes and close the connection
# conn.commit()
# conn.close()

# # Query data from the node_metrics table and display as a DataFrame
# conn = sqlite3.connect('bd_3.db')
# df = pd.read_sql_query('SELECT * FROM node_metrics', conn, index_col='node_id')





global final_aggregate

# import asyncio
class OrchestratorNode:
    def __init__(self):
        self.is_consumer = True
        self.is_testconsumer =False
        self.drivers=0
        self.count=2
        self.num_requests=0
        self.test_type=""
        self.interval=0
        self.producer = KafkaProducer(bootstrap_servers=sys.argv[2])
        self.consumer = KafkaConsumer("register_topic", bootstrap_servers=sys.argv[2])
        self.consumer_testconfig=KafkaConsumer("test_config",bootstrap_servers=sys.argv[2])
        self.metrics_consumer = KafkaConsumer("metrics_topic", bootstrap_servers=sys.argv[2])
        self.registered_driver={}  #ip adress and node-id
        self.configtest={}
        self.aggregated_metrics_drivers={"mean_latency":0,"median_latency":0,"min_latency":0,"max_latency":0,"requests":0}
        self.driver_metrics={}
        self.dashboard={}
        self.trigger_dashboard=0
        self.total_req=0
    def send_message(self, topic, message):
        key_bytes = str(uuid.uuid4()).encode('utf-8')
        self.producer.send(topic, key=key_bytes, value=json.dumps(message).encode('utf-8'))
        self.producer.flush()

    def switch_to_producer(self,m):
        try:
            self.is_consumer = False
            self.consumer.close()
            print("Switched to producer mode")
            # Create a new KafkaConsumer instance
            self.consumer = KafkaConsumer("register_topic", bootstrap_servers=sys.argv[2])
            self.run_producer(m)
        except Exception as e:
            print(f"Error in switch_to_producer: {e}")
            raise e

    def run_consumer(self):
        for msg in self.consumer:
            message=''
            if msg is None:
                continue
            
            try:
                print(message)
                message = json.loads(msg.value.decode('utf-8'))
                print(message)
            except json.JSONDecodeError:
                print("Error decoding JSON:", msg.value())
                continue
            if message:
                self.drivers=self.drivers+1
                print("OIGJWOIJOE",self.drivers)
                self.switch_to_producer(message)
    def register_driver(self, node_id, node_ip):
        # if node_ip not in self.registered_driver:
        register_message = {
                "node_id": node_id,
                "node_ip": node_ip,
                "message_type": "DRIVER_NODE_REGISTER"
                }
        self.registered_driver[node_ip]={"node_id":node_id,"metrics":self.aggregated_metrics_drivers}
        print("HERE",node_id)
            # metrics_list.append({'node_id':node_id})
            # metrics_list.append({"node_id":node_id,'metrics': {'mean_latency': 0.0, 'min_latency': 0.0, 'max_latency': 0.0,'requests':0}})
        self.send_message("register_topic" ,register_message)   
    def run_producer(self,m):
        while not self.is_consumer:
            print("ORCH RUN PRODUCER")
            drive_nodeid=str(uuid.uuid4())
            print(drive_nodeid)
            print(m)
            # message = "bye"
            self.register_driver(drive_nodeid,m["node_ip"])
            # self.send_message("register_topic", message)
            # print(f"the message is {message}")
            self.is_consumer=True
            self.run_consumer()


    def configure_test(self, test_type, test_message_delay):
        test_config_message = {
            "test_id": str(uuid.uuid4()),
            "test_type": test_type,
            "test_message_delay": int(test_message_delay),
            "message_count_per_driver":int(self.num_requests)
        }
        print("##############################")
        print(self.test_type,1)
        print(test_config_message)
        self.configtest=test_config_message
        self.send_message("test_config", test_config_message)

    # def switch_to_testconfigproducer(self,m):
    #     try:
    #         self.is_testconsumer = False
    #         self.testconsumer.close()
    #         print("Switched to producer mode")
    #         # Create a new KafkaConsumer instance
    #         self.testconsumer = KafkaConsumer("test_config", bootstrap_servers=sys.argv[2]) #localhost:9092
    #         self.run_producer(m)
    #     except Exception as e:
    #         print(f"Error in switch_to_producer: {e}")
    #         raise e

    # def run_testconfigconsumer(self):
    #     for msg in self.testconsumer:
    #         message=''
    #         if msg is None:
    #             continue
            
    #         try:
    #             print(message)
    #             message = json.loads(msg.value.decode('utf-8'))
    #             print(message)
    #         except json.JSONDecodeError:
    #             print("Error decoding JSON:", msg.value())
    #             continue
    #         if message:
    #             self.switch_to_testconfigproducer(message)
   
    # def run_producer_testconfig(self,m):
    #     while not self.is_consumer:
    #         print("Test config RUN PRODUCER")
    #         testid=self.configtest["test_id"]
    #         print(testid)
    #         print(m)
    #         # message = "bye"
    #         self.configure_test(self.test_type,self.interval)
    #         # self.send_message("register_topic", message)
    #         # print(f"the message is {message}")
    #         self.is_consumer=True
    #         self.run_consumer()  
    # def check_drivers_present(self):
    #     # print(self.drivers)
    #     while self.drivers!=self.count:
    #         pass
    #     time.sleep(1)
    #     self.configure_test(self.test_type,self.interval)
    #     # if self.drivers==self.count:
    #     #     self.configure_test("AVALANCHE",2)
    #     #     return

    def consume_metrics(self):
        for msg in self.metrics_consumer:
            try:
                # print("PRINT BEFORE\n")
                metrics_message = json.loads(msg.value.decode('utf-8'))
                # print("PRINT BEFORE\n")
                self.process_metrics_message(metrics_message)
            except json.JSONDecodeError:
                print("Error decoding metrics JSON:", msg.value())

    def process_heartbeat(self, heartbeat_message):
        print("Received heartbeat:", heartbeat_message["node_id"])

    def run_heartbeat_consumer(self):
        heartbeat_consumer = KafkaConsumer("heartbeat_topic", bootstrap_servers=sys.argv[2])
        for msg in heartbeat_consumer:
            try:
                heartbeat_message = json.loads(msg.value.decode('utf-8'))
                self.process_heartbeat(heartbeat_message)
            except json.JSONDecodeError:
                print("Error decoding heartbeat JSON:", msg.value()) 

    def calculate_metrics(self, driver_id, latency):
        if driver_id in self.driver_metrics:
            metrics = self.driver_metrics[driver_id]
            metrics["total_latency"] = metrics["total_latency"]+ latency
            metrics["total_requests"] =  metrics["total_requests"]+1
            metrics["mean_latency"] = metrics["total_latency"] / metrics["total_requests"]
            metrics["min_latency"] = min(metrics["min_latency"], latency)
            metrics["max_latency"] = max(metrics["max_latency"], latency)

        else:
           
            self.driver_metrics[driver_id] = {
                "total_latency": latency,
                "total_requests": 1,
                "mean_latency": latency,
                "min_latency": latency,
                "max_latency": latency
            }
    def calculate_mean_of_means(self,means):
        return sum(means) / len(means)

# Function to calculate mode
    def calculate_mode(self,values):
        counts = {}
        for value in values:
            counts[value] = counts.get(value, 0) + 1
        max_count = max(counts.values())
        mode_values = [key for key, count in counts.items() if count == max_count]
        return mode_values
    

    def calc_final(self):
        
        mean_latencies = [metrics["metrics"]["mean_latency"] for metrics in self.registered_driver.values()]
        median_latencies = [metrics["metrics"]["median_latency"] for metrics in self.registered_driver.values()]
        min_latencies = [metrics["metrics"]["min_latency"] for metrics in self.registered_driver.values()]
        max_latencies = [metrics["metrics"]["max_latency"] for metrics in self.registered_driver.values()]

# Calculate min of mins, max of maxs, median of medians, mode of modes, and mean of means
        min_of_mins = min(min_latencies)
        max_of_maxs = max(max_latencies)
        median_of_medians = (sorted(median_latencies)[len(median_latencies) // 2 - 1] + \
                            sorted(median_latencies)[len(median_latencies) // 2]) / 2 if len(median_latencies) % 2 == 0 \
                            else sorted(median_latencies)[len(median_latencies) // 2]
        mode_of_modes = self.calculate_mode([round(metric, 2) for metric in median_latencies])
        mean_of_means = self.calculate_mean_of_means(mean_latencies)
        total_requests_list = [i.get("metrics", {}).get("no_requests", 0) for i in self.registered_driver.values()]
        total_requests=sum(total_requests_list)
        self.dashboard={"Min_all":min_of_mins,"Max_all":max_of_maxs,"Median_all":median_of_medians,"Mean_all":mean_of_means,"Mode":mode_of_modes,"total_requests":total_requests}
        print("DASHBOARD:\n",self.dashboard,"\n")
        
    def process_metrics_message(self, received_message):
        print(received_message)
        if received_message:

            node_id = received_message.get("node_id")
            nodeip=received_message.get("ip")
            metric = received_message.get("metrics")
            req=received_message.get("requests")
            print(nodeip,node_id)
            if nodeip is not None:
                # Check if the node_id already exists in the dictionary
                if nodeip in self.registered_driver:
                    # Update the existing entry with the new data
                    # final_aggregate=threading.Thread(target=self.calc_final)
                    if self.trigger_dashboard==0:
                        final_aggregate.start()
                        self.trigger_dashboard+=1
                    self.calc_final()
                    self.registered_driver[nodeip].update({"node_id":node_id,"metrics":metric})
                  
                else:
                    # Create a new entry for the node_id
                    self.registered_driver[nodeip] = {"node_id":node_id,"metrics":{}}
            else:
                print("Error: Node ID not found in metrics message.")
        
                self.registered_driver[nodeip]={"node_id":node_id,"metrics":self.aggregated_metrics_drivers}
                
                print("\n")
                # driver_id = message.get("node_id")

                # latency = float(message.get("latency", 0)) if "latency" in message else 1
            print("REGISTERED NODES:\n",self.registered_driver)
          
               
            # received_message = json.loads(message.value.decode('utf-8'))
        
            # cursor.execute('''
            #     INSERT OR REPLACE INTO node_metrics (node_id, mean_latency, min_latency, max_latency,requests)
            #     VALUES (?, ?, ?, ?,?)
            # ''', (received_message["node_id"], received_message["metrics"]["mean_latency"], received_message["metrics"]["min_latency"], received_message["metrics"]["max_latency"],received_message["requests"]))

            # registered_driver[driver_id]=message

            # cursor.execute('SELECT MIN(min_latency) FROM final_metrics')

            # # Fetch the result
            # min_value = cursor.fetchone()[0]
            # cursor.execute('SELECT MAX(max_latency) FROM final_metrics')

            # # Fetch the result
            # max_value = cursor.fetchone()[0]
            # cursor.execute('SELECT AVG(mean_latency) FROM final_metrics')

            # # Fetch the result
            # avg_value = cursor.fetchone()[0]

            # cursor.execute('SELECT SUM(requests) FROM final_metrics')

            # # Fetch the result
            # total_request = cursor.fetchone()[0]
            # df = pd.read_sql_query('SELECT * FROM final_metrics', conn, index_col='node_id')


            # self.aggregated_metrics_drivers={"mean_latency":avg_value,"median_latency":-1,"min_latency":min_value,"max_latency":max_value,"requests":total_request}
            # print(self.aggregated_metrics_drivers)
            # self.calculate_metrics(driver_id, latency)

    def run_trigger_producer(self, message):
        
        try:
            message["test_id"]=self.configtest["test_id"]
            self.send_message("trigger_topic", message)
        except Exception as e:
            print(f"Error in run_trigger_producer: {e}")
@app.route('/trigger', methods=['POST'])
def trigger():
    trigger_message = {"test_id":-1, "trigger": "YES"}
    orchestrator_node.run_trigger_producer(trigger_message)
    return 'Trigger initiated successfully!'
@app.route('/config_init', methods=['POST'])
def config_init():
    print(orchestrator_node.test_type)
    orchestrator_node.configure_test(orchestrator_node.test_type,orchestrator_node.interval)
    return 'Config Init sent!'

@app.route('/new', methods=['POST'])
def inputs_get():
    user_input = int(request.form['req_no'])
    time_int=int(request.form['interval'])
    type_test=request.form.get('testtype')
    num_drivers=int(request.form['num_drivers'])
    # Do something with the user_input, for example, print it
    orchestrator_node.test_type=type_test
    orchestrator_node.num_requests=user_input
    orchestrator_node.interval=time_int
    orchestrator_node.drivers=num_drivers
    # orchestrator_node.user_input=

    print(orchestrator_node.test_type,orchestrator_node.drivers)
    print(f"User input: {user_input} {time_int}  {type_test}")
    return redirect('http://127.0.0.1:5002')
    return "DONE"

    # consumer_thread.join()
    # producer_thread.join()
    # config_test_thread.join()
    # flask_thread.join()
    # metrics_thread.join()
    # heartbeat_consumer_thread.join()

@app.route('/dashboard')
#@socketio.on('dashboard')
def dashboard():
    return render_template('index_1.html', dashboard=orchestrator_node.dashboard, registered_driver=orchestrator_node.registered_driver)

@app.route('/')
def index():
   
    return render_template('/index.html')


    
# commnadline argws: node_ip   localhost:9092
if __name__ == "__main__":
    # orchestrator_node=OrchestratorNode()
    # Connect to SQLite database (or create a new one if it doesn't exist)
    # global conn
    # conn = sqlite3.connect('bd_3.db')

    # Create a cursor object to execute SQL queries
    global cursor
    cursor = conn.cursor()

    # Create a table to store node metrics
    # cursor.execute('''
    #     CREATE TABLE IF NOT EXISTS node_metrics(
    #         node_id TEXT PRIMARY KEY,
    #         mean_latency REAL,
    #         min_latency REAL,
    #         max_latency REAL
    #     )
    # ''')

    # Sample metrics list
    # metrics_list = [
    #     {'node_id': '019232a8-c11b-42fb-b28b-ac373ef060fb', 'metrics': {'mean_latency': 33.4, 'min_latency': 25.1, 'max_latency': 40.0}},
    #     {'node_id': '12345678-1234-5678-1234-567812345678', 'metrics': {'mean_latency': 36.8, 'min_latency': 30.5, 'max_latency': 42.3}},
    #     # Add more metrics dictionaries as needed
    # ]
    global metrics_list 
    metrics_list=[]
    # Insert data into the node_metrics table
    # for metrics in metrics_list:
    #     node_id = metrics['node_id']
    #     mean_latency = metrics['metrics']['mean_latency']
    #     min_latency = metrics['metrics']['min_latency']
    #     max_latency = metrics['metrics']['max_latency']

    #     cursor.execute('''
    #         INSERT OR REPLACE INTO node_metrics (node_id, mean_latency, min_latency, max_latency)
    #         VALUES (?, ?, ?, ?)
    #     ''', (node_id, mean_latency, min_latency, max_latency))

    # # Commit the changes and close the connection
    # conn.commit()
    # conn.close()
    global df
    # # Query data from the node_metrics table and display as a DataFrame
    # conn = sqlite3.connect('bd_3.db')
    # df = pd.read_sql_query('SELECT * FROM final_metrics', conn, index_col='node_id')




    global orchestrator_node
    orchestrator_node=OrchestratorNode()
    consumer_thread = threading.Thread(target=orchestrator_node.run_consumer)
    producer_thread = threading.Thread(target=orchestrator_node.run_producer({"node_id":-1,"node_ip":sys.argv[1], "message_type": "ORCH_ON"}
        ))
    # config_test_thread=threading.Thread(target=orchestrator_node.check_drivers_present)

    metrics_thread = threading.Thread(target=orchestrator_node.consume_metrics)
    flask_thread = threading.Thread(target=app.run, args=('localhost', 5002))
    heartbeat_consumer_thread = threading.Thread(target=orchestrator_node.run_heartbeat_consumer)
    final_aggregate=threading.Thread(target=orchestrator_node.calc_final)
    # final_aggregate.start()
    consumer_thread.start()
    producer_thread.start()
    # config_test_thread.start()
    metrics_thread.start()
    flask_thread.start()
    heartbeat_consumer_thread.start()
    app.run(debug=True)
  
    consumer_thread.join()
    producer_thread.join()
    # config_test_thread.join()
    flask_thread.join()
    metrics_thread.join()
    heartbeat_consumer_thread.join()
    final_aggregate.join()
 


    
