from flask import Flask, jsonify, request
import requests
import os
import queue
import random
import threading
import time
from collections import defaultdict
from sortedcontainers import SortedDict
from itertools import dropwhile
import re

import psycopg2
from psycopg2 import errorcodes
import queue

app = Flask(__name__)

print("shard manager is running!!!")
time.sleep(10)

class ConnectionPool:
    # def __init__(self, db_params, max_connections=5):
    def __init__(self,max_connections):
        # self.db_params = db_params
        self.max_connections = max_connections
        self.connection_pool = queue.Queue(max_connections)
        self.connected=False
        self._initialize_pool()

    def open_connection(self):
        db_connection=None
        try:
            db_connection = psycopg2.connect(
                host=db_name,
                user="postgres",
                password="abc",
                # database="distributed_database"
            )
        except Exception as e:
            print(f"Error connecting to database:{str(e)}",500)
            return False
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = 'distributed_database';")
        db_connection.commit()
        if cursor.fetchone() is None:
            db_connection.autocommit = True
            create_database_query="CREATE DATABASE distributed_database;"
            cursor.execute(create_database_query)
            db_connection.commit()
        
        cursor.close()
        db_connection.close()

        db_connection = psycopg2.connect(
            host=db_name,
            user="postgres",
            password="abc",
            database="distributed_database"
        )
        print("Connected to database")
        self.connection_pool.put(db_connection)
        return True

    def return_connection(self,db_connection):
        self.connection_pool.put(db_connection)

    def _initialize_pool(self):
        for _ in range(self.max_connections):
            opened=False
            while(not opened):
                opened=self.open_connection()
        self.connected=True

    def get_connection(self):
        return self.connection_pool.get()

    def connection_status(self):
        return self.connected

    def close_all_connections(self):
        while not self.connection_pool.empty():
            db_connection = self.connection_pool.get()
            db_connection.close()

db_name = os.environ.get('DBNAME')
connection_pool = ConnectionPool(max_connections=40)

db_connection=None
data_type_mapping = {
    'Number': 'INT',   # Example mapping for 'Number' to 'INT'
    'String': 'VARCHAR'  # Example mapping for 'String' to 'VARCHAR'
}


# max_retries = 5
# retry_delay = 5
# url = "http://shard_manager:5000/config"

# for attempt in range(max_retries):
#     try:
#         response = requests.post(url, json={})
#         if response.status_code == 200:
#             print("Request successful!")
#             break
#         else:
#             print(f"Request failed with status code {response.status_code}. Retrying...")
    
#     except requests.exceptions.ConnectionError as e:
#         # If a connection error occurs, log the error and retry after a delay
#         print(f"Connection error: {e}. Retrying...")
#     # Wait for the specified delay before retrying
#     time.sleep(retry_delay)

# print("Maximum number of retries reached. Unable to send request.")

# response = requests.post("http://shard_manager:5000/config", json={})
# print("Data inserted!!")

@app.route('/getSecondaryServers', methods=['POST'])
def get_secondary_servers():
    data = request.json
    shard_id = data.get('shard_id')

    response = requests.post(f"http://load_balancer:5000/readVariables", json=["valid_server_name"])
    if response.status_code == 200:
        response_data = response.json()
        # current_server_names=response_data["server_names"]
        valid_server_name=response_data["valid_server_name"]
    else:
        print(f"Error: {response.status_code}, {response.text}")

    # # servers_list = [valid_server_name[key] for key in current_server_names if key in valid_server_name]

    # # secondary_servers = servers_list[1:]

    query = f"SELECT Server_name FROM MapT WHERE Shard_id = '{shard_id}';"
    db_connection = connection_pool.get_connection()
    cursor = db_connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection_pool.return_connection(db_connection)

    secondary_servers = [row[0] for row in rows]
    secondary_servers_valid = [valid_server_name[key] for key in secondary_servers]

    # secondary_servers_valid = ["ab", "cd"]
    print(secondary_servers_valid[1:])

    response_data = {
        'servers': secondary_servers_valid[1:]
    }
    return jsonify(response_data), 200

@app.route('/getLogs', methods=['POST'])
def get_logs():
    data = request.json
    shard_id = data.get('shard_id')
    valid_idx_from = data.get('from')
    valid_idx_to = data.get('to')

    # extract primary server of shard_id
    response = requests.post(f"http://load_balancer:5000/readVariables", json=["valid_server_name"])
    if response.status_code == 200:
        response_data = response.json()
        # current_server_names=response_data["server_names"]
        valid_server_name=response_data["valid_server_name"]
    else:
        print(f"Error: {response.status_code}, {response.text}")
    query = f"SELECT Server_name FROM MapT WHERE Shard_id = '{shard_id}';"
    db_connection = connection_pool.get_connection()
    cursor = db_connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection_pool.return_connection(db_connection)
    secondary_servers = [row[0] for row in rows]
    primary_server = valid_server_name[secondary_servers[0]]
    # send request to it
    response=requests.post(f"http://{primary_server}:5000/getLogs",json={"shard_id": shard_id, "from":valid_idx_from, "to": valid_idx_to})
    # forward the request as response
    response_json = response.json()

    response_data = {
        'queries': response_json['queries']
    }
    return jsonify(response_data), 200

def heartbeat():
    
    print("heartbeat started") 
    
    while True:
        time.sleep(10)
        respawn_server_names=[]
        serv_to_shards={}

        current_server_names=[]
        valid_server_name={}
        server_name_to_shards={}
        server_name_to_number={}

        response = requests.post(f"http://load_balancer:5000/readVariables", json=["server_names","valid_server_name"])
        if response.status_code == 200:
            response_data = response.json()
            current_server_names=response_data["server_names"]
            valid_server_name=response_data["valid_server_name"]
        else:
            print(f"Error: {response.status_code}, {response.text}")


        # print("Going to enter for loop")

        for server_name in current_server_names:
            try:
                # print(f"Inside loop for server: {server_name}")
                response = requests.get(
                    f"http://{valid_server_name[server_name]}:5000/heartbeat")
                response.raise_for_status()
            except requests.RequestException:
                # print(f"In exception for server: {server_name}")
                removed_servers_copy=[]
                response = requests.post(f"http://load_balancer:5000/readVariables", json=["removed_servers"])
                if response.status_code == 200:
                    response_data = response.json()
                    removed_servers_copy=response_data["removed_servers"]
                else:
                    print(f"Error: {response.status_code}, {response.text}")

                if server_name not in removed_servers_copy:
                    respawn_server_names.append(server_name)
                    response = requests.post(f"http://load_balancer:5000/readVariables", json=["server_name_to_shards","server_name_to_number"])
                    if response.status_code == 200:
                        response_data = response.json()
                        server_name_to_shards=response_data["server_name_to_shards"]
                        server_name_to_number=response_data["server_name_to_number"]
                    else:
                        print(f"Error: {response.status_code}, {response.text}")

                    serv_to_shards[server_name]=server_name_to_shards[server_name]

                    
                    # del valid_server_name[server_name]
                    response = requests.post("http://load_balancer:5000/setVariables", json={"count":-1})
                    # clear metadata of killed server
                    response = requests.post("http://load_balancer:5000/removeFromList", json={"server_names":server_name,"suggested_random_server_id":server_name_to_number[server_name]})
                    
                    for current_shard in server_name_to_shards[server_name]:
                        query = f"DELETE FROM MapT WHERE Shard_id = '{current_shard}' AND Server_name = '{server_name}'"
                        db_connection = connection_pool.get_connection()
                        cursor = db_connection.cursor()
                        cursor.execute(query)
                        db_connection.commit()
                        cursor.close()  
                        connection_pool.return_connection(db_connection)
                        response = requests.post("http://load_balancer:5000/deleteFromDict", json={"shard_id_to_consistent_hashing": current_shard+","+server_name})
                    response = requests.post("http://load_balancer:5000/deleteFromDict", json={"server_name_to_shards": server_name})
            # print(f"Request done for server: {server_name}")

        if len(respawn_server_names)>0:
            servers_to_add = len(respawn_server_names)
            new_names=[]
            
            servers_dict={}
            for serv in respawn_server_names:
                num=random.randint(100000,999999)
                name=f"Server{num}"
                new_names.append(name)
                servers_dict[name]=serv_to_shards[serv]

            payload = {
                'n': servers_to_add,
                'new_shards':[],
                'servers' : servers_dict
            }
            # print("going to send payload")
            response = requests.post(
                "http://load_balancer:5000/add", json=payload)
            
            # shard_data={}
            # for serv in servers_dict.keys():
            #     for shrd in servers_dict[serv]:
            #         if shrd not in shard_data:
            #             for copy_serv in MapT[shrd]:
            #                 if copy_serv not in servers_dict:
            #                     payload={
            #                         "shards":[shrd]
            #                     }
            #                     response=requests.get(f"http://{valid_server_name[copy_serv]}:5000/copy",json=payload)
            #                     if response.status_code == 200:
            #                         response_json = response.json()
                                    
            #                         sh_list = response_json.get(shrd, [])
            #                         shard_data[shrd]=sh_list
            #                         break

            # for serv in servers_dict.keys():
            #     for shrd in servers_dict[serv]:
            #         if shrd in shard_data:
            #             payload={
            #                 "shard": shrd,
            #                 "curr_idx":0,
            #                 "data":shard_data[shrd]
            #             }
            #             response=requests.post(f"http://{valid_server_name[serv]}:5000/write",json=payload)
        
     
        current_server_names=[]
        removed_servers_copy=[]
        response = requests.post(f"http://load_balancer:5000/readVariables", json=["server_names","removed_servers"])
        if response.status_code == 200:
            response_data = response.json()
            removed_servers_copy=response_data["removed_servers"]
            current_server_names=response_data["server_names"]
        else:
            print(f"Error: {response.status_code}, {response.text}")

        for server_name in removed_servers_copy:
            if server_name not in current_server_names:
                response = requests.post("http://load_balancer:5000/removeFromList", json={"removed_servers":server_name})


# Create heartbeat thread
threading.Thread(target=heartbeat, daemon=True).start()

# Catch-all endpoint
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
def catch_all(path):
    response = {
        'error': 'Path not found',
        'message': f'The requested path "{path}" does not exist on this server.'
    }
    return jsonify(response), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)