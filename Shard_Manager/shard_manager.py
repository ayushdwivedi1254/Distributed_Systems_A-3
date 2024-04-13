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


app = Flask(__name__)

print("shard manager is running!!!")

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
                        response = requests.post("http://load_balancer:5000/removeFromList", json={"MapT":current_shard+","+server_name})
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



if __name__ == '__main__':
    app.run()