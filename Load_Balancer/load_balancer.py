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
from ConsistentHashing import ConsistentHashing

app = Flask(__name__)

# backend_server = "http://server1:5000"
# N = int(os.environ.get('COUNT', ''))
N = 0
# server_names = os.environ.get('SERVER_NAMES', '')
count = N
# Split the string into a list using the delimiter
# server_names = server_names.split(',')
server_names = []

# Shared queue for incoming read requests
read_request_queue = queue.Queue()

server_name_to_number = {}
# Consistent hashing object
M = 512
K = 9
consistent_hashing = ConsistentHashing(count, M, K)

# Extra data structures for Assignment-2
MapT = defaultdict(set)
ShardT = SortedDict()
schema = {}
shards = {}
server_name_to_shards = {}
valid_server_name={}
shard_id_to_consistent_hashing = {}
shard_id_to_consistent_hashing_lock = {}
shard_id_to_write_request_lock = {}
shard_id_to_write_request_queue = {}
shard_id_to_write_thread = {}
shard_id_to_update_request_queue = {}
shard_id_to_update_thread = {}
shard_id_to_delete_request_queue = {}
shard_id_to_delete_thread = {}
suggested_random_server_id = []
suggested_random_server_id_lock = threading.Lock()
removed_servers = []
removed_servers_lock = threading.Lock()

lock = threading.Lock()
server_name_lock = threading.Lock()

seed_value = int(time.time())
random.seed(seed_value)

################################## UTILITY FUNCTIONS ###########################################

# Function to convert a list of integers to the desired string format
def convert_to_string(integers):
    if len(integers) == 1:
        return f"Add Server:{integers[0]}"
    elif len(integers) == 2:
        return f"Add Server:{integers[0]} and Server:{integers[1]}"
    else:
        return "Add " + ", ".join(f"Server:{x}" for x in integers[:-1]) + f", and Server:{integers[-1]}"


# Function to find the key value of ordered_map which is less than or equal to num, giving priority to lesser value if exists
def lower_bound_entry(ordered_map, num):
    keys = list(ordered_map.keys())
    left, right = 0, len(keys)
    if right == 0:
        return -1
    if keys[0] >= num:
        return keys[0]
    while right - left > 1:
        mid = (left + right) // 2
        if keys[mid] < num:
            left = mid
        else:
            right = mid
    return keys[left]

def is_valid_docker_name(name):
    pattern = r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,253}$"
    return re.match(pattern, name) is not None and len(name) <= 255

def generate_id(hostname):
    global suggested_random_server_id
    global suggested_random_server_id_lock

    # Regular expression to match hostname of the form Server{some_number} or server{some_number}
    pattern = re.compile(r'^Server(\d+)$', re.IGNORECASE)

    match = pattern.match(hostname)
    if match:
        some_number = int(match.group(1))
        with suggested_random_server_id_lock:
            if some_number not in suggested_random_server_id:
                suggested_random_server_id.append(some_number)
                return some_number

    # Generate a random id if the hostname doesn't match the pattern or some_number is already in the id_list
    while True:
        with suggested_random_server_id_lock:
            new_id = random.randint(100000, 999999)
            if new_id not in suggested_random_server_id:
                suggested_random_server_id.append(new_id)
                return new_id
################################################################################################


def read_worker(thread_number):
    global count
    global server_names
    global server_name_lock
    global lock
    global consistent_hashing
    global shard_id_to_consistent_hashing
    global shard_id_to_consistent_hashing_lock

    while True:
        # Get a request from the queue
        request_data = read_request_queue.get()

        reqID = request_data["id"]
        shardID = request_data["shard_id"]
        low_stud_id = request_data["low_stud_id"]
        high_stud_id = request_data["high_stud_id"]

        while True:
            # with lock:
            #     serverName = consistent_hashing.allocate(reqID)
            with shard_id_to_consistent_hashing_lock[shardID]:
                serverName = shard_id_to_consistent_hashing[shardID].allocate(reqID)

            try:
                # Send the request to the backend server
                read_payload = {
                    "shard": shardID,
                    "Stud_id": {"low":low_stud_id, "high":high_stud_id}
                }
                url = f'http://{valid_server_name[serverName]}:5000/read'
                response = requests.post(url, json=read_payload)
                break
            except requests.RequestException as e:
                pass

        response_data = response.json()

        # Send the response back to the client
        request_data['response_queue'].put({
            'status_code': response.status_code,
            'data': response_data['data'],
            'thread_number': thread_number,
            'server_name': serverName,
            'reqID': id
        })

        # Mark the task as done
        read_request_queue.task_done()


# Function to send write request to a server
def send_write_request(server_name, payload, write_responses, error_message, failed_entries):
    url = f'http://{valid_server_name[server_name]}:5000/write'
    try:
        response = requests.post(url, json=payload)
        #return response.json(), 200
        response_payload = response.json()
        if response.status_code != 200:
            error_message.append(response_payload['message'])
            if len(failed_entries) == 0:
                failed_entries.extend(response_payload['failed_entries'])
        write_responses.append(response.status_code)
        # write_responses.append(200)
    except Exception as e:
        # return {"error": str(e)}, 500  # Return error message and status code 500 for server error
        write_responses.append(500)
        error_message.append(str(e))
        if len(failed_entries) == 0:
            failed_entries.extend(payload['data'])


def write_worker(current_shard_id):
    global count
    global server_names
    global server_name_lock
    global lock
    global shards
    global ShardT
    global MapT
    global shard_id_to_write_request_lock
    global shard_id_to_write_request_queue

    while True:
        # Get a request from the queue
        request_data = shard_id_to_write_request_queue[current_shard_id].get()

        num_entries = request_data['num_entries']

        write_payload = {
            "shard": current_shard_id,
            "curr_idx": ShardT[shards[current_shard_id]['Stud_id_low']]['valid_idx'],
            "data": request_data['data_entries']
        }

        write_responses = []
        error_message = []
        failed_entries = []

        with shard_id_to_write_request_lock[current_shard_id]:
            server_names_list = MapT[current_shard_id]
            threads = []
            for server_name in server_names_list:
                thread = threading.Thread(target=send_write_request, args=(server_name, write_payload, write_responses, error_message, failed_entries))
                thread.start()
                threads.append(thread)

            writes_successful = True
            error_status_code = 200
            
            # Wait for all threads to complete and collect the responses
            for thread in threads:
                thread.join()
            for response in write_responses:
                if response != 200:
                    writes_successful = False
                    error_status_code = response
            
            if writes_successful:
                request_data['response_queue'].put({
                    'status_code': error_status_code,
                    'message': "Writes successful",
                    'responses': write_responses,
                    'failed_entries': failed_entries
                })
                ShardT[shards[current_shard_id]['Stud_id_low']]['valid_idx'] = ShardT[shards[current_shard_id]['Stud_id_low']]['valid_idx'] + num_entries
            else:
                request_data['response_queue'].put({
                    'status_code': error_status_code,
                    'message': error_message[0],
                    'responses': write_responses,
                    'failed_entries': failed_entries
                })

        # Mark the task as done
        shard_id_to_write_request_queue[current_shard_id].task_done()


# Function to send update request to a server
def send_update_request(server_name, payload, update_responses, error_message):
    url = f'http://{valid_server_name[server_name]}:5000/update'
    try:
        response = requests.put(url, json=payload)
        #return response.json(), 200
        response_payload = response.json()
        if response.status_code != 200:
            error_message.append(response_payload['message'])
        update_responses.append(response.status_code)
    except Exception as e:
        # return {"error": str(e)}, 500  # Return error message and status code 500 for server error
        update_responses.append(500)
        error_message.append(str(e))

def update_worker(current_shard_id):
    global MapT
    global shard_id_to_write_request_lock
    global shard_id_to_update_request_queue

    while True:
        # Get a request from the queue
        request_data = shard_id_to_update_request_queue[current_shard_id].get()

        update_payload = {
            "shard": current_shard_id,
            "Stud_id": request_data['Stud_id'],
            "data": request_data['data_entry']
        }

        update_responses = []
        error_message = []

        with shard_id_to_write_request_lock[current_shard_id]:
            server_names_list = MapT[current_shard_id]
            threads = []
            for server_name in server_names_list:
                thread = threading.Thread(target=send_update_request, args=(server_name, update_payload, update_responses, error_message))
                thread.start()
                threads.append(thread)

            updates_successful = True
            error_status_code = 200
            
            # Wait for all threads to complete and collect the responses
            for thread in threads:
                thread.join()
            for response in update_responses:
                if response != 200:
                    updates_successful = False
                    error_status_code = response

            if updates_successful:
                request_data['response_queue'].put({
                    'status_code': error_status_code,
                    'message': "Updates successful",
                    'responses': update_responses
                })
            else:
                request_data['response_queue'].put({
                    'status_code': error_status_code,
                    'message': error_message[0],
                    'responses': update_responses
                })

        # Mark the task as done
        shard_id_to_update_request_queue[current_shard_id].task_done()


# Function to send delete request to a server
def send_delete_request(server_name, payload, delete_responses, error_message):
    url = f'http://{valid_server_name[server_name]}:5000/del'
    try:
        response = requests.delete(url, json=payload)
        #return response.json(), 200
        response_payload = response.json()
        if response.status_code != 200:
            error_message.append(response_payload['message'])
        delete_responses.append(response.status_code)
        # delete_responses.append(response.json())
    except Exception as e:
        # return {"error": str(e)}, 500  # Return error message and status code 500 for server error
        delete_responses.append(500)
        error_message.append(str(e))
        # delete_responses.append(response.json())

def delete_worker(current_shard_id):
    global MapT
    global shard_id_to_write_request_lock
    global shard_id_to_delete_request_queue

    while True:
        # Get a request from the queue
        request_data = shard_id_to_delete_request_queue[current_shard_id].get()

        delete_payload = {
            "shard": current_shard_id,
            "Stud_id": request_data['Stud_id']
        }

        delete_responses = []
        error_message = []

        with shard_id_to_write_request_lock[current_shard_id]:
            server_names_list = MapT[current_shard_id]
            threads = []
            for server_name in server_names_list:
                thread = threading.Thread(target=send_delete_request, args=(server_name, delete_payload, delete_responses, error_message))
                thread.start()
                threads.append(thread)

            deletes_successful = True
            error_status_code = 200
            
            # Wait for all threads to complete and collect the responses
            for thread in threads:
                thread.join()
            for response in delete_responses:
                if response != 200:
                    deletes_successful = False
                    error_status_code = response

            if deletes_successful:
                request_data['response_queue'].put({
                    'status_code': error_status_code,
                    'message': "Updates successful",
                    'responses': delete_responses
                })
                ShardT[shards[current_shard_id]['Stud_id_low']]['valid_idx'] = ShardT[shards[current_shard_id]['Stud_id_low']]['valid_idx'] - 1
            else:
                request_data['response_queue'].put({
                    'status_code': error_status_code,
                    'message': error_message[0],
                    'responses': delete_responses
                })

        # Mark the task as done
        shard_id_to_delete_request_queue[current_shard_id].task_done()


def heartbeat():
    global count
    global server_names
    global server_name_to_number
    global server_name_lock
    global lock
    global consistent_hashing
    global valid_server_name
    global removed_servers
    global removed_servers_lock
    print("heartbeat started")
    while True:
        time.sleep(0.5)
        respawn_server_names=[]
        serv_to_shards={}

        with server_name_lock:
            current_server_names = server_names.copy()

        # print("Going to enter for loop")

        for server_name in current_server_names:
            try:
                # print(f"Inside loop for server: {server_name}")
                response = requests.get(
                    f"http://{valid_server_name[server_name]}:5000/heartbeat")
                response.raise_for_status()
            except requests.RequestException:
                # print(f"In exception for server: {server_name}")
                with removed_servers_lock:
                    removed_servers_copy = removed_servers.copy()
                with server_name_lock:
                    if server_name not in removed_servers_copy:
                        respawn_server_names.append(server_name)
                        serv_to_shards[server_name]=server_name_to_shards[server_name]
                        server_names.remove(server_name)
                        # del valid_server_name[server_name]
                        count -= 1
                        # clear metadata of killed server
                        with suggested_random_server_id_lock:
                            suggested_random_server_id.remove(server_name_to_number[server_name])
                        for current_shard in server_name_to_shards[server_name]:
                            MapT[current_shard].discard(server_name)
                            with shard_id_to_consistent_hashing_lock[current_shard]:
                                shard_id_to_consistent_hashing[current_shard].remove_server(server_name_to_number[server_name], server_name)
                        del server_name_to_shards[server_name]
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
        
        with server_name_lock:
            current_server_names = server_names.copy()
        with removed_servers_lock:
            removed_servers_copy = removed_servers.copy()

        for server_name in removed_servers_copy:
            if server_name not in current_server_names:
                with removed_servers_lock:
                    removed_servers.remove(server_name)


# Create read_worker threads
num_read_workers = 100
for _ in range(num_read_workers):
    threading.Thread(target=read_worker, args=(_,), daemon=True).start()

threading.Thread(target=heartbeat, daemon=True).start()

@app.route('/init', methods=['POST'])
def initialize_database():
    global schema
    # global N
    payload = request.json

    # Processing the payload
    N = payload.get('N', 0)
    schema = payload.get('schema', {})
    shards_list = payload.get('shards', [])
    servers = payload.get('servers', {})

    # Populating servers with empty shard list with at most 3 shards
    shard_allotments = {}
    for shard_id, server_ids in MapT.items():
        shard_allotments[shard_id] = len(server_ids)
    for shard in shards_list:
        shard_allotments[shard['Shard_id']] = 0
    for server, shards in servers.items():
        for shard_id in shards:
            shard_allotments[shard_id] += 1
    empty_servers = [server for server, shards in servers.items() if not shards]

    for empty_server in empty_servers:
        for _ in range(0, 3):
            shard_allotted = ""
            min_count = 1000000
            for shard_id, count in shard_allotments.items():
                if count <= min_count and shard_id not in servers[empty_server]:
                    min_count = count
                    shard_allotted = shard_id
            if min_count != 1000000:
                servers[empty_server].append(shard_allotted)
                shard_allotments[shard_allotted] += 1

    add_endpoint_payload = {
        "n": N,
        "new_shards": shards_list,
        "servers": servers
    }

    add_response = requests.post('http://load_balancer:5000/add', json=add_endpoint_payload)

    # Check if the response from /add contains an error
    if add_response.status_code != 200:
        return jsonify(add_response.json()), add_response.status_code     
    
    # Responding with success message and status code 200
    response = {
        "message": "Configured Database",
        "status": "success"
    }
    return jsonify(response), 200

@app.route('/status', methods=['GET'])
def get_status():
    global server_name_lock
    global count
    global schema
    global shards
    global server_name_to_shards

    with server_name_lock:
        count_copy = count
    
    shards_list = [{"Stud_id_low": details["Stud_id_low"], "Shard_id": shard_id, "Shard_size": details["Shard_size"]} for shard_id, details in shards.items()]
    
    # Construct the response JSON
    response = {
        "N": count_copy,
        "schema": schema,
        "shards": shards_list,
        "servers": server_name_to_shards
    }
    
    return jsonify(response), 200


@app.route('/add', methods=['POST'])
def add_server():

    global count
    global server_names
    global server_name_to_number
    global server_name_lock
    global lock
    global shards
    global schema
    global MapT
    global server_name_to_shards
    global ShardT
    global shard_id_to_consistent_hashing
    global shard_id_to_consistent_hashing_lock
    global shard_id_to_write_request_queue
    global shard_id_to_write_request_lock
    global shard_id_to_write_thread
    global shard_id_to_update_request_queue
    global shard_id_to_update_thread
    global shard_id_to_delete_request_queue
    global shard_id_to_delete_thread
    global valid_server_name

    payload = request.json

    n = payload.get('n')
    new_shards = payload.get('new_shards', [])
    servers = payload.get('servers', {})
    hostnames = list(payload.get('servers', {}).keys())

    # Check if Shard_id already exists
    for new_shard in new_shards:
        shard_id = new_shard['Shard_id']
        if shard_id in shards:
            return jsonify({
                "error": "Shard ID already exists: {}".format(shard_id),
                "status": "error"
            }), 400

    # Check if server names already exist
    for hostname in hostnames:
        with server_name_lock:
            if hostname in server_names:
                response_json = {
                    "message": f"<Error> Server name {hostname} already exists",
                    "status": "failure"
                }
                return jsonify(response_json), 400

    if n > len(hostnames):
        response_json = {
            "message": "<Error> Number of new servers (n) is greater than newly added instances",
            "status": "failure"
        }
        return jsonify(response_json), 400
    
    if n < len(hostnames):
        response_json = {
            "message": "<Error> Number of new servers (n) is lesser than newly added instances",
            "status": "failure"
        }
        return jsonify(response_json), 400
    
    with server_name_lock:
        server_names_copy = server_names
    MapT_copy = MapT
    
    for new_shard in new_shards:
        shard_id = new_shard['Shard_id']
        # Add new shard details to shards_map
        shards[shard_id] = {
            'Stud_id_low': new_shard['Stud_id_low'],
            'Shard_size': new_shard['Shard_size']
        }
        ShardT[new_shard['Stud_id_low']] = {
            'Shard_id': shard_id,
            'Shard_size': new_shard['Shard_size'],
            'valid_idx': 0
        }
        shard_id_to_consistent_hashing_lock[shard_id] = threading.Lock()
        shard_id_to_consistent_hashing[shard_id] = ConsistentHashing(3, M, K)
        shard_id_to_write_request_lock[shard_id] = threading.Lock()
        shard_id_to_write_request_queue[shard_id] = queue.Queue()
        shard_id_to_update_request_queue[shard_id] = queue.Queue()
        shard_id_to_delete_request_queue[shard_id] = queue.Queue()
        shard_id_to_write_thread[shard_id] = threading.Thread(target=write_worker, args=(shard_id,), daemon=True)
        shard_id_to_write_thread[shard_id].start()
        shard_id_to_update_thread[shard_id] = threading.Thread(target=update_worker, args=(shard_id,), daemon=True)
        shard_id_to_update_thread[shard_id].start()
        shard_id_to_delete_thread[shard_id] = threading.Thread(target=delete_worker, args=(shard_id,), daemon=True)
        shard_id_to_delete_thread[shard_id].start()

    # list of server ids added
    server_id_list = []

    for i in range(0, n):
        res = None
        hostname = None
        # flag = 0
        if (i < len(hostnames)):
            hostname=hostnames[i]
            num=generate_id(hostname)
            name=f"Server{num}"
            valid_server_name[hostname]=name
            validname = valid_server_name[hostname]
            res = os.popen(
                f'sudo docker run --name "{validname}" --network distributed_systems_a-3_net1 --network-alias "{validname}" -e HOSTNAME="{validname}" -e SERVER_ID="{num}" -d distributed_systems_a-3-server').read()

        if len(res) == 0:
            response_json = {
                "message": f"<Error> Failed to start server {hostname}",
                "status": "failure"
            }
            return jsonify(response_json), 400
        else:
            # if flag:
            #     hostname = hostname[:12]
            while True:
                inspect_command = f'curl --fail --silent --output /dev/null --write-out "%{{http_code}}" http://{validname}:5000/heartbeat'
                container_status = os.popen(inspect_command).read().strip()
                if container_status == '200':
                    break
                else:
                    time.sleep(0.1)
            
            server_id_list.append(num)

            with server_name_lock:
                count += 1
                server_names.append(hostname)

            server_name_to_number[hostname] = num

            server_name_to_shards[hostname] = servers[hostname]
            # populating MapT
            for current_shard in servers[hostname]:
                MapT[current_shard].add(hostname)

            # configure each server
            config_payload = {
                "schema": schema,
                "shards": servers[hostname]
            }
            url = f'http://{validname}:5000/config'
            config_response = requests.post(url, json=config_payload)
            if config_response.status_code != 200:
                return jsonify({
                    "error": "Error configuring server {}: {}".format(hostname, config_response.text),
                    "status": "error"
                }), 400
            
            # to the consistent_hashing of each shard, add the server
            for current_shard in servers[hostname]:
                with shard_id_to_consistent_hashing_lock[current_shard]:
                    shard_id_to_consistent_hashing[current_shard].add_server(num, hostname)
            
            shard_data = {}

            for current_shard in server_name_to_shards[hostname]:
                if current_shard not in shard_data:
                    if current_shard in MapT_copy:
                        for copy_serv in MapT_copy[current_shard]:
                            payload = {
                                "shards":[current_shard]
                            }
                            response=requests.get(f"http://{valid_server_name[copy_serv]}:5000/copy",json=payload)
                            if response.status_code == 200:
                                response_json = response.json()
                                
                                sh_list = response_json.get(current_shard, [])
                                shard_data[current_shard]=sh_list
                                break
            
            for current_shard in server_name_to_shards[hostname]:
                if current_shard in shard_data:
                    payload={
                        "shard": current_shard,
                        "curr_idx":0,
                        "data":shard_data[current_shard]
                    }
                    response=requests.post(f"http://{valid_server_name[hostname]}:5000/write",json=payload)

    with server_name_lock:
        count_copy = count

    message_string = convert_to_string(server_id_list)

    response_json = {
        "N": count_copy,
        "message": message_string,
        "status": "successful"
    }
    return jsonify(response_json), 200

@app.route('/rm', methods=['DELETE'])
def remove_server():
    global count
    global server_names
    global server_name_to_number
    global server_name_lock
    global lock
    global shards
    global MapT
    global server_name_to_shards
    global ShardT
    global shard_id_to_consistent_hashing
    global shard_id_to_consistent_hashing_lock
    global shard_id_to_write_request_lock
    global shard_id_to_write_request_queue
    global shard_id_to_update_request_queue
    global shard_id_to_delete_request_queue
    global removed_servers
    global removed_servers_lock

    payload = request.json
    n = payload.get('n')
    hostnames = payload.get('servers')

    if n > count:
        response_json = {
            "message": f"<Error> Number of servers to be removed is more than those running",
            "status": "failure"
        }
        return jsonify(response_json), 400

    if len(hostnames) > n:
        response_json = {
            "message": "<Error> Length of server list is more than removable instances",
            "status": "failure"
        }
        return jsonify(response_json), 400

    for hostname in hostnames:
        if hostname not in server_names:
            response_json = {
                "message": f"<Error> Server name {hostname} does not exist",
                "status": "failure"
            }
            return jsonify(response_json), 400

    removed_server_name_list = []

    for i in range(0, n):
        hostname = None

        if i < len(hostnames):
            hostname = hostnames[i]
        else:
            hostname = random.choice(server_names)
        with server_name_lock:
            count -= 1
        with removed_servers_lock:
            removed_servers.append(hostname)        
        validname=valid_server_name[hostname]
        res1 = os.system(f'sudo docker stop {validname}')
        res2 = os.system(f'sudo docker rm {validname}')

        if res1 != 0 or res2 != 0:
            response_json = {
                "message": f"<Error> Failed to remove server {hostname}",
                "status": "failure"
            }
            with server_name_lock:
                count+=1
            return jsonify(response_json), 400
        
        with server_name_lock:
            if hostname in server_names:
                server_names.remove(hostname)
                with suggested_random_server_id_lock:
                    suggested_random_server_id.remove(server_name_to_number[hostname])
                # del valid_server_name[hostname]
                for current_shard in server_name_to_shards[hostname]:
                    MapT[current_shard].discard(hostname)
                    with shard_id_to_consistent_hashing_lock[current_shard]:
                        shard_id_to_consistent_hashing[current_shard].remove_server(server_name_to_number[hostname], hostname)
                    if(len(MapT[current_shard]) == 0):
                        del MapT[current_shard]
                        del ShardT[shards[current_shard]['Stud_id_low']]
                        del shards[current_shard]
                        del shard_id_to_consistent_hashing[current_shard]
                        del shard_id_to_consistent_hashing_lock[current_shard]
                        del shard_id_to_write_request_lock[current_shard]
                        del shard_id_to_write_request_queue[current_shard]
                        del shard_id_to_delete_request_queue[current_shard]
                        del shard_id_to_update_request_queue[current_shard]
                
                del server_name_to_shards[hostname] 
                
        removed_server_name_list.append(hostname)

    with server_name_lock:
        count_copy = count

    response_json = {
        "message": {
            "N": count_copy,
            "servers": removed_server_name_list
        },
        "status": "successful"
    }
    return jsonify(response_json), 200


@app.route('/read', methods=['POST'])
def read_data():
    global ShardT

    # Extract low and high Student ID from the request payload
    payload = request.json
    low_id = payload["Stud_id"]["low"]
    high_id = payload["Stud_id"]["high"]

    shards_queried = []  # To store unique shard IDs queried
    shards_range = [] # The lower and upper id of students in each shard

    # Iterate over the range of Student IDs to determine shards to query
    data = []

    if low_id > high_id:
        response = {
            "message": "<Error> Low id is greater than high id",
            "status": "failure"
        }
        return jsonify(response), 400

    # find the shards
    starting_stud_id_low = lower_bound_entry(ShardT, low_id)

    # handle the case when shard list is empty
    if starting_stud_id_low == -1:
        response = {
            "shards_queried": [],
            "data": data,
            "status": "success"
        }
        return jsonify(response), 200
    
    for key, value in dropwhile(lambda item: item[0] < starting_stud_id_low, ShardT.items()):
        if high_id >= key:
            if low_id < key + value['Shard_size']:
                shards_queried.append(value['Shard_id'])
                temp_object = {
                    "low_id": max(low_id, key),
                    "high_id": min(high_id, key+value['Shard_size']-1)
                }
                shards_range.append(temp_object)
        else:
            break
    
    # if no shards found
    if len(shards_queried) == 0:
        response = {
            "shards_queried": [],
            "data": data,
            "status": "success"
        }
        return jsonify(response), 200
    
    response_queue_list = []

    # putting request for each shard in read_request_queue
    for i in range(0, len(shards_queried)):
        # Create a queue for each request to handle its response
        response_queue = queue.Queue()
        response_queue_list.append(response_queue)

        # Generate random 6 digit id
        id = random.randint(100000, 999999)

        # Put the request details into the shared queue
        read_request_queue.put({
            'method': request.method,
            'path': request.full_path,
            'headers': request.headers,
            'data': request.get_data(),
            'cookies': request.cookies,
            'response_queue': response_queue,
            'id': id,
            'shard_id': shards_queried[i],
            'low_stud_id': shards_range[i]['low_id'],
            'high_stud_id': shards_range[i]['high_id']
        })
   
    # waiting for response of each request and appending the results to data
    for response_queue in response_queue_list:
        # Wait for the response from the read_worker thread
        response_data = response_queue.get()
        data.extend(response_data['data'])

    response = {
        "shards_queried": shards_queried,
        "data": data,
        "status": "success"
    }

    return jsonify(response), 200


@app.route('/write', methods=['POST'])
def write():
    global ShardT
    global shard_id_to_write_request_queue

    payload = request.json
    data_entries = payload.get('data')

    total_entries = len(data_entries)
    shard_id_to_data_entries = {}

    for entry in data_entries:
        current_stud_id = entry['Stud_id']
        possible_stud_id_low = lower_bound_entry(ShardT, current_stud_id)
        current_shard_id = -1
        for key, value in dropwhile(lambda item: item[0] < possible_stud_id_low, ShardT.items()):
            if current_stud_id >= key:
                if current_stud_id >= key and current_stud_id < key + value['Shard_size']:
                    current_shard_id = value['Shard_id']
                    break
            else:
                break
        if current_shard_id == -1:
            return jsonify({"error": f"No suitable shard found for data entry: {entry}", "status": "error"}), 500
        if current_shard_id in shard_id_to_data_entries:
            shard_id_to_data_entries[current_shard_id].append(entry)
        else:
            shard_id_to_data_entries[current_shard_id] = []
            shard_id_to_data_entries[current_shard_id].append(entry)
    
    response_queue_list = []

    # putting request for each shard in corresponding write_request_queue
    for shard_id, shard_data_entries in shard_id_to_data_entries.items():
        # Create a queue for each request to handle its response
        response_queue = queue.Queue()
        response_queue_list.append(response_queue)

        # Put the request details into the shared queue
        shard_id_to_write_request_queue[shard_id].put({
            'method': request.method,
            'path': request.full_path,
            'headers': request.headers,
            'data': request.get_data(),
            'cookies': request.cookies,
            'response_queue': response_queue,
            'data_entries': shard_data_entries,
            'num_entries': len(shard_id_to_data_entries[shard_id])
        })
    
    failed_entries = []
    error_messages = []
    failed_flag = False
    
    # waiting for response of each request and appending the results to data
    for response_queue in response_queue_list:
        # Wait for the response from the read_worker thread
        response_data = response_queue.get()
        if response_data['status_code'] != 200:
            failed_flag = True
            error_messages.append(response_data['message'])
            failed_entries.extend(response_data['failed_entries'])
    
    if failed_flag == True:
        response = {
            "error messages": error_messages,
            "failed entries": failed_entries,
            "status": "failure"
        }
        return jsonify(response), 200

    response = {
        "message": f"{total_entries} Data entries added",
        "status": "success"
    }

    return jsonify(response), 200


@app.route('/update', methods=['PUT'])
def update():
    global ShardT
    global shard_id_to_update_request_queue

    payload = request.json
    Stud_id = payload['Stud_id']
    data = payload['data']

    # finding the corresponding shard
    possible_stud_id_low = lower_bound_entry(ShardT, Stud_id)
    shard_id = -1
    for key, value in dropwhile(lambda item: item[0] < possible_stud_id_low, ShardT.items()):
        if Stud_id >= key:
            if Stud_id >= key and Stud_id < key + value['Shard_size']:
                shard_id = value['Shard_id']
                break
        else:
            break
    if shard_id == -1:
        return jsonify({"error": f"No suitable shard found", "status": "error"}), 500

    # Create a queue for each request to handle its response
    response_queue = queue.Queue()

    # Put the request details into the shared queue
    shard_id_to_update_request_queue[shard_id].put({
        'method': request.method,
        'path': request.full_path,
        'headers': request.headers,
        'data': request.get_data(),
        'cookies': request.cookies,
        'response_queue': response_queue,
        'data_entry': data,
        'Stud_id': Stud_id
    })

    # Wait for the response from the read_worker thread
    response_data = response_queue.get()

    if response_data['status_code'] != 200:
        response = {
            "message": response_data['message'],
            "status": "failure"
        }

        return jsonify(response), response_data['status_code']
    else:
        response = {
            "message": f"Data entry for Stud_id: {Stud_id} updated",
            "status": "success"
        }

        return jsonify(response), 200



@app.route('/del', methods=['DELETE'])
def delete():
    global ShardT
    global shard_id_to_delete_request_queue

    payload = request.json
    Stud_id = payload['Stud_id']

    # finding the corresponding shard
    possible_stud_id_low = lower_bound_entry(ShardT, Stud_id)
    shard_id = -1
    for key, value in dropwhile(lambda item: item[0] < possible_stud_id_low, ShardT.items()):
        if Stud_id >= key:
            if Stud_id >= key and Stud_id < key + value['Shard_size']:
                shard_id = value['Shard_id']
                break
        else:
            break
    if shard_id == -1:
        return jsonify({"error": f"No suitable shard found", "status": "error"}), 500

    # Create a queue for each request to handle its response
    response_queue = queue.Queue()

    # Put the request details into the shared queue
    shard_id_to_delete_request_queue[shard_id].put({
        'method': request.method,
        'path': request.full_path,
        'headers': request.headers,
        'data': request.get_data(),
        'cookies': request.cookies,
        'response_queue': response_queue,
        'Stud_id': Stud_id
    })

    # Wait for the response from the read_worker thread
    response_data = response_queue.get()

    if response_data['status_code'] != 200:
        response = {
            "message": response_data['message'],
            "status": "failure"
            # "response": response_data['responses']
        }

        return jsonify(response), response_data['status_code']
    else:
        response = {
            "message": f"Data entry with Stud_id: {Stud_id} removed from all replicas",
            "status": "success"
            # "response": response_data['responses']
        }

        return jsonify(response), 200


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
