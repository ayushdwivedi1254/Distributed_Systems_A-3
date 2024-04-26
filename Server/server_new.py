import threading
from flask import Flask, request,jsonify
import requests
import os,socket,subprocess,json
# import mysql.connector
import psycopg2
from psycopg2 import errorcodes
import queue

app = Flask(__name__)

# Connect to the MySQL database
# container_name = socket.gethostname()
# db_connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="abc",
#     database="distributed_database"
# )

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
connection_pool = ConnectionPool(max_connections=98)

db_connection=None
data_type_mapping = {
    'Number': 'INT',   # Example mapping for 'Number' to 'INT'
    'String': 'VARCHAR'  # Example mapping for 'String' to 'VARCHAR'
}

# @app.route('/connect', methods=['POST'])
# def connect_to_database():
#     global db_connection
#     # db_connection = mysql.connector.connect(
#     #     host="localhost",
#     #     user="root",
#     #     password="abc",
#     #     database="distributed_database"
#     # )
#     try:
#         db_connection = psycopg2.connect(
#             host="DB1",
#             user="postgres",
#             password="abc",
#             # database="distributed_database"
#         )
#     except Exception as e:
#         print(f"Error connecting to database:{str(e)}",500)
#         return
    
#     cursor = db_connection.cursor()
#     cursor.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = 'distributed_database';")
#     db_connection.commit()
#     if cursor.fetchone() is None:
#         db_connection.autocommit = True
#         create_database_query="CREATE DATABASE distributed_database;"
#         cursor.execute(create_database_query)
#         db_connection.commit()
    
#     cursor.close()

#     db_connection = psycopg2.connect(
#         host="DB1",
#         user="postgres",
#         password="abc",
#         database="distributed_database"
#     )
#     return '',200

@app.route('/config', methods=['POST'])
def config():
    # global db_connection
    # while db_connection is None:
    #     connect_to_database()
    db_connection=connection_pool.get_connection()

    request_payload = request.json
    schema = request_payload.get('schema', {})
    shards = request_payload.get('shards', [])

    create_table_query = "CREATE TABLE IF NOT EXISTS WAL (Shard_id VARCHAR, Log_id INTEGER, Query TEXT, CONSTRAINT constraint_unique UNIQUE (Shard_id, Log_id))"
    
    # Execute the SQL query to create the table in the database
    cursor = db_connection.cursor()
    cursor.execute(create_table_query)
    db_connection.commit()
    cursor.close()
    
    create_table_query = "CREATE TABLE IF NOT EXISTS IDX (Shard_id VARCHAR, Curr_idx INTEGER, Valid_idx INTEGER)"
    
    # Execute the SQL query to create the table in the database
    cursor = db_connection.cursor()
    cursor.execute(create_table_query)
    db_connection.commit()
    cursor.close()
    curr_dict={}
    valid_dict={}
    for shard_id in shards:
        select_query=f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id='{shard_id}'"
        cursor = db_connection.cursor()
        cursor.execute(select_query)
        row=cursor.fetchone()
        cursor.close()
        # response = requests.post(f"http://shard_manager:5000/{row}", json={})
        if row:
            curr_dict[shard_id]=row[0]
            valid_dict[shard_id]=row[1]
            
            # response = requests.post(f"http://shard_manager:5000/HEMLO1", json={})
            continue
        insert_query = "INSERT INTO IDX (Shard_id, Curr_idx, Valid_idx) VALUES (%s, %s, %s)"
        cursor = db_connection.cursor()
        cursor.execute(insert_query, (shard_id, 0, 0))
        db_connection.commit()
        cursor.close()
        curr_dict[shard_id]=0
        valid_dict[shard_id]=0    

    for shard in shards:
        # columns=""
        columns = ', '.join([f'"{col}" {data_type_mapping[dtype]}' for col, dtype in zip(schema['columns'], schema['dtypes'])])
        columns = columns.split(', ')
        columns[0] += ' PRIMARY KEY'
        columns = ', '.join(columns)
        # for col, dtype in zip(schema['columns'], schema['dtypes']):
            # columns+=(f'{col} {data_type_mapping[dtype]}')
            # columns+=','
        # columns=columns[:-1]

        create_table_query = f"CREATE TABLE IF NOT EXISTS {shard} ({columns})"
    
        # Execute the SQL query to create the table in the database
        cursor = db_connection.cursor()
        cursor.execute(create_table_query)
        db_connection.commit()
        cursor.close()

    # db_connection.close()
    connection_pool.return_connection(db_connection)

    # response = requests.post(f"http://shard_manager:5000/HEMLO2", json={})
    
    for shard_id in shards:
        db_connection=connection_pool.get_connection()
        try:

            # response = requests.post(f"http://shard_manager:5000/HEMLO9", json={})
            
            index_response=requests.post("http://shard_manager:5000/getPrimaryIndex",json={"shard_id":shard_id})
            response_data=index_response.json()
            p_valid=response_data["valid_idx"]
            p_curr=response_data["curr_idx"]

            # response = requests.post(f"http://shard_manager:5000/HEMLO3", json={})

            log_response=requests.post("http://shard_manager:5000/getLogs",json={"shard_id":shard_id,"from":valid_dict[shard_id]+1,"to":p_valid})
            response_data=log_response.json()
            queries=response_data["queries"]

            # response = requests.post(f"http://shard_manager:5000/HEMLO4", json={})

            for query in queries:
                curr_dict[shard_id]=curr_dict[shard_id]+1
                insert_query="INSERT INTO WAL (Shard_id, Log_id, Query) VALUES (%s, %s, %s) ON CONFLICT ON CONSTRAINT constraint_unique DO NOTHING"
                cursor = db_connection.cursor()
                cursor.execute(insert_query, (shard_id, curr_dict[shard_id], query))
                db_connection.commit()
                cursor.close()   

            # response = requests.post(f"http://shard_manager:5000/HEMLO5", json={})

            for query in queries:
                valid_dict[shard_id]=valid_dict[shard_id]+1
                cursor = db_connection.cursor()
                cursor.execute(query)
                db_connection.commit()
                cursor.close()
            
            update_query = f"UPDATE IDX SET Curr_idx = {curr_dict[shard_id]}, Valid_idx = {valid_dict[shard_id]} WHERE Shard_id = '{shard_id}'"
            cursor = db_connection.cursor()
            cursor.execute(update_query)
            db_connection.commit()
            cursor.close()

            # response = requests.post(f"http://shard_manager:5000/HEMLO6", json={})
            connection_pool.return_connection(db_connection)
        except Exception as e:
            # response = requests.post(f"http://shard_manager:5000/HEMLO7", json={})
            connection_pool.return_connection(db_connection)
            return {"error": str(e)}

    # response = requests.post(f"http://shard_manager:5000/HEMLO8", json={})
    
    response_json = {
        "message": ", ".join([f"Server0:{shard}" for shard in shards]) + " configured",
        "status": "success"
    }
    return jsonify(response_json), 200

# @app.route('/home', methods=['GET'])
# def home():
#     container_name = os.environ.get('HOSTNAME')
#     if container_name is None:
#         with open('/etc/hostname', 'r') as file:
#             container_name = file.read().strip()
#     return f'Hello from Server: {container_name}'

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    if connection_pool.connection_status():
        return '',200
    return '',503

@app.route('/copy', methods=['GET'])
def copy():
    # global db_connection
    # while db_connection is None:
    #     connect_to_database()

    request_payload = request.json
    shards = request_payload.get('shards', [])
    response_data = {}

    for shard in shards:
        # while db_connection is None:
        #     connect_to_database()

        db_connection=connection_pool.get_connection()

        cursor = db_connection.cursor()
        cursor.execute(f"SELECT * FROM {shard};")
        data = cursor.fetchall()
        cursor.close()

        # db_connection.close()
        connection_pool.return_connection(db_connection)

        # response_data[shard] = [row for row in data]
        response_data[shard]=[]
        column_names = [column[0] for column in cursor.description]
    
        # Create dictionaries with column names as keys
        for row in data:
            row_dict = dict(zip(column_names, row))
            response_data[shard].append(row_dict)

    response_json = {
        "status": "success",
        **response_data
    }

    return jsonify(response_json), 200

@app.route('/read', methods=['POST'])
def read():
    # global db_connection
    # while db_connection is None:
    #     connect_to_database()

    request_payload = request.json
    shard = request_payload.get('shard')
    stud_id = request_payload.get('Stud_id', {})
    low=stud_id['low']
    high=stud_id['high']

    response_data = []

    # while db_connection is None:
    #     connect_to_database()

    db_connection=connection_pool.get_connection()

    cursor = db_connection.cursor()
    cursor.execute(f'SELECT * FROM {shard} WHERE "Stud_id" BETWEEN {low} AND {high};')
    data = cursor.fetchall()
    cursor.close()

    # db_connection.close()
    connection_pool.return_connection(db_connection)

    column_names = [column[0] for column in cursor.description]
    
    # Create dictionaries with column names as keys
    for row in data:
        row_dict = dict(zip(column_names, row))
        response_data.append(row_dict)
    # response_data = [row for row in data]

    response_json = {
        "data": response_data,
        "status": "success"
    }

    return jsonify(response_json), 200

@app.route('/getIndex', methods=['POST'])
def get_index():
    data = request.json
    shard_id = data.get('shard_id')

    query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard_id}'"
    
    db_connection=connection_pool.get_connection()

    cursor = db_connection.cursor()
    cursor.execute(query)
    row=cursor.fetchone()
    cursor.close()

    connection_pool.return_connection(db_connection)

    curr_idx = row[0]
    valid_idx = row[1]

    response_data = {
        'curr_idx': curr_idx,
        'valid_idx': valid_idx
    }
    return jsonify(response_data), 200

@app.route('/getLogs', methods=['POST'])
def get_logs():
    data = request.json
    shard_id = data.get('shard_id')
    valid_idx_from = data.get('from')
    valid_idx_to = data.get('to')

    db_connection=connection_pool.get_connection()

    select_query = f"SELECT Query FROM WAL WHERE Shard_id = '{shard_id}' AND Log_id >= {valid_idx_from} AND Log_id <= {valid_idx_to} ORDER BY Log_id ASC"
    cursor = db_connection.cursor()
    cursor.execute(select_query)
    rows=cursor.fetchall()
    cursor.close()

    connection_pool.return_connection(db_connection)

    queries = [row[0] for row in rows]

    response_data = {
        'queries': queries
    }
    return jsonify(response_data), 200

@app.route('/write_log', methods=['POST'])
def write_log():

    request_payload = request.json
    shard = request_payload.get('shard')
    req_curr_idx = request_payload.get('curr_idx')
    req_valid_idx = request_payload.get('valid_idx')
    flag=request_payload.get('flag')
    data=request_payload.get('data',[])

    query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard}'"
    
    db_connection=connection_pool.get_connection()

    cursor = db_connection.cursor()
    cursor.execute(query)
    row=cursor.fetchone()
    cursor.close()

    connection_pool.return_connection(db_connection)

    curr_idx = row[0]
    valid_idx = row[1]
    
    if valid_idx!=req_valid_idx and flag=="log":
        # response = requests.post(f"http://shard_manager:5000/HEMLO1", json={"shard_id": shard})
        response=requests.post("http://shard_manager:5000/getLogs",json={"shard_id": shard, "from":valid_idx+1, "to": req_valid_idx})
        response_json=response.json()
        queries=response_json.get("queries")
        
        db_connection=connection_pool.get_connection()
        
        for query in queries:
            curr_idx=curr_idx+1
            insert_query="INSERT INTO WAL (Shard_id, Log_id, Query) VALUES (%s, %s, %s) ON CONFLICT ON CONSTRAINT constraint_unique DO NOTHING"
            cursor = db_connection.cursor()
            cursor.execute(insert_query, (shard, curr_idx, query))
            db_connection.commit()
            cursor.close()   

        for query in queries:
            valid_idx=valid_idx+1
            cursor = db_connection.cursor()
            cursor.execute(query)
            db_connection.commit()
            cursor.close() 

        update_query = f"UPDATE IDX SET Curr_idx = {curr_idx}, Valid_idx = {valid_idx} WHERE Shard_id = '{shard}'"
        cursor = db_connection.cursor()
        cursor.execute(update_query)
        db_connection.commit()
        cursor.close()
        connection_pool.return_connection(db_connection)

    if flag=="log":
        query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard}'"

        # Execute the SQL query to create the table in the database
        db_connection=connection_pool.get_connection()
        cursor = db_connection.cursor()
        cursor.execute(query)
        row=cursor.fetchone()
        cursor.close()

        curr_idx = row[0]
        valid_idx = row[1]

        connection_pool.return_connection(db_connection)

        db_connection=connection_pool.get_connection()
        # response = requests.post(f"http://shard_manager:5000/HEMLO2", json={"shard_id": shard})

        for query in data:
            curr_idx=curr_idx+1
            insert_query="INSERT INTO WAL (Shard_id, Log_id, Query) VALUES (%s, %s, %s) ON CONFLICT ON CONSTRAINT constraint_unique DO NOTHING"
            cursor = db_connection.cursor()
            cursor.execute(insert_query, (shard, curr_idx, query))
            db_connection.commit()
            cursor.close()   

        # response = requests.post(f"http://shard_manager:5000/HEMLO23", json={"shard_id": shard})

        update_query = f"UPDATE IDX SET Curr_idx = {curr_idx} WHERE Shard_id = '{shard}'"
        cursor = db_connection.cursor()
        cursor.execute(update_query)
        db_connection.commit()
        cursor.close()
        connection_pool.return_connection(db_connection)

        # response = requests.post(f"http://shard_manager:5000/HEMLO3", json={"shard_id": shard})

        response_json={
            "flag": "log",
            "status": "success"
        }
        return jsonify(response_json), 200

    else:
        # query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard}'"
    
        # # Execute the SQL query to create the table in the database
        db_connection=connection_pool.get_connection()
        # cursor = db_connection.cursor()
        # cursor.execute(query)
        # row=cursor.fetchone[0]
        # cursor.close()

        # curr_idx = row[0]
        # valid_idx = row[1]
        # response = requests.post(f"http://shard_manager:5000/HEMLO4", json={"shard_id": shard})

        select_query = f"SELECT Query FROM WAL WHERE Shard_id = '{shard}' AND Log_id > {valid_idx} AND Log_id <= {curr_idx} ORDER BY Log_id ASC"
        cursor = db_connection.cursor()
        cursor.execute(select_query)
        rows=cursor.fetchall()
        cursor.close()

        # response = requests.post(f"http://shard_manager:5000/HEMLO44", json={"shard_id": shard})

        queries = [row[0] for row in rows]
        for query in queries:
            valid_idx=valid_idx+1
            cursor = db_connection.cursor()
            cursor.execute(query)
            db_connection.commit()
            cursor.close()

        # response = requests.post(f"http://shard_manager:5000/HEMLO48", json={"shard_id": shard})

        update_query = f"UPDATE IDX SET Valid_idx = {valid_idx} WHERE Shard_id = '{shard}'"
        cursor = db_connection.cursor()
        cursor.execute(update_query)
        db_connection.commit()
        cursor.close()
        connection_pool.return_connection(db_connection)

        # response = requests.post(f"http://shard_manager:5000/HEMLO5", json={"shard_id": shard})

        response_json={
            "flag": "commit",
            "status": "success"
        }
        return jsonify(response_json), 200

# Define a function to send requests to a server
def send_request(server_name, data):
    url = f"http://{server_name}:5000/write_log"
    try:
        response = requests.post(url, json=data)
        return response.json(), 200
    except Exception as e:
        return {"error": str(e)}

@app.route('/write', methods=['POST'])
def write():

    request_payload = request.json
    shard = request_payload.get('shard')
    data=request_payload.get('data',[])

    db_connection=connection_pool.get_connection()
    cursor = db_connection.cursor()

    for entry in data:
        columns = ', '.join(['"' + key + '"' for key in entry.keys()])
        values = ', '.join(f'{value}' if isinstance(value,int) else f"'{value}'" for value in entry.values())
        insert_query=f'INSERT INTO {shard} ({columns}) VALUES ({values});'
        try:
            cursor.execute(insert_query)
        except psycopg2.Error as e:
            if e.pgcode==errorcodes.UNIQUE_VIOLATION:
                response_json = {
                    "message": f"Duplicate entry for student ID {entry['Stud_id']}",
                    "failed_entries":data,
                    "status": "failure"
                }
                cursor.close()
                db_connection.close()
                connection_pool.open_connection()
                return jsonify(response_json),409
            else:
                response_json = {
                    "message": f"Error when writing into the database",
                    "failed_entries":data,
                    "status": "failure"
                }
                cursor.close()
                db_connection.close()
                connection_pool.open_connection()
                return jsonify(response_json),500
    
    cursor.close()
    db_connection.close()
    connection_pool.open_connection()

    # get the list of secondary servers from shard_manager

    response = requests.post("http://shard_manager:5000/getSecondaryServers", json={"shard_id": shard})
    response_json = response.json()
    secondary_servers = response_json["servers"]
    majority = (len(secondary_servers)+1)/2     # majority excluding primary

    # loop to each server and send /write_log request
    query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard}'"
    db_connection=connection_pool.get_connection()

    cursor = db_connection.cursor()
    cursor.execute(query)
    row=cursor.fetchone()
    cursor.close()

    connection_pool.return_connection(db_connection)

    curr_idx = row[0]
    valid_idx = row[1]
    queries = []

    for entry in data:
        columns = ', '.join(['"' + key + '"' for key in entry.keys()])
        values = ', '.join(f'{value}' if isinstance(value,int) else f"'{value}'" for value in entry.values())
        insert_query=f'INSERT INTO {shard} ({columns}) VALUES ({values});'
        queries.append(insert_query)

    payload = {
        "shard": shard,
        "curr_idx": curr_idx,
        "valid_idx": valid_idx,
        "flag": "log",
        "data": queries
    }

    url = f"http://localhost:5000/write_log"
    response = requests.post(url, json=payload)

    threads = []

    # Create threads to send requests to each server
    for server_name in secondary_servers:
        thread = threading.Thread(target=lambda: send_request(server_name, payload))
        threads.append(thread)
        thread.start()

    # Wait for threads to finish and collect responses
    responses = 0
    for thread in threads:
        thread.join()
        responses+=1
        if responses >= majority:
            break

    # upon receiving responses from a majority, send /write_log request to commit them

    payload["flag"] = "commit"
    threads = []

    # Create threads to send requests to each server
    for server_name in secondary_servers:
        thread = threading.Thread(target=lambda: send_request(server_name, payload))
        threads.append(thread)
        thread.start()

    # Wait for threads to finish and collect responses
    responses = 0
    for thread in threads:
        thread.join()
        responses+=1
        if responses >= majority:
            break

    # upon receiving responses from a majority, send /write_log to primary
    url = f"http://localhost:5000/write_log"
    response = requests.post(url, json=payload)
        
    response_json = {
        "message": "Data entries added",
        "status": "success"
    }

    return jsonify(response_json), 200

@app.route('/update', methods=['PUT'])
def update():

    request_payload = request.json
    shard = request_payload.get('shard')
    stud_id = request_payload.get('Stud_id')
    data=request_payload.get('data',{})

    columns=data.keys()
    values=list(data.values())

    db_connection=connection_pool.get_connection()
    cursor = db_connection.cursor()

    check_query = f'SELECT * FROM {shard} WHERE "Stud_id" = %s;'
    cursor.execute(check_query, (stud_id,))
    existing_record = cursor.fetchone()  # Fetch one record

    if not existing_record:
        cursor.close()
        connection_pool.return_connection(db_connection)
        response_json = {
            "message": f"No record found for Stud_id: {stud_id}",
            "status": "failure"
        }
        return jsonify(response_json), 404  
    
    set_clause=', '.join([f'"{column}"=\'{value}\' ' for column, value in zip(columns, values)])
    update_query=f'UPDATE {shard} SET {set_clause} WHERE "Stud_id"={stud_id};'
    try:
        cursor.execute(update_query)
    except psycopg2.Error as e:
        if e.pgcode==errorcodes.UNIQUE_VIOLATION:
            response_json = {
                "message": f"Duplicate entry for student ID {stud_id}",
                "status": "failure"
            }
            cursor.close()

            db_connection.close()
            connection_pool.open_connection()
            return jsonify(response_json),409
        else:
            response_json = {
                "message": f"Error when updating database",
                "status": "failure"
            }
            cursor.close()

            db_connection.close()
            connection_pool.open_connection()
            return jsonify(response_json),500

    cursor.close()
    db_connection.close()
    connection_pool.open_connection()

    # get the list of secondary servers from shard_manager

    response = requests.post("http://shard_manager:5000/getSecondaryServers", json={"shard_id": shard})
    response_json = response.json()
    secondary_servers = response_json["servers"]
    majority = (len(secondary_servers)+1)/2     # majority excluding primary

    # loop to each server and send /write_log request
    query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard}'"
    db_connection=connection_pool.get_connection()

    cursor = db_connection.cursor()
    cursor.execute(query)
    row=cursor.fetchone()
    cursor.close()

    connection_pool.return_connection(db_connection)

    curr_idx = row[0]
    valid_idx = row[1]
    queries = []
    queries.append(update_query)

    payload = {
        "shard": shard,
        "curr_idx": curr_idx,
        "valid_idx": valid_idx,
        "flag": "log",
        "data": queries
    }

    url = f"http://localhost:5000/write_log"
    response = requests.post(url, json=payload)

    threads = []

    # Create threads to send requests to each server
    for server_name in secondary_servers:
        thread = threading.Thread(target=lambda: send_request(server_name, payload))
        threads.append(thread)
        thread.start()

    # Wait for threads to finish and collect responses
    responses = 0
    for thread in threads:
        thread.join()
        responses+=1
        if responses >= majority:
            break

    # upon receiving responses from a majority, send /write_log request to commit them

    payload["flag"] = "commit"
    threads = []

    # Create threads to send requests to each server
    for server_name in secondary_servers:
        thread = threading.Thread(target=lambda: send_request(server_name, payload))
        threads.append(thread)
        thread.start()

    # Wait for threads to finish and collect responses
    responses = 0
    for thread in threads:
        thread.join()
        responses+=1
        if responses >= majority:
            break

    # upon receiving responses from a majority, send /write_log to primary
    url = f"http://localhost:5000/write_log"
    response = requests.post(url, json=payload)

    response_json = {
        "message": f"Data entry for Stud_id: {stud_id} updated",
        "status": "success"
    }
    return jsonify(response_json), 200

@app.route('/del', methods=['DELETE'])
def delete():

    request_payload = request.json
    shard = request_payload.get('shard')
    stud_id = request_payload.get('Stud_id')
    
    check_query = f'SELECT * FROM {shard} WHERE "Stud_id" = %s;'
    db_connection=connection_pool.get_connection()
    cursor = db_connection.cursor()
    cursor.execute(check_query, (stud_id,))
    existing_record = cursor.fetchone()  # Fetch one record

    if not existing_record:
        cursor.close()
        connection_pool.return_connection(db_connection)

        response_json = {
            "message": f"No record found for Stud_id: {stud_id}",
            "status": "failure"
        }
        return jsonify(response_json), 404
    
    delete_query=f'DELETE FROM {shard} WHERE "Stud_id"={stud_id};'

    cursor.close()
    connection_pool.return_connection(db_connection)

    # get the list of secondary servers from shard_manager

    response = requests.post("http://shard_manager:5000/getSecondaryServers", json={"shard_id": shard})
    response_json = response.json()
    secondary_servers = response_json["servers"]
    majority = (len(secondary_servers)+1)/2     # majority excluding primary

    # loop to each server and send /write_log request
    query = f"SELECT Curr_idx, Valid_idx FROM IDX WHERE Shard_id = '{shard}'"
    db_connection=connection_pool.get_connection()

    cursor = db_connection.cursor()
    cursor.execute(query)
    row=cursor.fetchone()
    cursor.close()

    connection_pool.return_connection(db_connection)

    curr_idx = row[0]
    valid_idx = row[1]
    queries = []
    queries.append(delete_query)

    payload = {
        "shard": shard,
        "curr_idx": curr_idx,
        "valid_idx": valid_idx,
        "flag": "log",
        "data": queries
    }

    url = f"http://localhost:5000/write_log"
    response = requests.post(url, json=payload)

    threads = []

    # Create threads to send requests to each server
    for server_name in secondary_servers:
        thread = threading.Thread(target=lambda: send_request(server_name, payload))
        threads.append(thread)
        thread.start()

    # Wait for threads to finish and collect responses
    responses = 0
    for thread in threads:
        thread.join()
        responses+=1
        if responses >= majority:
            break

    # upon receiving responses from a majority, send /write_log request to commit them

    payload["flag"] = "commit"
    threads = []

    # Create threads to send requests to each server
    for server_name in secondary_servers:
        thread = threading.Thread(target=lambda: send_request(server_name, payload))
        threads.append(thread)
        thread.start()

    # Wait for threads to finish and collect responses
    responses = 0
    for thread in threads:
        thread.join()
        responses+=1
        if responses >= majority:
            break

    # upon receiving responses from a majority, send /write_log to primary
    url = f"http://localhost:5000/write_log"
    response = requests.post(url, json=payload)

    response_json = {
        "message": f"Data entry with Stud_id: {stud_id} removed",
        "status": "success"
    }
    return jsonify(response_json), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)