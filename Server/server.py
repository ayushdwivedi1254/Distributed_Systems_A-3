from flask import Flask, request,jsonify
import os,socket,subprocess,json
# import mysql.connector
import psycopg2
from psycopg2 import errorcodes

app = Flask(__name__)

# Connect to the MySQL database
# container_name = socket.gethostname()
# db_connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="abc",
#     database="distributed_database"
# )
db_connection=None
data_type_mapping = {
    'Number': 'INT',   # Example mapping for 'Number' to 'INT'
    'String': 'VARCHAR'  # Example mapping for 'String' to 'VARCHAR'
}

# @app.route('/connect', methods=['POST'])
def connect_to_database():
    global db_connection
    # db_connection = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     password="abc",
    #     database="distributed_database"
    # )
    try:
        db_connection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="abc",
            # database="distributed_database"
        )
    except Exception as e:
        print(f"Error connecting to database:{str(e)}",500)
        return
    
    cursor = db_connection.cursor()
    cursor.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = 'distributed_database';")
    db_connection.commit()
    if cursor.fetchone() is None:
        db_connection.autocommit = True
        create_database_query="CREATE DATABASE distributed_database;"
        cursor.execute(create_database_query)
        db_connection.commit()
    
    cursor.close()

    db_connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="abc",
        database="distributed_database"
    )
    return '',200

@app.route('/config', methods=['POST'])
def config():
    global db_connection
    while db_connection is None:
        connect_to_database()

    request_payload = request.json
    schema = request_payload.get('schema', {})
    shards = request_payload.get('shards', [])

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
    return '', 200

@app.route('/copy', methods=['GET'])
def copy():
    global db_connection
    while db_connection is None:
        connect_to_database()

    request_payload = request.json
    shards = request_payload.get('shards', [])
    response_data = {}

    for shard in shards:
        while db_connection is None:
            connect_to_database()
        cursor = db_connection.cursor()
        cursor.execute(f"SELECT * FROM {shard};")
        data = cursor.fetchall()
        cursor.close()
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
    global db_connection
    while db_connection is None:
        connect_to_database()

    request_payload = request.json
    shard = request_payload.get('shard')
    stud_id = request_payload.get('Stud_id', {})
    low=stud_id['low']
    high=stud_id['high']

    response_data = []

    while db_connection is None:
        connect_to_database()
    cursor = db_connection.cursor()
    cursor.execute(f'SELECT * FROM {shard} WHERE "Stud_id" BETWEEN {low} AND {high};')
    data = cursor.fetchall()
    cursor.close()

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

@app.route('/write', methods=['POST'])
def write():
    global db_connection
    while db_connection is None:
        connect_to_database()

    request_payload = request.json
    shard = request_payload.get('shard')
    curr_idx = request_payload.get('curr_idx')
    data=request_payload.get('data',[])

    cursor = db_connection.cursor()
    old_curr_idx=curr_idx

    for entry in data:
        columns = ', '.join(['"' + key + '"' for key in entry.keys()])
        values = ', '.join(f'{value}' if isinstance(value,int) else f"'{value}'" for value in entry.values())
        insert_query=f'INSERT INTO {shard} ({columns}) VALUES ({values});'
        try:
            while db_connection is None:
                connect_to_database()
            cursor = db_connection.cursor()
            cursor.execute(insert_query)
        except psycopg2.Error as e:
            if e.pgcode==errorcodes.UNIQUE_VIOLATION:
                curr_idx=old_curr_idx
                response_json = {
                    "message": f"Duplicate entry for student ID {entry['Stud_id']}",
                    "failed_entries":data,
                    "current_idx": curr_idx,
                    "status": "failure"
                }
                cursor.close()
                db_connection.close()
                db_connection=None
                return jsonify(response_json),409
            else:
                curr_idx=old_curr_idx
                response_json = {
                    "message": f"Error when writing into the database",
                    "failed_entries":data,
                    "current_idx": curr_idx,
                    "status": "failure"
                }
                cursor.close()
                db_connection.close()
                db_connection=None
                return jsonify(response_json),500 
        curr_idx+=1
    
    cursor.close()
    db_connection.commit()
        
    response_json = {
        "message": "Data entries added",
        "current_idx": curr_idx,
        "status": "success"
    }

    return jsonify(response_json), 200

@app.route('/update', methods=['PUT'])
def update():
    global db_connection
    while db_connection is None:
        connect_to_database()

    request_payload = request.json
    shard = request_payload.get('shard')
    stud_id = request_payload.get('Stud_id')
    data=request_payload.get('data',{})

    columns=data.keys()
    values=list(data.values())

    cursor = db_connection.cursor()

    check_query = f'SELECT * FROM {shard} WHERE "Stud_id" = %s;'
    while db_connection is None:
        connect_to_database()
    cursor = db_connection.cursor()
    cursor.execute(check_query, (stud_id,))
    existing_record = cursor.fetchone()  # Fetch one record
    
    if existing_record:
        set_clause=', '.join([f'"{column}"=%s' for column in columns])
        update_query=f'UPDATE {shard} SET {set_clause} WHERE "Stud_id"={stud_id};'
        try:
            while db_connection is None:
                connect_to_database()
            cursor = db_connection.cursor()
            cursor.execute(update_query,values)
        except psycopg2.Error as e:
            if e.pgcode==errorcodes.UNIQUE_VIOLATION:
                response_json = {
                    "message": f"Duplicate entry for student ID {stud_id}",
                    "status": "failure"
                }
                cursor.close()
                db_connection.close()
                db_connection=None
                return jsonify(response_json),409
            else:
                response_json = {
                    "message": f"Error when updating database",
                    "status": "failure"
                }
                cursor.close()
                db_connection.close()
                db_connection=None
                return jsonify(response_json),500

        cursor.close()
        db_connection.commit()

        response_json = {
            "message": f"Data entry for Stud_id: {stud_id} updated",
            "status": "success"
        }
        return jsonify(response_json), 200
    else:
        cursor.close()
        response_json = {
            "message": f"No record found for Stud_id: {stud_id}",
            "status": "failure"
        }
        return jsonify(response_json), 404

@app.route('/del', methods=['DELETE'])
def delete():
    global db_connection
    while db_connection is None:
        connect_to_database()

    request_payload = request.json
    shard = request_payload.get('shard')
    stud_id = request_payload.get('Stud_id')

    cursor = db_connection.cursor()
    
    check_query = f'SELECT * FROM {shard} WHERE "Stud_id" = %s;'
    while db_connection is None:
        connect_to_database()
    cursor = db_connection.cursor()
    cursor.execute(check_query, (stud_id,))
    existing_record = cursor.fetchone()  # Fetch one record

    if existing_record:
        delete_query=f'DELETE FROM {shard} WHERE "Stud_id"={stud_id};'
        while db_connection is None:
            connect_to_database()
        cursor = db_connection.cursor()
        cursor.execute(delete_query)
        db_connection.commit()
    
        cursor.close()

        response_json = {
            "message": f"Data entry with Stud_id: {stud_id} removed",
            "status": "success"
        }
        return jsonify(response_json), 200
    else:
        cursor.close()
        response_json = {
            "message": f"No record found for Stud_id: {stud_id}",
            "status": "failure"
        }
        return jsonify(response_json), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)