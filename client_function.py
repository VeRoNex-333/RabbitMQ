import pyodbc
import pika
import json
import sys
import os
import importlib
import pandas as pd
import decimal
import traceback
from datetime import datetime
import logging
from cryptography.fernet import Fernet
from multiprocessing import Process 

# Encryption key and cipher setup
encryption_key = b''
cipher = Fernet(encryption_key)

# Logging Setup
def setup_logger():
    logger = logging.getLogger(f"main_logger")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt='%H:%M:%S')

    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')              # Create logs directory if not exists
    os.makedirs(logs_dir, exist_ok=True)

    file_path = os.path.join(logs_dir, f"client.log")

    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()                                # Also log to console for live feedback
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False                                                # Prevent logging duplication
    return logger

logger = setup_logger()

# Logging helper
def log_message(msg):
    logger.info(msg)

# Function to decrypt passwords
def decrypt_password(encrypted_password, cipher):
    return cipher.decrypt(encrypted_password.encode()).decode()

# Clean up decimal fields
def sanitize_row(row):
    return [int(col) if isinstance(col, decimal.Decimal) else col for col in row]

# Chunk data into safe payloads
def chunk_by_max_bytes(data, max_chunk_bytes=900000):  # 900 KB safe limit
    chunk = []
    current_chunk_bytes = 0

    for row in data:
        row_bytes = json.dumps(row, ensure_ascii=False).encode('utf-8')
        row_size = len(row_bytes)

        if row_size > max_chunk_bytes:
            raise ValueError("Single row exceeds maximum allowed size")

        if current_chunk_bytes + row_size > max_chunk_bytes:
            yield chunk
            chunk = []
            current_chunk_bytes = 0

        chunk.append(row)
        current_chunk_bytes += row_size

    if chunk:
        yield chunk

# Load the configuration from the specified file
def load_config():
    try:
        if len(sys.argv) > 1:
            folder_path = sys.argv[1]
        else:
            folder_path = os.path.dirname(sys.argv[0])

        config_file_path = os.path.join(folder_path, 'client_config_1.py')

        if not os.path.isfile(config_file_path):
            folder_path = input("Enter the folder path containing the config file without the \\client_config_1.py file name: ")
            config_file_path = os.path.join(folder_path, 'client_config_1.py')

            if not os.path.isfile(config_file_path):
                log_message("Config file not found in the specified path")
                sys.exit()

        spec = importlib.util.spec_from_file_location("client_config_1", config_file_path)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return config.sybase_config, config.rabbitmq_config

    except Exception as e:
        log_message(f"Error loading configuration: {e}")
        traceback.print_exc()
        sys.exit()

# Establish RabbitMQ connection
def connect_rabbitmq(rabbitmq_config):
    try:
        credentials = pika.PlainCredentials(rabbitmq_config['user'], decrypt_password(rabbitmq_config['password'], cipher))
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_config['host'], credentials=credentials))
        channel = connection.channel()
        return connection, channel
    except Exception as e:
        log_message(f"Failed to connect to RabbitMQ: {e}")
        traceback.print_exc()
        sys.exit()

# Publish response in chunks
def publish_response_chunks(payload_chunks, response_queue, rabbitmq_config, client_id, sybase_record_count, log_id, view_id):
    try:
        connection, channel = connect_rabbitmq(rabbitmq_config)
        channel.queue_declare(queue=response_queue, durable=True)

        for idx, chunk in enumerate(payload_chunks, start=1):
            response_payload = {
                "data": chunk,
                "sybase_record_count": sybase_record_count,
                "chunk_number": idx,
                "log_id": log_id,
                "viewid": view_id
            }
            channel.basic_publish(
                exchange='',
                routing_key=response_queue,
                body=json.dumps(response_payload),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            log_message(f"Sent chunk {idx} with {len(chunk)} records to queue {response_queue}")

        connection.close()
        log_message(f"All chunks sent for client {client_id}, viewid ID {view_id}. Total records: {sybase_record_count}")

        send_log_file(rabbitmq_config, client_id, view_id)

    except Exception as e:
        log_message(f"Error publishing response chunks: {e}")
        traceback.print_exc()

def process_query_message(query, client_id, view_id, log_id, sybase_config, license_key, rabbitmq_config):
    try:
        log_message(f"Thread started for viewid {view_id}")
        response_queue = f"client_{client_id}_{view_id}_response"
        # Connect to Sybase and execute query
        sybase_connection = pyodbc.connect(f"Driver={sybase_config['Driver']};host={sybase_config['host']}:{sybase_config['port']};server={sybase_config['server']};uid={sybase_config['uid']};pwd={decrypt_password(sybase_config['pwd'], cipher)};Database={sybase_config['Database']};CONNECTION_AUTHENTICATION={license_key};",timeout=18000)
        cursor = sybase_connection.cursor()
        log_message(f"Sybase connection established for viewid {view_id}")

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        sanitized_rows = [sanitize_row(row) for row in rows]

        df = pd.DataFrame(sanitized_rows, columns=columns)
        df_json = df.to_dict(orient='records')
        sybase_record_count = len(df_json)
        log_message(f"Fetched {sybase_record_count} records for viewid {view_id}")

        if sybase_record_count == 0:
            log_message(f"No data found for viewid {view_id}. Sending empty response.")
            publish_response_chunks([[]], response_queue, rabbitmq_config, client_id, sybase_record_count, log_id, view_id)
        else:
            publish_response_chunks(chunk_by_max_bytes(df_json), response_queue, rabbitmq_config, client_id, sybase_record_count, log_id, view_id)
        log_message(f"Thread completed for viewid {view_id}")
    except Exception as e:
        log_message(f"Error processing message for viewid {view_id}: {e}")
        traceback.print_exc()
        
# Callback for message consumption
def create_rabbitmq_callback(client_id, sybase_config, license_key, rabbitmq_config):
    def callback(ch, method, properties, body):
        global sybase_record_count
        try:
            message = json.loads(body)
            received_client_id = message['clientid']
            query = message['query']
            log_id = message.get('log_id')
            view_id = message.get('viewid')

            if str(received_client_id) != str(client_id):
                log_message(f"Ignored message for different client: {received_client_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            ch.basic_ack(delivery_tag=method.delivery_tag)
            log_message(f"Received query for client {client_id}, viewid {view_id}. Starting thread...")

            p = Process(target=process_query_message, args=(query, client_id, view_id, log_id, sybase_config, license_key, rabbitmq_config))
            p.start()
            
        except Exception as e:
            log_message(f"Callback error: {e}")
            traceback.print_exc()

    return callback    

def send_log_file(rabbitmq_config, client_id, view_id):
    try:
        connection, channel = connect_rabbitmq(rabbitmq_config)
        log_queue = f"client_{client_id}_log"
        channel.queue_declare(queue=log_queue, durable=True)

        log_file_path = os.path.join(os.path.dirname(__file__), 'logs', 'client.log')
        if os.path.isfile(log_file_path):
            with open(log_file_path, 'r') as f:
                log_content = f.read()

            payload = {
                "clientid": client_id,
                "viewid": view_id,
                "log_filename": "client.log",
                "log_content": log_content
            }

            channel.basic_publish(
                exchange='',
                routing_key=log_queue,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            log_message(f"Log file sent for viewid {view_id}")
        else:
            log_message("No log file found to send.")

        connection.close()
    
    except Exception as e:
        log_message(f"Failed to send log file: {e}")
        traceback.print_exc()
