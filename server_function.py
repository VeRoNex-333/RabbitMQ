import pika
import psycopg2
import json
import sys
import os
import importlib
import time
from datetime import datetime
import logging
from cryptography.fernet import Fernet
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import date
from multiprocessing import Process


# Encryption key and cipher setup
encryption_key = b''
cipher = Fernet(encryption_key)

# Function to decrypt passwords
def decrypt_password(encrypted_password, cipher):
    return cipher.decrypt(encrypted_password.encode()).decode()

#Per Client Logging Setup
client_loggers = {}

def get_client_logger(client_id, view_id):
    key = f"{client_id}_{view_id}"
    if key in client_loggers:
        return client_loggers[key]
    
    logger = logging.getLogger(f"Client_{client_id}_View_{view_id}")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt='%H:%M:%S')

    # Create client specific log directory
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    client_log_dir = os.path.join(logs_dir, f"client_{client_id}")
    os.makedirs(client_log_dir, exist_ok=True)

    # Server side log path
    server_log_path = os.path.join(client_log_dir, 'server.log')
    file_handler = logging.FileHandler(server_log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()                                # Also log to console for live feedback
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False                                                # Prevent logging duplication
    client_loggers[key] = logger
    return logger

record_tracker = {}                             # Initializes a dictionary to track how many records have been inserted per client

# Generic logger for non-client-specific logs
def log_message(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}")         # Logs messages with the current time

# Load config
def load_config():
    try:
        if len(sys.argv) > 1:
            folder_path = sys.argv[1]
        else:
            folder_path = os.path.dirname(sys.argv[0])

        config_file_path = os.path.join(folder_path, 'server_config.py')

        if not os.path.isfile(config_file_path):
            folder_path = input("Enter the folder path containing the config file without the \\server_config.py file name: ")
            config_file_path = os.path.join(folder_path, 'server_config.py')

            if not os.path.isfile(config_file_path):
                log_message("Config file not found in the specified path")
                sys.exit()

        spec = importlib.util.spec_from_file_location("server_config", config_file_path)     # Creates a blueprint for the config file
        config = importlib.util.module_from_spec(spec)                                  # Creates an module object
        spec.loader.exec_module(config)                                                 # Executes the code in server_config.py and loads its variables/functions into the config object.
        return config.postgres_config,  config.rabbitmq_config, config.email_config, config.query_templates
    except Exception as e:
        log_message(f"Config load error: {e}")
        sys.exit()


# Fetch all active clients
def get_clients(main_cursor, query_templates):
    try:
        main_cursor.execute(query_templates["active_client_query"])
        clients = main_cursor.fetchall()
        columns = [desc[0] for desc in main_cursor.description]
        return clients, columns
    except Exception as e:
        log_message(f"Error fetching clients: {e}")
        return [], []

# Consume messages for a single client
def consume_for_client(client_id, client_info, rabbitmq_host, rabbitmq_credentials, postgres_config, view_id):         # client_info: a dictionary containing that client's postgresql credentials
    client_logger = get_client_logger(client_id, view_id)
    main_conn = psycopg2.connect(**postgres_config)                   # Main log connection
    main_cursor = main_conn.cursor()

    pg_conn = None
    pg_cursor = None

    try:
        client_logger.info(f"Starting consumer thread for client {client_id} and viewid {view_id}")

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=rabbitmq_credentials))
        channel = connection.channel()

        response_queue = f"client_{client_id}_{view_id}_response"
        channel.queue_declare(queue=response_queue, durable=True)
        pg_conn = psycopg2.connect(host=client_info['host'],port=client_info['port'],dbname=client_info['dbname'],user=client_info['user'],password=client_info['password'])    # Connect to Client specific postgresql db
        pg_cursor = pg_conn.cursor()

        def callback(ch, method, properties, body):                 # RabbitMQ calls this function every time a message is received
            log_id = None
            try:
                payload = json.loads(body.decode())                 # JSON into a Python dict
                data = payload.get('data', [])
                total = payload.get('sybase_record_count', 0)
                log_id = payload.get('log_id')

                if not data:  
                    ch.basic_ack(delivery_tag=method.delivery_tag)      # If there’s no data in the message, it acknowledges the message and exits early.
                    return

                if client_id not in record_tracker:
                    record_tracker[client_id] = {'inserted': 0, 'expected': total, 'log_id': log_id}
                    pg_cursor.execute("TRUNCATE TABLE patient_master_staging")  # Clear staging table only once, before inserting the first chunk
                    # Update total_records in refresh log (first time only)
                    main_cursor.execute("""UPDATE refresh_logs SET total_records = %s WHERE id = %s""", (total, log_id))
                    main_conn.commit()

                cols = list(data[0].keys())

                # Step 1: Insert into patient_master_staging
                sql = f"INSERT INTO patient_master_staging ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
                vals = [tuple(row[col] for col in cols) for row in data]
                pg_cursor.executemany(sql, vals)                    # Inserts all records in one go using executemany().

                #Step 2: Delete matching records from patient_master
                delete_sql = """
                    DELETE FROM patient_master pm
                    USING patient_master_staging stg
                    WHERE pm.id = stg.id
                """
                pg_cursor.execute(delete_sql)

                # Step 3: Insert all records from staging into patient_master
                insert_main_sql = f"""
                    INSERT INTO patient_master ({', '.join(cols)})
                    SELECT {', '.join(cols)} FROM patient_master_staging
                """
                pg_cursor.execute(insert_main_sql)

                # # Step 4: Backup patient_master into patient_master_bkp (truncate + insert)
                # pg_cursor.execute("TRUNCATE TABLE patient_master_bkp")
                # backup_sql = f"INSERT INTO patient_master_bkp ({cols}) SELECT {cols} FROM patient_master"
                # pg_cursor.execute(backup_sql)

                pg_conn.commit()

                record_tracker[client_id]['inserted'] += len(data)   # Increments the inserted record count for the client.
                client_logger.info(f"Client {client_id} for viewid {view_id}: Inserted {record_tracker[client_id]['inserted']}/{total}")

                if record_tracker[client_id]['inserted'] == total:
                    status = 'success' if record_tracker[client_id]['inserted'] == total else 'failure'
                    log_refresh_end(main_cursor, log_id, status=status, message='All records inserted.', inserted_records=record_tracker[client_id]['inserted'])
                    main_conn.commit()

                    client_logger.info(f"Insertion Complete for Client {client_id} and viewid {view_id}.")
                    del record_tracker[client_id]

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except json.JSONDecodeError as je:
                client_logger.error(f"JSON decode error: {je}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            except Exception as e:
                client_logger.error(f"Error for client {client_id}: {e}")
                if pg_conn:
                    pg_conn.rollback()                                   # Rolls back the transaction (to undo partial insertions).
                
                try:
                    if log_id:
                        log_refresh_end(main_cursor, log_id, status='failure', message=str(e))
                        main_conn.commit()
                except Exception as log_err:
                    client_logger.error(f"Failed to log refresh error: {log_err}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue = True)      # Sends a negative acknowledgment to RabbitMQ (message can be requeued or dropped).

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=response_queue, on_message_callback=callback)

        client_logger.info(f"Consumer ready for client {client_id} on queue {response_queue}")
        channel.start_consuming()

    except Exception as e:
        client_logger.error(f"Consumer setup failed for client {client_id}: {e}")
    finally:
        pg_cursor.close()
        pg_conn.close()
        main_cursor.close()


# Send query to each client
def send_query(client_id, query, rabbitmq_host, rabbitmq_credentials, postgres_config, view_id):
    client_logger = get_client_logger(client_id, view_id)
    try:
        client_logger.info(f"Sending query to client {client_id}")
        main_con = psycopg2.connect(**postgres_config)
        main_cursor = main_con.cursor()
        log_id = log_refresh_start(main_cursor, client_id)
        main_con.commit()

        conn = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=rabbitmq_credentials))
        channel = conn.channel()
        request_queue = f"client_{client_id}_request"
        channel.queue_declare(queue=request_queue, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=request_queue,
            body=json.dumps({'query': query, 'clientid': client_id, 'log_id': log_id, 'viewid': view_id}),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        client_logger.info(f"Sent query to {request_queue} with log_id={log_id}")
        conn.close()
        main_cursor.close()
    except Exception as e:
        client_logger.error(f"Send failed for client {client_id}: {e}")


# Scheduler to check and send queries based on record_time
def schedule_query_dispatch(rabbitmq_host, rabbitmq_credentials, postgres_config):
    try:
        main_con = psycopg2.connect(**postgres_config)
        main_cursor = main_con.cursor()
        while True:
            try:
                now = datetime.now().replace(second=0, microsecond=0).time()
                print(f"Now: {now}")

                # Print record_time from database to debug
                main_cursor.execute("""SELECT record_time FROM client_postgre_credentials_detail WHERE live_flag = true""")
                record_times = main_cursor.fetchall()
                print(f"Record Times in DB: {record_times}")

                main_cursor.execute("""SELECT * FROM client_postgre_credentials_detail WHERE live_flag = true AND record_time = %s""", (now,))
                matched_clients = main_cursor.fetchall()
                print(f"Matched Clients: {matched_clients}")

                columns = [desc[0] for desc in main_cursor.description]

                # Iterates through each matched client.
                for client in matched_clients:
                    client_info = dict(zip(columns, client))
                    client_id = client_info['clientid']
                    query = client_info.get('query')
                    view_id = client_info.get('viewid')
                    if query:
                        p = Process(target=send_query, args=(client_id, query, rabbitmq_host, rabbitmq_credentials, postgres_config, view_id))        # Start a new background thread to call send_query for this client without blocking the scheduler loop.
                        p.start()
                        log_message(f"Scheduled query sent to client {client_id} at {now}")

            except Exception as e:
                log_message(f"Scheduler error: {e}")
                main_con.rollback()

            time.sleep(60)  # wait 1 minutes
    except Exception as e:
        log_message(f"Scheduler setup failed: {e}")

# Refresh log tracking
def log_refresh_start(cursor, client_id, refresh_type='scheduled', rabbitmq_msg_id=None, total_records=0, view_id=None):
    cursor.execute("""
        INSERT INTO refresh_logs (clientid, refresh_type, status, started_at, rabbitmq_message_id, total_records, inserted_records, viewid)
        VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
        RETURNING id
    """, (client_id, refresh_type, 'In Progress', rabbitmq_msg_id, total_records, 0, view_id))
    return cursor.fetchone()[0]

def log_refresh_end(cursor, log_id, status, message=None, inserted_records=0, retry_count=0, view_id=None):
    cursor.execute("""
        UPDATE refresh_logs
        SET status = %s,
            completed_at = NOW(),
            message = %s,
            inserted_records = %s,
            retry_count = %s,
            viewid = coalesce(%s, viewid)
        WHERE id = %s
    """, (status, message, inserted_records, retry_count, view_id, log_id))

# Send an email notification with file upload details
def send_email(email_config):
    smtp_password = 'nbvdmwxsqbgnuepv'
    smtp_connection = smtplib.SMTP(email_config['email_smtp'], email_config['email_port'])
    smtp_connection.starttls()
    smtp_connection.login(email_config['email_sender'], smtp_password)

    # Compose the email message
    sender = 'datatransfer@meditab.com'
    recipient = email_config['email_recipients'].split(',')
    subject = email_config['client_name'] +' || '+ email_config['dashboard_name']+' || '+str(date.today())
    message = "Hello,\n\nThere is an issue with the data Refresh. \n\nThanks & Regards,\n--\nData2Data Team"
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ', '.join(recipient)
    msg['Subject'] = subject
    msg.attach(MIMEText(message))

    # Send the email
    smtp_connection.sendmail(sender, recipient, msg.as_string())
    smtp_connection.quit()

    print('Email sent.')


def check_refresh_logs_and_notify(postgres_config, email_config, query_templates):
    try:
        main_conn = psycopg2.connect(**postgres_config)                   # Main log connection
        main_cursor = main_conn.cursor()
        query = query_templates['refresh_logs']
        main_cursor.execute(query)
        results = main_cursor.fetchall()
        main_cursor.close()
        main_conn.close()

        if results:
            print(f"Found {len(results)} record(s) with 'In Progress' or 'failure'. Sending email...")
            send_email(email_config)
        else:
            print("No 'in-progress' or 'failure' records found.")

    except Exception as e:
            print("Error checking refresh_logs:", e)


def start_log_listener(rabbitmq_host, rabbitmq_credentials, clients, columns, base_log_dir='logs'):
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=rabbitmq_credentials))
        channel = connection.channel()

        # Function to handle incoming log messages
        def callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                client_id = data.get('clientid')
                view_id = data.get('viewid')
                log_filename = data.get('log_filename', 'client.log')
                log_content = data.get('log_content', '')

                # Create a directory for the client's logs if it doesn't exist
                client_log_directory = os.path.join(base_log_dir, f"client_{client_id}")
                os.makedirs(client_log_directory, exist_ok=True)

                # Construct the full log path (with timestamp or daily rotation if desired)
                client_log_path = os.path.join(client_log_directory, 'client.log')

                # Append the content to the server-side log file
                with open(client_log_path, 'a') as log_file:
                    log_file.write(f"\n\n--- Received from viewid: {view_id} at {datetime.now()} ---\n")
                    log_file.write(log_content)

                print(f"Log file received and appended for client {client_id} (viewid: {view_id})")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                print(f"Error handling log message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag)

        # Bind to all client log queues dynamically
        for client in clients:
            client_info = dict(zip(columns, client))
            queue_name = f"client_{client_info['clientid']}_log"
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print("Waiting for log files...")
        channel.start_consuming()

    except Exception as e:
        print(f"Failed to start log listener: {e}")
