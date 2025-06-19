import psycopg2
from server_functions import load_config, get_clients, schedule_query_dispatch, consume_for_client, decrypt_password, check_refresh_logs_and_notify, start_log_listener
import pika
from cryptography.fernet import Fernet
from multiprocessing import Process

if __name__ == '__main__':
    # Encryption key and cipher setup
    ENCRYPTION_KEY = b''
    cipher = Fernet(ENCRYPTION_KEY)

    # Load config and connect to the main postgres database
    postgres_config, rabbitmq_config, email_config, query_templates = load_config()
    postgres_config["password"] = decrypt_password(postgres_config["password"], cipher)
    main_connection = psycopg2.connect(**postgres_config)       ## ** will unpack the dictionary
    main_cursor = main_connection.cursor()

    # Get RabbitMQ credentials
    rabbitmq_host, rabbitmq_user, rabbitmq_pass = rabbitmq_config['host'], rabbitmq_config['user'], decrypt_password(rabbitmq_config['password'], cipher)
    rabbitmq_credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)

    # Get clients
    clients, columns = get_clients(main_cursor, query_templates)

    # Start the log listener process
    log_listener_process = Process(target=start_log_listener,args=(rabbitmq_host, rabbitmq_credentials,clients, columns))
    log_listener_process.start()


    # Start consumer processes
    consumer_processes = []
    for client in clients:                              # Each client is a tuple of values
        client_info = dict(zip(columns, client))
        process = Process(target=consume_for_client, args=(client_info['clientid'], client_info, rabbitmq_host, rabbitmq_credentials, postgres_config, client_info['viewid']))
        process.start()
        consumer_processes.append(process)

    check_refresh_logs_and_notify(postgres_config, email_config, query_templates)

    # Start scheduler
    schedule_query_dispatch(rabbitmq_host, rabbitmq_credentials, postgres_config)

