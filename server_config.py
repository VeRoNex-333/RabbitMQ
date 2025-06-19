#Server Side Config file

postgres_config = {
    "host":"",
    "port":"",
    "dbname":"", 
    "user":"", 
    "password":""
}


rabbitmq_config = {
    "host":"",
    "user":"",
    "password":""
}


email_config = {
    "email_recipients": "",
    "dashboard_name": "",
    "client_name": "",
    "smtp_encrypted_password": "",  #Encryted app password
    "smtp_username": "",
    "email_smtp": "",
    "email_port": ,
    "email_sender": ""
}


query_templates = {
    "active_client_query": "SELECT * FROM client_postgre_credentials_detail WHERE live_flag = true;",
    "refresh_logs": "SELECT * FROM refresh_logs WHERE status IN ('In Progress', 'failure');"
}


