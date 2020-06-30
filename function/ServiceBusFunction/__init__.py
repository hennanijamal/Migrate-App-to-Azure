import logging

import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    
    # TODO: Get connection to database
    dbname=os.environ.get(config.BaseConfig.POSTGRES_DB)
    user = os.environ.get(config.BaseConfig.POSTGRES_USER)
    host = os.environ.get(config.BaseConfig.POSTGRES_URL)
    password=os.environ.get(config.BaseConfig.POSTGRES_PW)

    conn = psycopg2.connect(dbname=dbname,
                            user=user,
                            password=password)
    try:
        # TODO: Get notification message and subject fron database using the notification_id
        notf_query = f"select message, subject form notifications where id = {notification_id}"
        cur = conn.cursor()
        cur.execute(notf_query)
        notification_message, subject = cur.fetchall()
        # TODO: Get attendees email and name
        attendees_query = "select email, name from attendee"
        cur = conn.cursor()
        cur.execute(attendees_query)
        # TODO: Loop thru each attendee and send an email with a personalized subject
        attendees = cur.fetchall()
        
        for attendee in attendees:
            message = Mail(
                from_email=os.environ.get(config.getBaseConfig.ADMIN_EMAIL_ADDRESS),
                to_emails=attendee[0],
                subject=subject,
                plain_text_content=notification_message)

        sg = SendGridAPIClient(os.environ.get(config.BaseConfig.SENDGRID_API_KEY))
        sg.send(message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        cur = conn.cursor()
        status = f'{msg.delivery_count} Notifications submitted'
        query = f'update notification set status = {status}, completed_date = {msg.time_to_live} where id = {notification_id}'
        cur.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        if(conn):
            cur.close()
            conn.close()
            print("PostgresSQL connection closed \n")
