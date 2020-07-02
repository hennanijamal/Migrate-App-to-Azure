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
    dbname="techconfdb"
    user = "techconadmin@techcondb"
    host = "techcondb.postgres.database.azure.com"
    password="@Udacity"

    conn = psycopg2.connect(host=host,
                            dbname=dbname,
                            user=user,
                            password=password)
    try:
        # TODO: Get notification message and subject fron database using the notification_id
        cur = conn.cursor()
        notf_query = f"select message, subject form notifications where id = {notification_id}"
        cur.execute(notf_query)
        notification_message, subject = cur.fetchall()
        # TODO: Get attendees email and name
        attendees_query = "select email, name from attendee"
        cur.execute(attendees_query)
        # TODO: Loop thru each attendee and send an email with a personalized subject
        attendees = cur.fetchall()
        
        if not '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"':
            for attendee in attendees:
                message = Mail(
                    from_email="info@techconf.com",
                    to_emails=attendee[0],
                    subject=subject,
                    plain_text_content=notification_message)

            sg = SendGridAPIClient('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"')
            sg.send(message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        status = f'Notified {msg.delivery_count} attendees'
        query = f'update notification set status = {status}, completed_date = {datetime.utcnow()} where id = {notification_id}'
        cur.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        if(conn):
            conn.commit()
            cur.close()
            conn.close()
            print("PostgresSQL connection closed \n")
