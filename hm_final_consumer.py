# Student: Hayley M
# Date: 08Jun24

# Message listener 

# Basic imports to run code
import pika
import sys
import os
import time
import math
import webbrowser
import traceback
from collections import deque
from datetime import datetime

from util_logger import setup_logger

logger, logname = setup_logger(__file__)

def offer_rabbitmq_admin_site():
   """Offer to open the RabbitMQ Admin website."""
   ans = input("Would you like to monitor RabbitMQ queues? y or n ")
   print()
   if ans.lower() == "y":
       webbrowser.open_new("http://localhost:15672/#/queues")
       logger.info("Opened RabbitMQ")

# Define the deques and window
smokerA_deque = deque(maxlen=5)

smoker_drop_threshold = 15.0

# Define gas_eruo callback
def check_smoker_alert():
    
    if len(smokerA_deque) == smokerA_deque.maxlen:
        initial_temp = smokerA_deque[0][1]
        latest_temp = smokerA_deque[-1][1]
        if initial_temp - latest_temp >= smoker_drop_threshold:
            alert_message = f" [!] Smoker Alert! Temp dropped by {initial_temp - latest_temp}F in 2.5 minutes."
            print(alert_message)
            logger.info(alert_message)          

def consumer():
    """ Continuously listen for task messages on named queues."""
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()

        queues = ["smokerA", "jackfruit", "pineapple"]
        for queue_name in queues:
            channel.queue_declare(queue=queue_name, durable=True)

        def callback(ch, method, properties, body):
            """Define behavior on getting a message."""
            message = eval(body.decode())
            timestamp, temp = message
            timestamp = datetime.strptime(timestamp, '%m/%d/%y %H:%M:%S')

            if method.routing_key == "smokerA":
                smokerA_deque.append((timestamp, temp))
                check_smoker_alert()
            elif method.routing_key == "jackfruit":
                jackfruit_deque.append((timestamp, temp))
                jackfruit_stall(jackfruit_deque, "jackfruit")
            elif method.routing_key == "pineapple":
                pineapple_deque.append((timestamp, temp))
                pineapple_stall(pineapple_deque, "pineapple")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        for queue_name in queues:
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Connection error: {e}")
    except pika.exceptions.AMQPChannelError as e:
        logger.error(f"Channel error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if connection and not connection.is_closed:
            connection.close()

if __name__ == "__main__":
    try:
        offer_rabbitmq_admin_site()
        consumer()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
