"""
Student: Hayley M
Date: 11Jun24

Message sender / emitter /producer

Description:
Create channels or 3 different queues for our temperatures for each producer that creates a temperature.

"""
# Imports from standard Library

import csv
import pika
import sys
import webbrowser
import traceback  

# Call setup_logger to initialize logging
from util_logger import setup_logger
logger, log_file_name = setup_logger(__file__)

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website.

    This function prompts the user to open the RabbitMQ Admin website.
    If the user answers 'y', it opens the web browser to the RabbitMQ Admin site.
    """
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def main_work():
    """Perform the main work of the program.

    This function connects to RabbitMQ, deletes existing queues, and declares new ones.
    It then processes a CSV file and sends messages to RabbitMQ queues based on the data in the file.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()

        
        queues = ["smokerA", "jackfruit", "pineapple"]
        for queue_name in queues:
            channel.queue_delete(queue=queue_name)
            channel.queue_declare(queue=queue_name, durable=True)

        # Process CSV and send messages to RabbitMQ queues
        csv_file_path = "C:\\Users\\Hayley\\Documents\\streaming-06-smart-smoker\\smoker-temps.csv"
        with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for data_row in reader:
                timestamp = data_row['Time (UTC)']
                smoker_temp_str = data_row['Channel1']
                jackfruit_temp_str = data_row['Channel2']
                pineapple_temp_str = data_row['Channel3']

                if smoker_temp_str:
                    smoker_temp = float(smoker_temp_str)
                    send_message(channel, "smokerA", (timestamp, smoker_temp))
                    logger.info(f" [x] Smoker Temperature is {smoker_temp}")

                if jackfruit_temp_str:
                    jackfruit_temp = float(jackfruit_temp_str)
                    send_message(channel, "jackfruit", (timestamp, jackfruit_temp))
                    logger.info(f" [x] Food A Temperature is {jackfruit_temp}")

                if pineapple_temp_str:
                    pineapple_temp = float(pineapple_temp_str)
                    send_message(channel, "pineapple", (timestamp, pineapple_temp))
                    logger.info(f" [x] Food A Temperature is {pineapple_temp}")


    except FileNotFoundError:
        print("CSV file not found.")
    except ValueError as e:
        print(f"Error processing CSV: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()  # Print the traceback for detailed error information
    finally:
        connection.close()

def send_message(channel, queue_name, message):
    """Publish a message to the specified queue.

    Parameters:
        channel (pika.channel.Channel): The communication channel to RabbitMQ.
        queue_name (str): The name of the RabbitMQ queue to publish the message to.
        message (tuple): The message to be published.
    """
    try:
        channel.basic_publish(exchange="", routing_key=queue_name, body=str(message))
        print(f"Sent message to {queue_name}: {message}")
    except Exception as e:
        print(f"Error sending message to {queue_name}: {e}")

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    main_work()