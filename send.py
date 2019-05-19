"""send.py

This module sends a message to a RabbitMQ messages queue which contains a path to a sqlite DB, country name and a year.
It is assumed that the message contains 3 parameters separated by comma only in the following way and order: 'Path,Country,Year'
For example: 'C:\sqlite\db\chinook.db,USA,2009'

This script requires that RabbitMW be installed.

This script requires that 'pika' will be installed within the Python
environment you are running this script in.

"""
import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='chinook_queue')

"""Sending a message to the queue"""
channel.basic_publish(exchange='', routing_key='chinook_queue', body='C:\sqlite\db\chinook.db,USA,2009')

connection.close()

