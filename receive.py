"""receive.py

This module listens to a RabbitMQ messages queue and once a message is being sent it performs the callback function.
It is assumed that the message contains 3 parameters separated by comma only in the following way and order: 'Path,Country,Year'
It is necessary that ths message will contain a correct sqlite db file Path.
In case tha message will contain only a path (for example: 'C:\sqlite\db\chinook.db,,') the files and the tables will be created with no content

This script requires that RabbitMW and sqlite will be installed.

This script requires that `pika`, 'sqlite3', 'csv', 'json' and 'xml' to be installed within the Python
environment you are running this script in.

This module contains the following functions(notations are added under each one):

    * callback
    * part_1
    * part_2
    * part_3
    * part_4
    * part_5
"""
import pika
import sqlite3
import csv
import json
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element
import xml.etree.ElementTree as etree

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='chinook_queue')

def callback(ch, method, properties, body):
    """ This function will be performed once a message is being sent"""
    message = str(body, 'utf-8')
    try:
        path, country, year = message.split(",")
        try:
            conn = sqlite3.connect(path)
            c = conn.cursor()
            part_1(c, country)
            part_2(c, country)
            part_3(c, country)
            part_4(c, country, year)
            part_5(c, conn, country, year)

        except:
            print('DB path is missing or incorrect. please resend a correct path.')

        finally:
            conn.close()
    except:
        print("Please send the 3 parameters separated by comma only in the following way and order: 'Path,Country,Year'")


def part_1(c, country):
    """ This function creates a csv file with the country that was sent in the message along with its Number of purchases"""
    data = c.execute("SELECT BillingCountry as Country, count(*) as NumOfPurchases "
                     "FROM invoices WHERE BillingCountry='%s' GROUP BY BillingCountry" % country)

    with open('Purchases_Per_Country.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in c.description])
        writer.writerows(data)
        f.close()

def part_2(c, country):
    """ This function creates a csv file with the country that was sent in the message along with its Number of purchased items"""
    data2 = c.execute("SELECT BillingCountry as Country ,sum(Quantity) as NumOfPurchasedItems "
                      "FROM invoice_items IT join invoices I on IT.InvoiceID=I.InvoiceID "
                      "WHERE BillingCountry='%s' GROUP BY BillingCountry" % country)

    with open('Items_Per_Country.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in c.description])
        writer.writerows(data2)
        f.close()

def part_3(c, country):
    """ This function creates a json file with the country that was sent in the message along with its purchused albums list"""
    data3 = c.execute(
        "select distinct A.Title from albums A join tracks T on A.AlbumId=T.AlbumId "
        "join invoice_items IT on T.TrackId=IT.TrackId join invoices I on IT.InvoiceId=I.InvoiceId "
        "WHERE BillingCountry='%s'" % country)

    all_rows = c.fetchall()

    albums = {}
    albums["Country"] = country
    albums["albums"] = all_rows

    file3 = open('Albums_At_Country.txt', 'w', encoding="utf-8")
    json.dump(albums, file3,  indent=2, ensure_ascii=False)
    file3.close()


def part_4(c, country, year):
    """ This function creates a xml file with the country, track, year and Purchased_Amount_Per_Year for the most sold track in the country and since the year that were sent in the message"""
    data4 = c.execute(
        "select a.Name, a.BillingCountry as Country,sum(Quantity) AS Purchased_Amount_Per_Year, strftime('%Y',b.InvoiceDate) as Year "
        "from (select  T.Name, I.BillingCountry, sum(quantity) AS Total_Purchased_Amount from  tracks T "
        "join genres G on T.GenreId=G.GenreId join invoice_items IT on T.TrackId=IT.TrackId join invoices I on IT.InvoiceId=I.InvoiceId "
        "where strftime('%Y',I.InvoiceDate) >= '{yn}' "
        "and G.Name='Rock' "
        "and I.BillingCountry='{cn}' "
        "Group by  T.Name, I.BillingCountry "
        "ORDER BY Total_Purchased_Amount DESC LIMIT 1) a "
        "left join "
        "(select * from invoice_items IT join invoices I on IT.InvoiceId=I.InvoiceId join tracks T on T.TrackId=IT.TrackId ) b "
        "on a.Name= b.name AND a.BillingCountry=b.BillingCountry "
        "group by strftime('%Y',b.InvoiceDate)".format(yn=year, cn=country))

    root = Element('data')
    tree = ElementTree(root)
    columns = [i[0] for i in c.description]
    all_rows = c.fetchall()

    for row in all_rows:
        for i in range(4):
            name = Element(columns[i])
            root.append(name)
            name.text = str(row[i])

    the_data = str(etree.tostring(root))
    file4 = open("Most_sold.xml", "w", newline='')
    file4.write(the_data)
    file4.close()

def part_5(c, conn, country, year):
    """ This function creates 3 new tables in the database along with the data from 1,2,4 respectively"""

    c.execute('''DROP TABLE if exists Purchases_Per_Country;''')

    c.execute('''CREATE TABLE if not exists Purchases_Per_Country
             (Country        TEXT     ,
              NumOfPurchases INT    );''')

    data = c.execute(
        "SELECT BillingCountry, count(*) FROM invoices WHERE BillingCountry='%s' GROUP BY BillingCountry" % country)

    all_rows = c.fetchall()
    for row in all_rows:
        c.execute("INSERT INTO Purchases_Per_Country (Country,NumOfPurchases) VALUES ('%s', %s)" % (row[0], row[1]))

    conn.commit()

    c.execute('''DROP TABLE if exists Items_Per_Country;''')

    c.execute('''CREATE TABLE if not exists Items_Per_Country
             (Country        TEXT ,
              NumOfPurchasedItems INT    );''')

    data2 = c.execute("SELECT BillingCountry,sum(Quantity) FROM invoice_items IT join invoices I "
                      "on IT.InvoiceID=I.InvoiceID "
                      "WHERE BillingCountry='%s' GROUP BY BillingCountry" % country)

    all_rows = c.fetchall()
    for row in all_rows:
        c.execute("INSERT INTO Items_Per_Country (Country,NumOfPurchasedItems) VALUES ('%s', %s)" % (row[0], row[1]))

    conn.commit()

    c.execute('''DROP TABLE if exists Most_sold;''')

    c.execute('''CREATE TABLE if not exists Most_sold
             (Name        TEXT     ,
              Country     TEXT     ,
              Purchased_Amount_Per_Year INT,
              Year        TEXT      );''')

    data4 = c.execute(
        "select a.Name, a.BillingCountry as Country,sum(Quantity) AS Purchased_Amount_Per_Year, strftime('%Y',b.InvoiceDate) as Year "
        "from (select  T.Name, I.BillingCountry, sum(quantity) AS Total_Purchased_Amount from  tracks T "
        "join genres G on T.GenreId=G.GenreId join invoice_items IT on T.TrackId=IT.TrackId join invoices I on IT.InvoiceId=I.InvoiceId "
        "where strftime('%Y',I.InvoiceDate) >= '{yn}' "
        "and G.Name='Rock' "
        "and I.BillingCountry='{cn}' "
        "Group by  T.Name, I.BillingCountry "
        "ORDER BY Total_Purchased_Amount DESC LIMIT 1) a "
        "left join "
        "(select * from invoice_items IT join invoices I on IT.InvoiceId=I.InvoiceId join tracks T on T.TrackId=IT.TrackId ) b "
        "on a.Name= b.name AND a.BillingCountry=b.BillingCountry "
        "group by strftime('%Y',b.InvoiceDate)".format(yn=year, cn=country))

    all_rows = c.fetchall()
    for row in all_rows:
        c.execute(
            "INSERT INTO Most_sold (Name, Country, Purchased_Amount_Per_Year, Year) VALUES ('%s', '%s', %s, '%s' )" % (
            row[0], row[1], row[2], row[3]))

    conn.commit()


"""consuming a message from the queue"""
channel.basic_consume(queue='chinook_queue', on_message_callback=callback, auto_ack=True)


print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()



