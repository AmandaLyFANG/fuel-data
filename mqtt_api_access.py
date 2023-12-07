# -*- coding: utf-8 -*-
"""
API Access

Original file location:
    https://colab.research.google.com/drive/1sBl7JxvcAzCDUXxfLBhlOn8GOW6vWTEE
DBMS: PostgreSQL (ver. 14.4)
Case sensitivity: plain=lower, delimited=exact
Driver: PostgreSQL JDBC Driver (ver. 42.6.0, JDBC4.2)
Ping: 22 ms
SSL: no

Comment:

    # I have done and tested option1 using data from api.
    # option1:on_message(), option2:on_message1()
    # Both of them are available,
    # However, due to the situation of MQTT server, I cannot test the recall of on_message function.

"""
import psycopg2
import requests
from datetime import datetime
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
import json
import plotly.graph_objs as go
from plotly.subplots import make_subplots

# publish.single("Hello", "World", hostname="10.86.174.51")

AUTHORIZATION = 'anVGVU9jbkdzZEtUTkh2MkxCUmM0Y3d1U2NDNTJxVEY6S3JBbFMxZEJlQTh0QXJyMA=='
API_KEY = 'juFUOcnGsdKTNHv2LBRc4cwuScC52qTF'


def get_access_token():
    token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken?grant_type=client_credentials"
    headers_token = {
        "accept": "application/json",
        "Authorization": f"Basic {AUTHORIZATION}"
    }
    response = requests.get(token_url, headers=headers_token)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print("Failed to obtain access token.")
        exit()


def get_fuel_prices(access_token):
    current_utc = datetime.utcnow()
    formatted_date = current_utc.strftime('%d/%m/%Y %I:%M:%S %p')

    fuel_prices_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"
    headers_fuel_prices = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
        "apikey": f"{API_KEY}",
        "transactionid": str(current_utc.timestamp()),
        "requesttimestamp": formatted_date
    }
    response = requests.get(fuel_prices_url, headers=headers_fuel_prices)

    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch fuel prices.")
        return []


def store_fuel_prices_in_db(data):
    connection = psycopg2.connect(
        dbname="5339",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )

    try:
        cursor = connection.cursor()

        for record in data.get("stations", []):
            cursor.execute("""
                INSERT INTO fuel_prices_api (brand, name, address, latitude, longitude) 
                VALUES (%s, %s, %s, %s, %s)
            """, (record['brand'], record['name'], record['address'], str(record['location']['latitude']),
                  str(record['location']['longitude'])))

        for record in data.get("prices", []):
            cursor.execute("""
                        INSERT INTO api_prices (stationcode, fueltype, price, lastupdated) 
                        VALUES (%s, %s, %s, %s)
                    """, (record['stationcode'], record['fueltype'], record['price'], record['lastupdated']))

        connection.commit()
        print("-------Finished import---------")
    except psycopg2.Error as e:
        print(f"Error storing data: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# Subscribe a topic
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("COMP5339/prices")


# Global variable to store data
data_dict = {}
# Create a Plotly figure
fig = make_subplots(rows=1, cols=1)


# option1
# def on_message(client, userdata, msg):
def on_message(client, userdata, msg):
    print(f"Received message on topic {msg.topic}, {msg.payload}")
    # The payload should be one single station object, like this:
    # {"stationcode": "18712", "fueltype": "E10", "price": 188.7, "lastupdated": "15/10/2023 08:01:47"}
    message = json.loads(msg.payload)
    station = message['stationcode']
    fuel_type = message['fueltype']
    price = message['price']
    timestamp = datetime.strptime(message['lastupdated'], '%d/%m/%Y %H:%M:%S')
    key = (station, fuel_type)
    if key not in data_dict:
        data_dict[key] = {
            'x': [timestamp],
            'y': [price]
        }
        # Add trace to the figure
        fig.add_trace(go.Scatter(x=data_dict[key]['x'], y=data_dict[key]['y'], mode='lines+markers',
                                 name=f'Station {station} - {fuel_type}'))
    else:
        data_dict[key]['x'].append(timestamp)
        data_dict[key]['y'].append(price)
        # Find the correct trace and update it with the new data
        for trace in fig.data:
            if trace.name == f'Station {station} - {fuel_type}':
                trace.x = data_dict[key]['x']
                trace.y = data_dict[key]['y']
                break

    # Refresh the figure
    fig.update_layout(title_text='Real-time fuel prices')
    fig.show()
    # store_fuel_prices_in_db(price_data)


# Store input value
float_prices = []


# option2
def on_message1():
    # print(f"Received message on topic {msg.topic}: {msg.payload}")
    try:
        price = float(input('float price'))
    except ValueError:
        print("Invalid input")
        price = 0
    float_prices.append(price)
    timestamps = list(range(len(float_prices)))
    fig.add_trace(go.Scatter(x=timestamps, y=float_prices, mode='lines+markers', name='Values over time'))
    fig.update_layout(title='Line Chart of Float Values',
                      xaxis_title='Time or Sequence',
                      yaxis_title='Values',
                      template='plotly_dark')
    fig.show()


def message_sending(payload):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("10.86.174.51", 1883, 60)

    client.publish("lfan0920/mqtt", payload)
    client.loop_forever()
    # function test
    # on_message(payload)


def main():
    access_token = get_access_token()
    fuel_data = get_fuel_prices(access_token)
    prices = fuel_data.get("prices", [])
    formatted_date = datetime.utcnow().strftime('%d/%m/%Y')
    filtered_data_today = [item for item in prices if formatted_date in item['lastupdated']]

    print(filtered_data_today[0:20])

    # I have done and tested option1 using data from api.
    # option1:on_message(), option2:on_message1()
    # Both of them are available,
    # However, due to the situation of MQTT server, I cannot test the recall of on_message function.

    # option1
    for data in filtered_data_today:
        message_sending(json.dumps(data))

    # option2
    i = 0
    while i < 10:
        on_message1()
        i += 1


if __name__ == "__main__":
    main()
