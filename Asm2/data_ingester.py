# data ingester that receives cleaned data items and adds them to a database
import psycopg2
import json
import paho.mqtt.client as mqtt
from config import AUTHORIZATION, host_name, API_KEY

broker_address = host_name
receive_clean_price_from = 'lfan0920/mqtt/clean_data/price'
receive_clean_station_from = 'lfan0920/mqtt/clean_data/station'
dbname = "5339"
user = "postgres"
password = "1234"
host = "localhost"
port = "5432"

connection = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cursor = connection.cursor()


def insert_station_data(data):
    cursor.execute(
        """
        INSERT INTO station (stationid, brandid, brand, code, name, address, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (code) DO UPDATE SET
        stationid = EXCLUDED.stationid,
        brandid = EXCLUDED.brandid,
        brand = EXCLUDED.brand,
        name = EXCLUDED.name,
        address = EXCLUDED.address,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude;
        """,
        (data['stationid'], data['brandid'], data['brand'], data['code'], data['name'], data['address'], data['location']['latitude'], data['location']['longitude'])
    )
    connection.commit()


def insert_price_data(data):
    cursor.execute("SELECT 1 FROM station WHERE code = %s", (data['stationcode'],))
    if cursor.fetchone() is None:
        # The stationcode does not exist, skip the price data
        print('station not exist')
    else:
        # The stationcode exists, safe to insert the price data
        cursor.execute(
            """
            INSERT INTO price (stationcode, fueltype, price, lastupdated)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stationcode, fueltype, lastupdated) DO UPDATE SET
            price = EXCLUDED.price;
            """,
            (data['stationcode'], data['fueltype'], data['price'], data['lastupdated'])
        )
        connection.commit()


def initial_tables():
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS station (
                code VARCHAR PRIMARY KEY,
                stationid VARCHAR,
                brandid VARCHAR,
                brand VARCHAR,
                name VARCHAR,
                address VARCHAR,
                latitude FLOAT,
                longitude FLOAT
            );
        """)
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS price (
                price_id SERIAL PRIMARY KEY,
                stationcode VARCHAR,
                fueltype VARCHAR,
                price FLOAT,
                lastupdated TIMESTAMP,
                FOREIGN KEY (stationcode) REFERENCES station(code),
                UNIQUE (stationcode, fueltype, lastupdated)
            );
        """)
    connection.commit()


# MQTT Client Setup
def mqtt_setup():
    client = mqtt.Client("DataIngester")
    client.connect(broker_address)
    client.subscribe([(receive_clean_price_from, 0), (receive_clean_station_from, 0)])
    client.on_message = on_message
    try:
        print("Data Ingester running")
        client.loop_forever()
    except KeyboardInterrupt:
        print("Stopping Data Ingester")
    # close
    client.disconnect()


def on_message(client, userdata, message):
    print('topic:', message.topic)
    print('message:', message.payload)
    data = json.loads(message.payload.decode('utf-8'))
    if message.topic == receive_clean_station_from:
        insert_station_data(data)
    elif message.topic == receive_clean_price_from:
        insert_price_data(data)
        # print('haha')


def main():
    mqtt_setup()
    initial_tables()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()

