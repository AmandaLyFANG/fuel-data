'''
data cleaner that examines the incoming data, cleans it and republishes
'''
from datetime import datetime
import json
import paho.mqtt.client as mqtt
import psycopg2
from paho.mqtt import publish
from config import AUTHORIZATION, host_name, API_KEY


receive_price_from = 'lfan0920/mqtt/api_data/price'
receive_station_from = 'lfan0920/mqtt/api_data/station'
send_price_to = 'lfan0920/mqtt/clean_data/price'
send_station_to = 'lfan0920/mqtt/clean_data/station'
send_combined_to = 'lfan0920/mqtt/clean_data/combined'

stations = []

dbname = "5339"
user = "postgres"
password = "1234"
host = "localhost"
port = "5432"
# Establish a connection to the database
connection = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# Create a cursor object
cursor = connection.cursor()
# Fetches the latest update time from the database to filter out outdated price data.
latest_update_time = ''

'''
This combine_information() will find matching station by ‘stationcode’ in cleansed price data, and combined them together. To ensure the reliability of subsequent procedures, unmatched data will be discarded.
'''


def combine_information(price_dict):
    # Find the station with the matching code
    for station in stations:
        if station['code'] == price_dict['stationcode']:
            # Combine the two dictionaries
            combined_dict = {**price_dict, **station}
            # Remove the 'stationcode' key
            combined_dict.pop('stationcode', None)
            return combined_dict
    print('cannot find matching station:', str(price_dict))
    return None


# Function to handle incoming messages
def on_message(client, userdata, message):
    print('topic:', message.topic)
    print('message:', message.payload)
    raw_data = json.loads(message.payload.decode("utf-8"))
    if message.topic == receive_station_from:
        cleaned_data = clean_station_data(raw_data)
        if cleaned_data:
            publish.single(send_station_to, json.dumps(cleaned_data), hostname=host_name)
    elif message.topic == receive_price_from:
        global latest_update_time
        cleaned_data = clean_price_data(raw_data)
        if cleaned_data:
            up_time = datetime.strptime(cleaned_data['lastupdated'], "%d/%m/%Y %H:%M:%S")
            # drop expired data
            if latest_update_time <= up_time:
                latest_update_time = up_time
                publish.single(send_price_to, json.dumps(cleaned_data), hostname=host_name)
                # data combine, drop data without station
                combined = combine_information(cleaned_data)
                if combined:
                    with open('received_data.json', 'a') as file:
                        file.write(json.dumps(combined) + ',\n')
                    publish.single(send_combined_to, json.dumps(combined), hostname=host_name)


def clean_price_data(entry):
    # Define the price threshold, for example, price should be between 0 and 1000
    PRICE_THRESHOLD = (0, 1000)
    # Check price for correct type and within the threshold
    try:
        price = float(entry['price'])
        if not (PRICE_THRESHOLD[0] <= price <= PRICE_THRESHOLD[1]):
            return None  # Price out of threshold
    except (ValueError, TypeError):
        print('price out of threshold', str(entry))
        return None  # Price is not a float

    # Check lastupdated for correct format
    try:
        # Attempt to parse the lastupdated string to a datetime object
        datetime.strptime(entry['lastupdated'], "%d/%m/%Y %H:%M:%S")
    except ValueError:
        print('Value Error of price data', str(entry))
        return None  # Lastupdated is not a valid datetime

    # Check for null values in all fields
    for key, value in entry.items():
        if value is None or value == '':
            print('Null value found', str(entry))
            return None  # Null value found

    # Return the cleaned entry if all checks pass
    return entry


# The clean_station_data() function is responsible for stations data cleaning,
# it ensures latitude and longitude data are of the correct type and handles null values in the station data.
# The cleansed data will be republishing it for future use. The data repeatability verification will be performed during
# the process of storing into the database.
def clean_station_data(entry):
    # Check latitude and longitude for correct type
    try:
        if entry['location']:
            entry['location']['latitude'] = float(entry['location']['latitude'])
            entry['location']['longitude'] = float(entry['location']['longitude'])
    except (ValueError, TypeError):
        print('Value Error or TypeError of station data', str(entry))
        return None  # Latitude or Longitude is not a float

    # Check for null values in all fields
    for key, value in entry.items():
        if key not in ['brandid', 'stationid'] and (value is None or value == ''):
            print('Null value found')
            return None  # Null value found

    # Return the cleaned entry if all checks pass
    return entry


def mqtt_setting():
    client = mqtt.Client("DataCleaner")
    client.on_message = on_message
    client.connect("127.0.0.1", 1883, 60)
    client.subscribe([(receive_price_from, 0), (receive_station_from, 0)])
    print("Data Cleaner running")
    client.loop_forever()


def get_latest_update_time():
    # Query to get the latest update time from the price table
    cursor.execute("SELECT MAX(lastupdated) FROM price;")
    # Fetch the result
    result = cursor.fetchone()
    # Check if result is not None and extract the latest update time
    latest_datetime = result[0] if result[0] is not None else None
    # Return the latest_datetime
    return latest_datetime


# Retrieves all station information from the database.
def get_all_stations():
    # Query to select all stations
    query = "SELECT * FROM station;"
    try:
        # Execute the query
        cursor.execute(query)
        # Fetch all records
        records = cursor.fetchall()
        # Convert records to a list of dictionaries
        return_stations = []
        for record in records:
            station = {
                'code': record[0],
                'stationid': record[1],
                'brandid': record[2],
                'brand': record[3],
                'name': record[4],
                'address': record[5],
                'latitude': record[6],
                'longitude': record[7]
            }
            return_stations.append(station)
        return return_stations
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()


def main():
    global stations, latest_update_time
    query_time = get_latest_update_time()
    if query_time:
        latest_update_time = get_latest_update_time()
    else:
        # datetime initialize
        latest_update_time = datetime(2023, 1, 1, 12, 00, 00)
    stations = get_all_stations()
    mqtt_setting()


if __name__ == "__main__":
    main()
