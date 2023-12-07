# data gatherer that uses the fuel API to fetch the most recent price data from Data NSW.
import requests
from datetime import datetime, timedelta
import time
import paho.mqtt.publish as publish
import json
from config import AUTHORIZATION, host_name, API_KEY

push_to_price = "lfan0920/mqtt/api_data/price"
push_to_station = "lfan0920/mqtt/api_data/station"
client_id = "Data Gatherer"


# The get_access_token() function is responsible for obtaining an access token from the NSW API,
# enabling subsequent API requests. It uses HTTP Basic Authentication to exchange the provided credentials for a token.
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


# the get_fuel_prices() function uses the access token to request the most recent fuel prices.
# It prepares a properly formatted date and includes required headers,
# handling the API's authorization and content-type requirements.
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


# The data_publish() function takes the retrieved data and performs filtering to extract the most recent records from
# the last seven days. It then publishes the data to predefined MQTT topics in batches, reducing the likelihood of
# overloading the broker or the network. To manage the load, the script publishes records in batches, pausing briefly
# after each to ensure a steady flow of data without overwhelming the system.
def data_publish(data):
    # filter most recent price
    prices = data.get("prices", [])
    current_date = datetime.utcnow()
    one_month_ago = current_date - timedelta(days=30)
    filtered_prices = [item for item in prices if datetime.strptime(item['lastupdated'], '%d/%m/%Y %H:%M:%S') >= one_month_ago]
    # sort it by time
    filtered_prices = sorted(filtered_prices, key=lambda x: datetime.strptime(x['lastupdated'], "%d/%m/%Y %H:%M:%S"))
    stations = data.get('stations')

    print('price length:', len(filtered_prices), 'station length:', len(stations))
    batch_size = 500
    count = 0
    for record in stations:
        publish.single(push_to_station, json.dumps(record), hostname=host_name, client_id=client_id, keepalive=60)
    for record in filtered_prices:
        publish.single(push_to_price, json.dumps(record), hostname=host_name, client_id=client_id, keepalive=60)
        count += 1
        if count % batch_size == 0:
            time.sleep(3)  # Sleep for 3 seconds after every 500 records

    print('finish data_publish')


def main():
    access_token = get_access_token()
    fuel_data = get_fuel_prices(access_token)
    data_publish(fuel_data)


if __name__ == "__main__":
    main()
