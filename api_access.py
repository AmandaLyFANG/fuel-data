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
"""
import psycopg2
import requests
from datetime import datetime

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


def main():
    access_token = get_access_token()
    fuel_data = get_fuel_prices(access_token)
    store_fuel_prices_in_db(fuel_data)


if __name__ == "__main__":
    main()
