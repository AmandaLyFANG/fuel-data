# data dashboard that receives cleanded data items and updates a visualisation
# https://github.com/randyzwitch/streamlit-folium
import json
import time
from datetime import datetime, timedelta

import folium
import psycopg2
from streamlit_folium import st_folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
import altair as alt
import paho.mqtt.client as mqtt
import plotly.express as px
from config import host_name

# MQTT topics
receive_clean_price_from = 'lfan0920/mqtt/clean_data/price'
receive_clean_station_from = 'lfan0920/mqtt/clean_data/station'
receive_clean_combined_from = 'lfan0920/mqtt/clean_data/combined'
broker_address = host_name
combined = []
combined_df = pd.DataFrame()
latest_data = []
start_number = 10
data_received = False


# MQTT message handler
def on_message(client, userdata, message):
    print('topic:', message.topic)
    print('message:', message.payload)
    data = json.loads(message.payload.decode('utf-8'))
    if message.topic == receive_clean_combined_from:
        latest_data.append(data)
        # global start_number
    # if len(combined) > start_number:
    #     combined_df = fetch_latest_data()
    #     build_dashboard(combined_df)
    #     st.rerun()


def build_dashboard():
    print('build_dashboard refresh')
    global combined_df
    # Set the page configuration
    st.set_page_config(page_title='Fuel Price Dashboard', layout='wide')

    # Use markdown with HTML to center the title
    st.markdown("<h1 style='text-align: center;'>Fuel Price Dashboard</h1>", unsafe_allow_html=True)
    # Display key metrics at the top of the dashboard
    st.header("Key Metrics")
    try:
        avg_price = combined_df['price'].mean()
    except KeyError:
        print('No value yet')
    cheapest_fuel = combined_df.loc[combined_df['price'].idxmin()]

    combined_df_copy = combined_df.copy()
    combined_df_copy['lastupdated'] = pd.to_datetime(combined_df_copy['lastupdated'], format='%d/%m/%Y %H:%M:%S', )
    num_stations = len(list(combined_df_copy['code'].unique()))
    combined_df_today = combined_df_copy[
        combined_df_copy['lastupdated'].dt.date == datetime.now().date()]
    avg_price_today = combined_df_today['price'].mean()
    num_stations_today = len(list(combined_df_today['code'].unique()))
    cheapest_fuel_today = combined_df_today.loc[combined_df_today['price'].idxmin()]

    # Convert the 'price' column to numeric
    combined_df['price'] = pd.to_numeric(combined_df['price'])
    combined_df['lastupdated'] = pd.to_datetime(combined_df['lastupdated'], format='%d/%m/%Y %H:%M:%S')

    average_day = combined_df.groupby([combined_df['lastupdated'].dt.date, 'fueltype'])[
        'price'].mean().unstack().reset_index()

    # First row with two charts
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Average Fuel Price in Past one month", f"${avg_price:.2f}/L")
    with col2:
        st.metric("Stations Reporting", num_stations)
    with col3:
        st.metric("Cheapest Fuel in Past one month", f"${cheapest_fuel['price']:.2f}/L - {cheapest_fuel['fueltype']}")

    col1_today, col2_today, col3_today = st.columns(3)
    with col1_today:
        st.metric("Today's Average", f"${avg_price_today:.2f}/L")
    with col2_today:
        st.metric("Today's Stations Reporting", num_stations_today)
    with col3_today:
        st.metric("Today's Cheapest", f"${cheapest_fuel_today['price']:.2f}/L - {cheapest_fuel_today['fueltype']}")

    # Second row with two charts
    col4, col5 = st.columns(2)
    with col4:
        # Filters as part of the main page, not the sidebar
        geographical = combined_df.copy()
        geographical_sorted = geographical.sort_values(by=['code', 'fueltype', 'lastupdated'],
                                                     ascending=[True, True, False])
        latest_prices = geographical_sorted.drop_duplicates(subset=['code', 'fueltype', 'lastupdated'])
        latest_prices = latest_prices[['lastupdated', 'fueltype', 'price', 'longitude', 'latitude']]
        m1 = folium.Map(location=[-33.869, 151.209], zoom_start=12)
        # Create a marker cluster.
        marker_cluster = MarkerCluster().add_to(m1)
        # Create a color dictionary for each MainComponent.
        color_dict = {
            'DL': 'blue',
            'E10': 'green',
            'LPG': 'orange',
            'P95': 'beige',
            'P98': 'purple',
            'PDL': 'gray',
            'U91': 'lightgreen'
            # Add more colors for other MainComponents if needed.
        }
        # Add markers to the map.
        for idx, row in latest_prices.iterrows():
            popup_text = f"Fuel type: {row['fueltype']}<br>Price: {row['price']:.2f}<br>Lastupdated At:{row['lastupdated']}"
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=popup_text,
                icon=folium.Icon(color=color_dict.get(row['fueltype'], 'white'))
                # default to yellow if MainComponent not in color_dict
            ).add_to(marker_cluster)
        st_folium(m1, width=1800)
    with col5:
        # Create a multiselect box for users to choose fuel types to visualize
        combined_df_dateformat = combined_df.copy()
        combined_df_dateformat['lastupdated'] = pd.to_datetime(combined_df_dateformat['lastupdated'],
                                                               format='%d/%m/%Y %H:%M:%S', errors='coerce')
        combined_df_dateformat['lastupdated'] = combined_df_dateformat['lastupdated'].dt.date
        selected_fuel_types = st.multiselect(
            "Choose fuel types", options=combined_df['fueltype'].unique(),
            default=combined_df_dateformat['fueltype'].unique()
        )
        # Filter data based on selected fuel types
        filtered_data = combined_df_dateformat[combined_df_dateformat['fueltype'].isin(selected_fuel_types)]
        # Pivot the data to have dates as rows and fuel types as columns with prices as values
        pivoted_data = filtered_data.pivot_table(index='lastupdated', columns='fueltype', values='price')
        # Reset the index to make 'lastupdated' a column again
        pivoted_data = pivoted_data.reset_index()
        # Melting the DataFrame to have a suitable form for Altair
        data_to_plot = pivoted_data.melt('lastupdated', var_name='fueltype', value_name='price')
        # Create the chart
        chart = alt.Chart(data_to_plot, height=700).mark_line(point=True).encode(
            x=alt.X('lastupdated:T', axis=alt.Axis(title='Date')),
            y=alt.Y('price:Q', axis=alt.Axis(title='Price')),
            color='fueltype:N',
            tooltip=['lastupdated', 'price', 'fueltype']
        ).interactive()
        # Display the chart in the Streamlit app
        st.altair_chart(chart, use_container_width=True)
        # Handle cases where no data is available after filtering
        if data_to_plot.empty:
            st.error("No data available for the selected fuel types. Please select different fuel types.")

    col6, col7 = st.columns(2)
    with col6:
        col6_1, col6_2 = st.columns(2)
        with col6_1:
            # Comparative Analysis Section
            st.header("Comparative Analysis")
            # Bar Chart
            bar_chart = alt.Chart(combined_df).mark_bar().encode(
                x='fueltype:N',
                y='average(price):Q',
                color='fueltype:N'
            )
            st.altair_chart(bar_chart, use_container_width=True)
        with col6_2:
            # Process the data to count occurrences of each fuel type
            fuel_types = combined_df['fueltype'].value_counts().reset_index()
            fig = px.pie(fuel_types, values='count', names='fueltype', title='Fuel Type Distribution')
            # Use Streamlit to render the pie chart
            st.plotly_chart(fig)
    with col7:
        col7_1, col7_2 = st.columns(2)
        with col7_1:
            st.header("Detailed Average Price past one month")
            st.dataframe(average_day, width=1200)  # Simple representation
        with col7_2:
            st.header("Detailed Average Price of Today")
            average_df_today = combined_df_today.groupby([combined_df['lastupdated'].dt.date, 'fueltype'])[
                'price'].mean().reset_index()
            st.dataframe(average_df_today, width=700)  # Simple representation


def mqtt_connect():
    # MQTT Client Setup
    client = mqtt.Client("DataDashboard")
    client.connect(broker_address)
    client.subscribe([(receive_clean_combined_from, 0)])
    client.on_message = on_message
    try:
        print("Data Dashboard running")
        client.loop_start()
    except KeyboardInterrupt:
        print("Stopping Data Dashboard")


def data_initialize():
    dbname = "5339"
    user = "postgres"
    password = "1234"
    host = "localhost"
    port = "5432"
    # Set up your database connection
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Calculate the date 7 days ago from today
    now = datetime.now()
    midnight_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    one_month_ago = midnight_today - timedelta(days=30)

    # The SQL query to get the past seven days' prices for each fuel type at each station
    query = """
    SELECT 
        p.fueltype, 
        p.price, 
        p.lastupdated, 
        s.code, 
        s.stationid, 
        s.brandid, 
        s.brand, 
        s.name, 
        s.address, 
        s.latitude, 
        s.longitude
    FROM 
        station s
    JOIN 
        price p ON s.code = p.stationcode
    WHERE 
        p.lastupdated > %s
    ORDER BY 
        s.code, 
        p.fueltype, 
        p.lastupdated DESC;
    """

    # Execute the query with the date parameter
    cursor.execute(query, (one_month_ago,))

    # Fetch all the results
    results = cursor.fetchall()

    # Convert the results to the desired JSON structure
    json_results = [
        {
            "fueltype": result[0],
            "price": result[1],
            "lastupdated": result[2].strftime("%d/%m/%Y %H:%M:%S"),
            "code": result[3],
            "stationid": result[4],
            "brandid": result[5],
            "brand": result[6],
            "name": result[7],
            "address": result[8],
            "latitude": result[9],
            "longitude": result[10]
        }
        for result in results
    ]
    global combined, combined_df
    # Convert to JSON list
    combined = json_results
    combined_df = pd.DataFrame(combined)
    cursor.close()
    conn.close()


def main():
    mqtt_connect()
    data_initialize()
    build_dashboard()
    while True:
        if len(latest_data) > 0:
            combined.append(latest_data)
            st.rerun()
        time.sleep(60)


if __name__ == "__main__":
    main()
