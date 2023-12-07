#!/usr/bin/env python3
# pip3 install -r requirement.txt
import pandas as pd
import seaborn as sns
import folium
import numpy as np
from folium.plugins import MarkerCluster

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
import matplotlib
import matplotlib.pyplot as plt


def load_data():
    """Load datasets."""
    prices = pd.read_csv('./Prices.csv')
    fuels = pd.read_csv('./Fuel.csv')
    observations = pd.read_csv('./Observations.csv')
    companies = pd.read_csv('./Companies.csv')
    stations = pd.read_csv('./Stations.csv')

    return prices, fuels, observations, companies, stations


def preprocess_data(prices, fuels, observations, companies, stations):
    """Preprocess the datasets."""
    companies.fillna({'HQCountry': 'Independent'}, inplace=True)
    prices.rename(columns={'Observation': 'ObservationNo'}, inplace=True)
    combined = pd.merge(observations, prices, on='ObservationNo', how='left')
    combined['PriceDate'] = pd.to_datetime(combined['PriceDate'])
    fuels.rename(columns={'Fuel': 'FuelCode'}, inplace=True)
    final = pd.merge(combined, fuels, on='FuelCode', how='left')
    final = pd.merge(final, stations, on='ServiceStationNo', how='left')
    final.rename(columns={'Brand': 'Company'}, inplace=True)
    final = pd.merge(final, companies, on='Company', how='left')
    final['PriceTime'] = pd.to_datetime(final['PriceTime'], format='%H:%M')
    final = final.drop(['Founded', 'Source'], axis=1)

    return final


def data_exploration(final, prices, fuels, observations, companies, stations):
    """Explore the data."""
    print(final.isnull().sum())
    print(final.describe())
    print(observations['PriceDate'].min())
    print(observations['PriceDate'].max())

    print(companies.head())
    print(fuels.head())
    print(observations.head())
    print(prices.head())
    print(stations.head())

    print(prices['Price'].describe())

    print('HQCountries:', companies['HQCountry'].unique())
    print('MainComponents:', fuels['MainComponent'].unique())


    hist, bin_edges = np.histogram(prices['Price'], bins=30)

    hist_table = pd.DataFrame({
        'Bin_start': bin_edges[:-1],
        'Bin_end': bin_edges[1:],
        'Count': hist
    })

    print(hist_table)

    sns.histplot(prices['Price'], bins=30, kde=True)
    plt.title('Distribution of Fuel Prices')
    plt.xlabel('Price')
    plt.ylabel('Frequency')
    plt.show()

    # Pie chart showing the distribution of main components among fuels
    colors = ['gold', 'lightskyblue', 'lightcoral']
    plt.figure(figsize=(10, 5))
    fuels['MainComponent'].value_counts().plot(kind='pie', autopct='%1.1f%%', startangle=90,
                                               explode=[0.1] * fuels['MainComponent'].nunique(), shadow=True,
                                               colors=colors)
    plt.title('Distribution of Main Components among Fuels')
    plt.show()
    print('MainComponent Counts：', fuels['MainComponent'].value_counts())
    # Main Components Distribution: Check the price distribution across main components of fuels, like Unleaded, Diesel, and Gas.
    avg_price_by_component = final.groupby('MainComponent')['Price'].mean().sort_values()
    print('Average Price of Main Component:', avg_price_by_component)


def data_visualization(final, prices):
    """Visualize the data."""
    # Main Components Distribution
    avg_price_by_component = final.groupby('MainComponent')['Price'].mean().sort_values()
    print('Average Price of Main Component:', avg_price_by_component)

    # Average Fuel Price by Fuel Type
    avg_prices = prices.groupby('FuelCode')['Price'].mean().sort_values()
    avg_prices.plot(kind='barh', color='skyblue')
    plt.title('Average Fuel Price by Fuel Type')
    plt.xlabel('Average Price')
    plt.ylabel('Fuel Type')
    plt.show()
    print(avg_prices)

    # Average Fuel Price by Company
    avg_price_by_company = final.groupby('Company')['Price'].mean().sort_values()
    avg_price_by_company.plot(kind='barh', color='skyblue')
    plt.title('Average Fuel Price by Company')
    plt.xlabel('Average Price')
    plt.show()
    print(avg_price_by_company)

    # Average Price Over Time
    avg_price_over_time = final.groupby(['PriceDate', 'FuelCode'])['Price'].mean().unstack()
    avg_price_over_time.plot()
    plt.title('Average Price of Different Fuels Over Date')
    plt.ylabel('Price')
    plt.xlabel('Date')
    plt.legend(title='Fuel Type')
    plt.show()

    # Average Price of Different Fuels Over Hour in 2023-02-23
    avg_price_by_hour = final[final['PriceDate'] == '2023-02-23'].groupby([final['PriceTime'].dt.hour, 'FuelCode'])[
        'Price'].mean().unstack()
    avg_price_by_hour.plot()
    plt.title('Average Price of Different Fuels Over Hour in 2023-02-23')
    plt.ylabel('Price')
    plt.xlabel('Hour')
    plt.show()

    # Average Fuel Price by Suburb
    avg_price_by_suburb = final.groupby('Suburb')['Price'].mean()
    avg_price_by_suburb.sort_values()[0:15].plot(kind='barh', color='skyblue')
    plt.title('Average Fuel Price by Suburb')
    plt.xlabel('Average Price')
    plt.show()

    # Correlation matrix
    correlation = final.copy()
    # Create dummy variables
    dummies = pd.get_dummies(correlation['MainComponent'], prefix='MainComponent')
    correlation = pd.concat([correlation, dummies], axis=1)
    correlation_numeric = correlation.select_dtypes(include=['float64', 'int64'])
    correlation_matrix = correlation_numeric.corr()
    print(correlation_matrix)
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
    plt.title('Correlation Heatmap')
    plt.show()

    # Geographical Price Distribution using Folium
    geographical = final.copy()
    avg_prices = geographical.groupby(['Suburb', 'MainComponent']).Price.mean().reset_index()
    avg_location = geographical.groupby('Suburb').agg({'latitude': 'mean', 'longitude': 'mean'}).reset_index()
    avg_prices = pd.merge(avg_prices, avg_location, on='Suburb', how='inner')

    m1 = folium.Map(location=[-33.869, 151.209], zoom_start=12)
    marker_cluster = MarkerCluster().add_to(m1)
    color_dict = {
        'Unleaded': 'blue',
        'Diesel': 'green',
        'Gas': 'orange'
    }

    for idx, row in avg_prices.iterrows():
        popup_text = f"Suburb: {row['Suburb']}<br>Main Component: {row['MainComponent']}<br>Avg Price: {row['Price']:.2f}"
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=popup_text,
            icon=folium.Icon(color=color_dict.get(row['MainComponent'], 'red'))
        ).add_to(marker_cluster)

    m1.save('map1.html')


def main():
    prices, fuels, observations, companies, stations = load_data()
    final = preprocess_data(prices, fuels, observations, companies, stations)
    data_exploration(final, prices, fuels, observations, companies, stations)
    data_visualization(final, prices)


if __name__ == '__main__':
    main()
