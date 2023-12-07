import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

prices = pd.read_csv('./Fuel-Data/Prices.csv')
fuels = pd.read_csv('./Fuel-Data/Fuel.csv')
observations = pd.read_csv('./Fuel-Data/Observations.csv')
companies = pd.read_csv('./Fuel-Data/Companies.csv')
stations = pd.read_csv('./Fuel-Data/Stations.csv')

# Data exploration
print(companies.head())
print(fuels.head())
print(observations.head())
print(prices.head())
print(stations.head())

# Missing values checking
print(companies.isnull().sum())
print(fuels.isnull().sum())
print(observations.isnull().sum())
print(prices.isnull().sum())
print(stations.isnull().sum())

companies.fillna({'HQCountry':'Independent'}, inplace=True)

print(prices.describe())

print(observations['PriceDate'].min())
print(observations['PriceDate'].max())

print('HQCountries:', companies['HQCountry'].unique())

print('MainComponents:', fuels['MainComponent'].unique())

sns.histplot(prices['Price'], bins=30, kde=True)
plt.title('Distribution of Fuel Prices')
plt.xlabel('Price')
plt.ylabel('Frequency')
plt.show()

prices.rename(columns={'Observation':'ObservationNo'},inplace=True)
prices.head()
combined = pd.merge(observations,prices, on='ObservationNo',how='left')
combined['PriceDate'] = pd.to_datetime(combined['PriceDate'])
fuels.rename(columns={'Fuel':'FuelCode'},inplace=True)
final = pd.merge(combined, fuels, on='FuelCode', how='left')
final = pd.merge(final, stations, on='ServiceStationNo', how='left')
final.rename(columns={'Brand':'Company'},inplace=True)
final = pd.merge(final, companies, on='Company', how='left')

final = final.drop(['Founded'], axis = 1)
final.isnull().sum()

final.head()

# Country of Origin Effect: Analyze if companies from specific HQ countries tend to have higher or lower prices.
avg_price_by_country = final.groupby('HQCountry')['Price'].mean().sort_values()
print(avg_price_by_country)
plt.bar(avg_price_by_country.index, avg_price_by_country)
plt.show()

avg_price_over_time = final.groupby(['PriceDate', 'FuelCode'])['Price'].mean().unstack()
print(avg_price_over_time.isnull().sum())
print(avg_price_over_time.head())

# Average Fuel Price by Fuel Type: Calculate the average price for each fuel type.
avg_prices = prices.groupby('FuelCode')['Price'].mean().sort_values()
avg_prices.plot(kind='barh', color='skyblue')
plt.title('Average Fuel Price by Fuel Type')
plt.xlabel('Average Price')
plt.ylabel('Fuel Type')
plt.show()
print(avg_prices)

# Average Fuel Price by Company: Calculate the average price for each fuel type across different companies.
avg_price_by_company = final.groupby('Company')['Price'].mean().sort_values()
print(avg_price_by_company)
avg_price_by_company.plot(kind='barh', color='skyblue')
plt.title('Average Fuel Price by Company')
plt.xlabel('Average Price')
plt.show()

# Price Trends Over Time: Analyze how prices of different fuel types change over time. This can help determine when
# prices tend to rise or fall.
avg_price_over_time.plot()
plt.title('Average Price of Fuels Over Time')
plt.ylabel('Price')
plt.xlabel('Date')
plt.legend(title='Fuel Type')
plt.show()

final['PriceTime'] = pd.to_datetime(final['PriceTime'], format='%H:%M')
avg_price_by_hour = final.groupby(final['PriceTime'].dt.hour)['Price'].mean()

avg_price_by_hour.plot()
plt.title('Average Price of Fuels Over Hour')
plt.ylabel('Price')
plt.xlabel('Hour')
plt.legend(title='Fuel Type')
plt.show()

# Geographical Price Distribution: Examine how fuel prices vary by Suburb or Postcode.
avg_price_by_suburb = final.groupby('Suburb')['Price'].mean()
print(avg_price_by_suburb)
avg_price_by_suburb.sort_values()[0:15].plot(kind='barh', color='skyblue')
plt.title('Average Fuel Price by Suburb')
plt.xlabel('Average Price')
plt.show()

# Companies with Highest and Lowest Prices: Identify which companies tend to have the highest or lowest prices.
highest_prices = final.groupby('Company')['Price'].max()
lowest_prices = final.groupby('Company')['Price'].min()
print(highest_prices, lowest_prices)