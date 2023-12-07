Notice: 
1. Although the error already be handled, when running code, 
better to insert station first, followed by insert price data.
Read the data_ingester.py file for more details.
2. Remember to modity the related database information.


Recommend Running order:
1. Data Ingester: create tables.
2. Data Cleaner: build a bridge between Data Ingester and Data Gather.
3. Data Gatherer: retrieve data.
4. Data Ingester: store station data.
5. Data Cleaner: query station data to maintain and check the validity of price data.
6. Data Gatherer: running again, transform price data to Data Ingester, the Data Ingester will start to store price data after validity checking. 
7. Data dashboard: visualize data.
8. Running Data Gatherer schedually to update data.