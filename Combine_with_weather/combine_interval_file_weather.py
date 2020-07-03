import pandas as pd
import sqlite3
from Prepare_file_10min_interval import process_dataframe_insert_10min_interval


def add_weather_data() :
    # read csv file number of rows including the column name 2502762
    df = pd.read_csv(r'D:/Server Code/Required_csvs'
                     r'/final_1.csv',
                     sep=',', usecols=['connectorID', 'type', 'parkID', 'timestamp', 'status',
                                       'municipality_name', 'population_density_inhabitants_per_kmsq',
                                       'station_accesstype_pub_priv', 'LOCATION', 'join_NAME', 'join_type',
                                       'distance_nearest_highway', 'IntensityofCars'],
                     low_memory=False)
    # convert the timestamp to the datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    # drop duplicates
    df = df.drop_duplicates()
    # drop connectors without 'type', 'station_accesstype_pub_priv', 'join_type',
    # 'population_density_inhabitants_per_kmsq'
    df = df.dropna(
        subset=['type', 'station_accesstype_pub_priv', 'join_type', 'population_density_inhabitants_per_kmsq'])
    print(df)
    # ouput has 1801886 rows
    final_table = process_dataframe_insert_10min_interval.include_intervals_one(df)
    weather_data_path = "D:/Server Code/Required_csvs/weather_data_8_code_1.csv"
    weather_data = pd.read_csv(
        r"D:/Server Code/Required_csvs/Weather/weather_data_8_code_1.csv", encoding='utf-8')
    weather_data['DTG'] = pd.to_datetime(weather_data['DTG'], format="%Y-%m-%d %H:%M:%S")
    weather_data['DTG_New'] = pd.to_datetime(weather_data['DTG_New'], format="%Y-%m-%d %H:%M:%S")
    final_table['timestamp'] = pd.to_datetime(final_table['timestamp'], format="%Y-%m-%d %H:%M:%S")
    print(final_table)
    conn = sqlite3.connect(':memory:')
    f = open('D:/Server Code/Output/weather_data_added_1.csv',
             'w',
             encoding='utf-8', newline='')
    cur = conn.cursor()
    # conn.text_factory = str
    weather_data.to_sql('weather_data', conn, index=False)
    final_table.to_sql('final_table', conn, index=False)
    sqlcode = '''select c.* ,co.* from final_table c inner join weather_data co where (c.timestamp >= co.DTG and c.timestamp <= co.DTG_New) and (c.LOCATION = co.LOCATION) '''
    # newdf = pd.read_sql_query(sqlcode, conn)
    cur.execute(sqlcode)
    print("Executed query")
    # Get data in batches
    while True :
        # Read the data
        df = pd.DataFrame(cur.fetchmany(100000))
        # We are done if there are no data
        if len(df) == 0 :
            break
        # Let's write to the file
        else :
            df.rename(
                columns={0 : 'timestamp', 1 : 'connectorID', 2 : 'type', 3 : 'parkID', 4 : 'status', 5 : 'municipality',
                         6 : 'population_density', 7 : 'station_accesstype', 8 : 'join_LOCATION', 9 : 'join_NAME',
                         10 : 'road_type',
                         11 : 'distance_nearest_highway', 12 : 'intensity_of_cars', 13 : 'DTG', 14 : 'LOCATION',
                         15 : 'NAME', 16 : 'LATITUDE', 17 : 'LONGITUDE', 18 : 'weather_current', 19 : 'WW_PAST_10',
                         20 : 'DTG_New'}, inplace=True)
            df = df.drop(
                columns=['LOCATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'join_LOCATION', 'join_NAME', 'WW_PAST_10',
                         'DTG_New', 'DTG'], axis=1)
            df_1 =df
            # df.to_csv(f, index=False, encoding='utf-8')
    # Clean up
    print('Done')
    f.close()
    cur.close()
    conn.close()
    return df_1



