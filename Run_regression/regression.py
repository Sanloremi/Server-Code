import pandas as pd
import statsmodels.api as sm
from Process_dataframe_for_regression import process_dataframe_add_remove_columns


def regression_statsmodel() :
    chunk_full = pd.DataFrame()
    try :
        chunk_full = process_dataframe_add_remove_columns.remove_and_add_columns()
        chunk_full = chunk_full.drop(
            columns=['parkID', 'connectorID', 'municipality', ], axis=1)
        # chunk_full = chunk_full[~chunk_full['ConnectorID'].isin(['ConnectorID'])]
        chunk_full['timestamp'] = pd.to_datetime(chunk_full['timestamp'], format='%Y-%m-%d %H:%M:%S')
        # chunk_full = chunk_full[(chunk_full.status != 'UNKNOWN')]
        chunk_full = pd.get_dummies(chunk_full, drop_first=True)
        # chunk_full.to_csv('D:/Server Code/Output/chunk_full.csv', index=False)
        X = chunk_full[
            ['population_density', 'distance_nearest_highway', 'intensity_of_cars', 'Hour_H1', 'Hour_H2', 'Hour_H3',
             'Hour_H4', 'Hour_H5', 'Hour_H6', 'Hour_H7', 'Hour_H8', 'Hour_H9',
             'Hour_H10', 'Hour_H11', 'Hour_H12', 'Hour_H13', 'Hour_H14', 'Hour_H15',
             'Hour_H16', 'Hour_H17', 'Hour_H18', 'Hour_H19', 'Hour_H20', 'Hour_H21',
             'Hour_H22', 'Hour_H23', 'weather_current_Fog', 'weather_current_Rain', 'weather_current_Snow',
             'weather_current_Storm', 'weather_current_Sunny', 'Day_Monday', 'Day_Saturday', 'Day_Sunday',
             'Day_Thursday', 'Day_Tuesday', 'Day_Wednesday', 'station_accesstype_Public']]
        y = chunk_full['status_CHARGING']
        # Assign the output to four variables
        X2 = sm.add_constant(X)
        est = sm.OLS(y, X2)
        est2 = est.fit()
        print(est2.summary())
    except() :
        print('Something went wrong')
    finally :
        print('Regression complete')

