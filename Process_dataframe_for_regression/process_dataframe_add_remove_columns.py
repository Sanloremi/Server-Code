import pandas as pd

pd.options.mode.chained_assignment = None
from Combine_with_weather import combine_interval_file_weather


def remove_and_add_columns() :
    df = combine_interval_file_weather.add_weather_data()
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['Day'] = df['timestamp'].dt.day_name()
    df['Month'] = df['timestamp'].dt.month_name()
    df['Hour'] = df['timestamp'].dt.hour
    df['Hour'] = df['Hour'].replace([0], 'H0')
    df['Hour'] = df['Hour'].replace([1], 'H1')
    df['Hour'] = df['Hour'].replace([2], 'H2')
    df['Hour'] = df['Hour'].replace([3], 'H3')
    df['Hour'] = df['Hour'].replace([4], 'H4')
    df['Hour'] = df['Hour'].replace([5], 'H5')
    df['Hour'] = df['Hour'].replace([6], 'H6')
    df['Hour'] = df['Hour'].replace([7], 'H7')
    df['Hour'] = df['Hour'].replace([8], 'H8')
    df['Hour'] = df['Hour'].replace([9], 'H9')
    df['Hour'] = df['Hour'].replace([10], 'H10')
    df['Hour'] = df['Hour'].replace([11], 'H11')
    df['Hour'] = df['Hour'].replace([12], 'H12')
    df['Hour'] = df['Hour'].replace([13], 'H13')
    df['Hour'] = df['Hour'].replace([14], 'H14')
    df['Hour'] = df['Hour'].replace([15], 'H15')
    df['Hour'] = df['Hour'].replace([16], 'H16')
    df['Hour'] = df['Hour'].replace([17], 'H17')
    df['Hour'] = df['Hour'].replace([18], 'H18')
    df['Hour'] = df['Hour'].replace([19], 'H19')
    df['Hour'] = df['Hour'].replace([20], 'H20')
    df['Hour'] = df['Hour'].replace([21], 'H21')
    df['Hour'] = df['Hour'].replace([22], 'H22')
    df['Hour'] = df['Hour'].replace([23], 'H23')
    df['weather_current'] = df['weather_current'].replace([0], 'Cloud')
    df['weather_current'] = df['weather_current'].replace([1], 'Fog')
    df['weather_current'] = df['weather_current'].replace([2], 'Rain')
    df['weather_current'] = df['weather_current'].replace([3], 'Snow')
    df['weather_current'] = df['weather_current'].replace([4], 'Storm')
    df['weather_current'] = df['weather_current'].replace([5], 'Sunny')
    df['weather_current'] = df['weather_current'].replace([6], 'Thunderstorm')
    df['weather_current'] = df['weather_current'].replace([7], 'Windy')
    df.to_csv('D:/Server Code/Output/file_for_regression.csv', index=False)
    return df
