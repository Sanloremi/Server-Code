import pandas as pd


def include_intervals_one(dataframe) :
    dataframe_grouped = dataframe.groupby('connectorID')
    df_1 = pd.DataFrame()
    for group_name, df_group in dataframe_grouped :
        # print(df_group)
        # df_group['timestamp'] = pd.to_datetime(df_group['timestamp'])
        cols = ['timestamp', 'connectorID']
        df = pd.concat([
            d.asfreq('10min', method='ffill').ffill(downcast='infer')
            for _, d in df_group.drop_duplicates(cols, keep='last')
                .set_index('timestamp').groupby('connectorID')
        ]).reset_index()
        df_1 = df_1.append(df)
    print('Csv extracted')
    return df_1

