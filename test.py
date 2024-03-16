import pandas as pd
import numpy as np

df = pd.read_csv('data/crypto/formatted/btcusd/btcusd_15_minute_data_formatted.csv')

print(df.index.duplicated())


for i, row in enumerate(df.index.duplicated()):
    if row == False:
        print(df.iloc[i])