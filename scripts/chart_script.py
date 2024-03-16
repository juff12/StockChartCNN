import os
from pathlib import Path
from tqdm import tqdm
import pickle
from chart_image_create import screenshot_chart

def main():
    # read in the arrays for tickers
    crypto_tickers = pickle.load(open('data/crypto/iterables/coinbase_tickers.pkl', 'rb'))
    time_intervals = pickle.load(open('data/crypto/iterables/time_intervals.pkl', 'rb'))
    crypto_tickers = ['btcusd', 'ethusd', 'solusd']
    time_intervals = ['5_minute','15_minute']
    
    
    for ticker in tqdm(crypto_tickers, desc='Crypto: '):
        for bartime in time_intervals:
            ticker = ticker.lower()
            screenshot_chart(ticker,bartime,'candle','crypto')

# run script
if __name__ == '__main__':
    main()