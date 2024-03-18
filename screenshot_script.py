from tqdm import tqdm
import pickle
from stock_chart_cnn import ChartImage
from PIL import Image
import numpy as np
import io

def testing():
    """
    Function to perform testing.

    This function sets up the necessary parameters for testing and generates chart images.
    """
    # presets for testing
    ticker = 'btcusd'
    bartime = '1_hour'
    chart_type = 'candle'
    parentdir = 'crypto'
    barspacing = 1.5
    # 60 and 10 for 1/4 hour, 
    num_bars_show = 60
    num_bar_gen = 10
    chart_width = 250
    chart_height = 250
    filename = f'data/{parentdir}/formatted/{ticker}/{ticker}_{bartime}_data_formatted.csv'
    
    chart_image_gen = ChartImage(filename=filename, ticker=ticker, bartime=bartime, 
                                 chart_type=chart_type, parentdir=parentdir,
                                 barspacing=barspacing, num_bars_show=num_bars_show,
                                 num_bar_gen=num_bar_gen, chart_width=chart_width, 
                                 chart_height=chart_height)

    image1, image2 = chart_image_gen.sample_screenshots()
    # display the images
    image1 = Image.open(io.BytesIO(image1))
    image1.show()
    image2 = Image.open(io.BytesIO(image2)) 
    image2.show()

def running():
    """
    Function to generate chart images for crypto tickers and time intervals.
    
    This function reads in the arrays for tickers and time intervals, and then iterates over each ticker and time interval
    combination to generate chart images using the specified parameters. The generated images are saved in the specified
    directory.
    """
    # presets for running
    chart_type = 'candle'
    parentdir = 'crypto'
    barspacing = 1.5
    num_bars_show = 60
    num_bar_gen = 5
    chart_width = 250
    chart_height = 250
    
    # read in the arrays for tickers
    crypto_tickers = pickle.load(open('data/crypto/iterables/coinbase_tickers.pkl', 'rb'))
    time_intervals = pickle.load(open('data/crypto/iterables/time_intervals.pkl', 'rb'))
    
    # custom tickers and range
    crypto_tickers = ['btcusd', 'ethusd']
    time_intervals = ['1_hour', '4_hour']
    
    for ticker in tqdm(crypto_tickers, desc='Crypto: '):
        for bartime in tqdm(time_intervals, desc='Bartime: '):
            filename = 'data/crypto/formatted/{t}/{t}_{b}_data_formatted.csv'.format(t=ticker, b=bartime)
            
            chart_image_gen = ChartImage(filename=filename, ticker=ticker, bartime=bartime, 
                                         chart_type=chart_type, parentdir=parentdir,
                                         barspacing=barspacing, num_bars_show=num_bars_show,
                                         num_bar_gen=num_bar_gen, chart_width=chart_width, 
                                         chart_height=chart_height)
            # save the generated images
            chart_image_gen.batch_screenshot(batch_size=10_000)

def main():
    #testing()
    running()
    
# run script
if __name__ == '__main__':
    main()