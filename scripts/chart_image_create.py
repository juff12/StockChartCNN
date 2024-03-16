import pandas as pd
from lightweight_charts import Chart
from pathlib import Path

# interval of viewing the chart

# test values (interval/increment/barspacing) day: 140/10/3, 4_hour: 400/15/1.75, 1_hour: 500/30/1.5
interval = {'1_day': 140, '4_hour': 120, '1_hour': 120,
            '15_minute': 120,  '5_minute': 120, '1_minute': 120}
# amount to increment based on candel size
increment = {'1_day': 10, '4_hour': 15, '1_hour': 20,
             '15_minute': 20, '5_minute': 20, '1_minute': 20}
# times the candle sticks can be
bartimes = ['1_day','4_hour','1_hour',
            '15_minute','5_minute','1_minute'] 
# bar spacing prests -> fits max zoom -> lower value = more zoom out, higher value = less zoom out
barspacing = {'1_day': 3, '4_hour': 1.75, '1_hour': 1.5,
              '15_minute': 1.5, '5_minute': 1.5, '1_minute': 1.5}


def save_screenshot(image: list,filename: str, ticker: str, chart_type: str, bartime: str, parentdir: str):
    filepath = Path('inputs/raw/{p}/images/{t}/{c}/{b}/{f}_screenshot.png'.format(p=parentdir,
                                                                                  t=ticker,c=chart_type,
                                                                                  b=bartime,f=filename))
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "wb") as out:
        out.write(image)

def screenshot_chart(ticker: str, bartime: str, chart_type: str, parentdir: str):    
    col_labels = ['date','open','high','low','close']
    # Columns: time | open | high | low | close
    try:
        df = pd.read_csv('data/{p}/formatted/{t}/{t}_{b}_data_formatted.csv'.format(p=parentdir,
                                                                                    t=ticker,b=bartime))
    except Exception:
        return # file does not exist, skip
    chart = Chart(1200, 600)
    chart.crosshair('hidden')
    
    chart.grid(False,False)
    # set the first set on the chart
    chart.time_scale(min_bar_spacing=barspacing[bartime])
    
    chart.set(df[col_labels].iloc[0:interval[bartime]])
    chart.fit()
    chart.show()
    image = chart.screenshot()
    
    _, unit = bartime.split('_')
    filename = '{t}_{b}_{c}_{u}_1_to_{u}_{e}'.format(t=ticker,b=bartime,c=chart_type,
                                                     u=unit,e=str(interval[bartime]))
    
    save_screenshot(image, filename, ticker, chart_type, bartime, parentdir)       
    # intialize variables to keep track of updates
    count, start_idx, idx = 1, increment[bartime], interval[bartime]
    
    while idx < df['date'].size:
        chart.update(df[col_labels].iloc[idx])
        # next increment starting point
        size, unit = bartime.split('_')
        if count == increment[bartime] or idx + 1 == df['date'].size:
            filename = '{t}_{b}_{c}_{u}_{s}_to_{u}_{e}'.format(t=ticker,b=bartime,c=chart_type,
                                                               u=unit,s=str(start_idx),e=str(idx))
            
            image = chart.screenshot()
            save_screenshot(image, filename, ticker, chart_type, bartime, parentdir)
            count = 1
            idx += 1
            start_idx += increment[bartime]
            continue
        idx += 1
        count += 1
        #time.sleep(0.005)
    chart.exit()