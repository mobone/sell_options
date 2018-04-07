import pandas_datareader.data as web
from datetime import datetime, timedelta
import pymongo
import pickle
import pandas as pd
import warnings
import queue
import threading
warnings.filterwarnings('ignore')
class backtest(threading.Thread):
    def __init__(self, q, out_q):
        threading.Thread.__init__(self)
        self.q = q
        self.out_q = out_q

    def run(self):
        while self.q.qsize():
            symbol = self.q.get()
            self.symbol = symbol
            self.option_symbol = None
            print(self.symbol)
            self.get_start_end_dates()
            try:
                self.get_historical_data()
            except:
                continue
            self.iterate_through_history()

    def get_start_end_dates(self):
        #options = options_coll.find({'Root': self.symbol, 'iteration': 7}).distinct('Update_Date')
        #dates = pd.DataFrame(list(options))
        #dates = dates.sort_values(by=0)
        #self.start_date = dates.head(1).values[0][0]
        #self.end_date = dates.tail(1).values[0][0]
        self.start_date = '20170301'
        self.end_date = '20171231'

    def get_historical_data(self):
        self.historical = web.DataReader(self.symbol, 'morningstar', self.start_date, self.end_date)
        self.historical = self.historical.reset_index()

    def iterate_through_history(self):
        for self.date_index in range(1,len(self.historical)):
            prev_close =  self.historical.iloc[self.date_index-1]['Close']
            todays_date = str(self.historical.iloc[self.date_index]['Date']).replace('-','').split(' ')[0]

            query = {'Root': self.symbol, 'iteration': 7, 'Update_Date': todays_date, 'Type': 'p'}
            #print(query)
            try:
                price = options_coll.find_one(query,{'Underlying_Price': 1})['Underlying_Price']
            except:
                continue

            alert_level = (price-prev_close)/prev_close
            #print(todays_date, alert_level)
            if abs(alert_level)>.03 and abs(alert_level)<.8:
                self.get_todays_option(query, price, todays_date, alert_level)

    def get_option_results(self):
        #print(self.selected_option)
        expiry_date = str(self.selected_option['Expiry']).replace('-','').split(' ')[0]
        start_date = str(self.historical.iloc[self.date_index]['Date']).replace('-','').split(' ')[0]
        query = {'Symbol': self.option_symbol,
                  'Update_Date': {
                        '$lt': expiry_date,
                        '$gt': start_date
                        }
                    }

        option_history = pd.DataFrame(list(options_coll.find(query)))
        try:
            option_history = option_history.sort_values(by=['Update_Date', 'iteration'])
        except:
            self.selected_option = None
            self.option_symbol = None
            return
        buy_price = self.selected_option['Ask']
        fifty_percent = option_history[option_history['Bid']<(buy_price/2.0)]
        if len(fifty_percent):
            self.selected_option['Fifty Percent'] = True
        else:
            self.selected_option['Fifty Percent'] = False
        final_option = option_history.ix[:,['Update_Date', 'Expiry', 'iteration', 'Bid', 'Ask']].tail(3).head(1).squeeze()
        #print(len(option_history))
        self.option_symbol = None
        sell_price = final_option['Bid']
        self.selected_option['Buy Price'] = buy_price
        self.selected_option['Sell Price'] = sell_price
        self.selected_option['Return Percent'] = (sell_price-buy_price)/buy_price
        self.selected_option['Profit'] = ((buy_price - sell_price)*100)-6
        self.out_q.put(self.selected_option)
        self.selected_option = None

    def get_todays_option(self, query, price, todays_date, alert_level):
        option_start_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=20)).strftime('%Y%m%d')
        option_end_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=60)).strftime('%Y%m%d')
        query['Strike'] = {'$lt': price}
        query['Expiry'] = {'$gt': option_start_date, '$lt': option_end_date}
        #print(query)
        options = pd.DataFrame(list(options_coll.find(query)))
        if len(options) == 0:
            return
        # TODO: incorporate into query
        options = options[(options['Bid']>=.8) & (options['Bid']<=1.3)]
        options = options[(options['Ask']>=.8) & (options['Ask']<=1.3)]
        if len(options) == 0:
            return
        options = options.sort_values(by='Strike')

        options['Expiry'] = pd.to_datetime(options['Expiry'])
        #option_start_date = datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=14)
        #option_end_date = datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=60)
        #print(option_start_date, option_end_date)
        options = options[options['Expiry']>option_start_date]
        options = options[options['Expiry']<option_end_date]
        option = options.head(1)
        option['Alert Level'] = alert_level

        self.option_symbol = option['Symbol'].values[0]
        self.selected_option = option.squeeze()
        #print(self.option_symbol)
        self.get_option_results()





mongo_string = 'mongodb://192.168.1.24:27017/'
client = pymongo.MongoClient(mongo_string)
db = client.finance
options_coll = db.options
q = queue.Queue()
out_q = queue.Queue()


symbols = list(options_coll.find({},{'Root': 1}).distinct('Root'))
for symbol in symbols:
    q.put(symbol)

for i in range(5):
    x = backtest(q, out_q)
    x.start()
results = []
while q.qsize():
    result = out_q.get()
    results.append(result.T)
    df = pd.DataFrame(results)
    print(df)

    metrtics = [df['Profit'].sum(),df[df['Alert Level']>0]['Profit'].sum(), df[df['Alert Level']<0]['Profit'].sum(), \
          df[df['Alert Level']>0]['Profit'].mean(),df[df['Alert Level']<0]['Profit'].mean(), \
          df[df['Alert Level']>0]['Profit'].min(),df[df['Alert Level']<0]['Profit'].min()]
    print(pd.DataFrame([metrtics], columns = ['Total Profit', 'Pos Alert Total', 'Neg Alert Total', 'Pos Alert Mean', 'Neg Alert Mean', 'Pos Alert Max Loss', 'Neg Alert Max Loss']))
    df.to_csv('backtes_result.csv')
