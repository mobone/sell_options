import pandas as pd
from requests_toolbelt.threaded import pool
import re
import requests as r
import time
from datetime import datetime, timedelta
import pandas_datareader.data as web
import configparser
from time import sleep
import pymongo

config = configparser.ConfigParser()
config.read('config.cfg')
username = config['creds']['User']
password = config['creds']['Pass']
ip = config['conn']['ip']

finviz_url = 'https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option,ta_change_u1&ft=3&r=%s'
todays_date = datetime.now()
client = pymongo.MongoClient(ip+':27017',
                             username = username,
                             password = password,
                             authSource='finance')

db = client.finance

class option_getter(object):
    def __init__(self, alert_df):
        self.alert_df = alert_df
        self.symbol = self.alert_df['Ticker']
        #if self.symbol != 'GOOS':
        #    return

        self.output_columns = ['SMA20', 'SMA50', 'SMA200', 'Perf Week',
                               'Perf Month', 'Perf Quarter', 'Volatility_Week',
                               'Recom', 'RSI (14)', 'Rel Volume',
                               '_id', 'Bid', 'Ask', 'Strike Distance', 'Strike Num',
                               'Market Cap', 'Price', 'Change']


        self.run()

    def run(self):
        self.options_coll = db.options_2
        self.finviz_coll = db.finviz

        #print(self.symbol)


        try:
            self.get_historical_data()
        except Exception as e:
            #print(e)
            return
        self.get_finviz()
        self.get_options()

    def get_historical_data(self):
        # used for getting trade dates

        self.end_date = datetime.today() - timedelta(days=1) # TODO: remove for production
        self.start_date = self.end_date - timedelta(days=5)

        self.yesterday = web.DataReader(self.symbol, 'morningstar', self.start_date, self.end_date)
        self.yesterday = self.yesterday.reset_index()

        self.yesterday = self.yesterday.tail(1)


    def get_finviz(self):
        previous_date = str(self.yesterday['Date'].values[0]).split('T')[0]
        previous_date = datetime.strptime(previous_date, '%Y-%m-%d').strftime('%Y%m%d')


        finviz_query = {'Root': self.symbol, 'Date': int(previous_date)}


        finviz_df = pd.DataFrame(self.finviz_coll.find_one(finviz_query), index=[0])


        for i in list(finviz_df.columns):
            if i not in self.output_columns:
                del finviz_df[i]
        if 'Market Cap' in finviz_df.columns:
            market_cap_value = finviz_df['Market Cap'].values[0]
            if 'K' in market_cap_value:
                market_cap_value = float(market_cap_value[:-1])*1000
            elif 'M' in market_cap_value:
                market_cap_value = float(market_cap_value[:-1])*1000000
            elif 'B' in market_cap_value:
                market_cap_value = float(market_cap_value[:-1])*1000000000

            finviz_df['Market Cap'] = market_cap_value

        if 'Recom' not in finviz_df.columns:
            finviz_df['Recom'] = None
        if 'Perf Quarter' not in finviz_df.columns:
            finviz_df['Perf Quarter'] = None
        if 'RSI (14)' not in finviz_df.columns:
            finviz_df['RSI (14)'] = None


        self.finviz_df = finviz_df.squeeze()
        #print(self.finviz_df)



    def get_options(self):
        todays_date = datetime.today().strftime('%Y%m%d')
        query = {'Root': self.symbol, 'iteration': 7, 'Update_Date': todays_date, 'Type': 'p'}


        option_start_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=30)).strftime('%Y%m%d')
        option_end_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=50)).strftime('%Y%m%d')
        query['Strike'] = {'$lt': self.yesterday['Close'].values[0]}
        query['Expiry'] = {'$gte': option_start_date, '$lte': option_end_date}

        #input()

        options = pd.DataFrame(list(self.options_coll.find(query)))

        #print(len(options))
        if len(options) == 0:
            return
        # TODO: incorporate into query
        options = options[(options['Bid']>=.8) & (options['Bid']<=2)]
        options = options[(options['Ask']>=.8) & (options['Ask']<=2)]
        options['Strike Distance'] = 1 - (options['Strike'] / options['Underlying_Price'])
        #options = options[options['Strike Distance']>self.strike_distance]
        if len(options) == 0:
            return
        options = options.sort_values(by='Strike')
        #print(options)
        #input()
        options['Expiry'] = pd.to_datetime(options['Expiry'])

        options = options.head(5)
        options = options.sort_values(by='Strike', ascending=False)
        options['Strike Num'] = options['Strike'].rank(method='dense')
        #options['Update_Date'] = pd.to_datetime(options['Update_Date'])
        for i in list(options.columns):
            if i not in self.output_columns:
                del options[i]


        #self.option_symbol = option['Symbol'].values[0]
        self.selected_options = options
        self.selected_options = self.selected_options.assign(**pd.Series(self.finviz_df))
        self.selected_options = self.selected_options[self.output_columns]





def get_alert_stocks():
    finviz_page = r.get(finviz_url % 1)
    symbol_count = int(re.findall('Total: </b>[0-9]*', finviz_page.text)[0].split('>')[1])
    urls = []

    for symbol_i in range(1, symbol_count, 20):
        urls.append(finviz_url % symbol_i)
        #break

    p = pool.Pool.from_urls(urls)
    p.join_all()

    total_df = []
    for response in p.responses():
        start = response.text.find('<table width="100%" cellpadding="3" cellspacing="1" border="0" bgcolor="#d3d3d3">')
        end = response.text.find('</table>',start)+10

        #symbols = re.findall(r'primary">[A-Z]*', response.text)
        df = pd.read_html(response.text[start:end])[0]
        df.columns = df.loc[0]
        df = df.drop([0])
        total_df.append(df)
    total_df = pd.concat(total_df)
    del total_df['No.']

    return total_df

total_option_df = []
alert_df = get_alert_stocks()
for row in alert_df.iterrows():
    try:
        company = option_getter(row[1])
    except:
        continue

    if hasattr(company, 'selected_options'):
        total_option_df.append(company.selected_options)
        print(pd.concat(total_option_df))
pd.concat(total_option_df).to_csv('test_data.csv')
