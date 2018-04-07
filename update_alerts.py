import pandas as pd
import pymongo
from datetime import datetime, timedelta
import json
from time import sleep
class alert_update(object):
    def __init__(self, alert):
        self.alert = alert

        self.get_options()
        self.check_if_50_percent_profit()
        self.get_current_price()
        self.check_close_trade()
        self.store()

    def get_options(self):
        options = options_coll.find({'Symbol': self.alert['Symbol']}) #TODO
        options = pd.DataFrame(list(options))

        self.options = options.sort_values(by=['Update_Date', 'iteration'])

    def check_if_50_percent_profit(self):
        fifty_percent = self.alert['Start Price'] * .5
        self.options['Trade Price'] = (self.options['Bid'] + self.options['Ask'])/2
        below_fifty_df = self.options[self.options['Trade Price']<fifty_percent]
        if not below_fifty_df.empty:
            self.alert['Fifty Percent'] = True
        else:
            self.alert['Fifty Percent'] = False

    def get_current_price(self):
        self.options['Current Price'] = (self.options['Bid'] + self.options['Ask'])/2
        most_recent_trade = self.options.tail(1)
        self.alert['Current Price'] = most_recent_trade['Current Price'].values[0]

        # update some formating things, first time seen
        #if 'Last_Trade_Date' in self.alert.keys():
            #self.alert['Start Trade Date'] = self.alert['Last_Trade_Date']
            #self.alert['Start Iteration'] = self.alert['iteration']
        if 'Last_Trade_Date' in self.alert.keys():
            del self.alert['Last_Trade_Date']
        if 'Quote_Time' in self.alert.keys():
            del self.alert['Quote_Time']
        if 'Iteration' in self.alert.keys():
            del self.alert['iteration']

        self.alert['Last Trade Date'] = most_recent_trade['Last_Trade_Date'].values[0]

        self.alert['Current Iteration'] = most_recent_trade['iteration'].values[0]

        self.alert['Current Bid'] = most_recent_trade['Bid'].values[0]
        self.alert['Current Ask'] = most_recent_trade['Ask'].values[0]
        self.alert['Update_Date'] = datetime.now().strftime('%Y%m%d')
        self.alert['Return'] = round(((self.alert['Current Price']-self.alert['Start Price'])/self.alert['Start Price'])*-1,3)
        self.alert['Profit'] = round((self.alert['Start Price'] - self.alert['Current Price'])*100, 3)
        self.alert['Worst Profit'] =  round((self.alert['Bid'] - self.alert['Current Ask'])*100, 3)-5
        print(self.alert['Root'], self.alert['Fifty Percent'], self.alert['Symbol'], self.alert['Bid'], self.alert['Ask'], self.alert['Current Bid'], self.alert['Current Ask'], self.alert['Return'], self.alert['Profit'], self.alert['Worst Profit'])
        #print(self.alert)
        #input()

    def check_close_trade(self):
        if self.alert['Current Iteration']>20 and datetime.now().strftime('%Y%m%d')==self.alert['Expiry']:
            self.alert['Closed'] = True


    def store(self):
        try:
            alerts_coll.replace_one({'_id': self.alert['_id']}, json.loads(self.alert.to_json()))
            self.profit = self.alert['Profit']
        except Exception as e:
            print('storing failed', e)


def get_start_times():
    dt = datetime.now().strftime('%m-%d-%y')
    end_dt = datetime.strptime(dt+' 15:00:00', '%m-%d-%y %H:%M:%S')
    dt = datetime.strptime(dt+' 8:50:00', '%m-%d-%y %H:%M:%S')
    start_times = []
    while dt < end_dt:
        start_times.append(dt)
        dt = dt + timedelta(minutes=15)

    # skip to current time window
    for start_index in range(len(start_times)):
        if datetime.now()<start_times[start_index]:
            break
    return start_times, start_index

mongo_string = 'mongodb://192.168.1.24:27017/'
client = pymongo.MongoClient(mongo_string)
db = client.finance
options_coll = db.options
alerts_coll = db.put_sales


start_times, start_index = get_start_times()
for start_index in range(start_index, len(start_times)):
    print("Updater sleeping", datetime.now(), start_times[start_index], start_index)
    while datetime.now()<start_times[start_index]:
        sleep(10)
    alerts = alerts_coll.find({'Closed': False})
    profits = []
    worst_profits = []
    alerts_df = pd.DataFrame(list(alerts))

    for alert in alerts_df.iterrows():
        x = alert_update(alert[1])
        profits.append(x.profit)
        worst_profits.append(x.alert['Worst Profit'])
    print('>>',sum(profits), sum(worst_profits))
