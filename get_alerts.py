import pandas as pd
import pymongo
from datetime import datetime, timedelta
import json
class option_getter(object):
    def __init__(self, alert):

        self.alert = alert
        self.symbol = self.alert['Ticker']

        self.get_option()
        if self.option is not None:
            self.get_metrics()
            self.store()

    def get_option(self):
        date = datetime.now().strftime('%Y%m%d')
        coll = collection.find({'Root': self.symbol, 'Type': 'p', 'Update_Date': date})
        options = pd.DataFrame(list(coll))
        options = options[(options['Bid']>=.8) & (options['Bid']<=1.3)]
        options = options[(options['Ask']>=.8) & (options['Ask']<=1.3)]
        options = options[options['Strike']<options['Underlying_Price']]

        options = options.sort_values(by=['Expiry', 'Strike'])
        options['Expiry'] = pd.to_datetime(options['Expiry'])

        start_date = datetime.now()+timedelta(days=7)
        end_date = start_date+timedelta(days=21)

        options = options[options['Expiry']>start_date]
        options = options[options['Expiry']<end_date]

        if options.empty:
            self.option = None
            return

        expiration = options['Expiry'].unique()[0]
        options = options[options['Expiry']==expiration]
        options['Expiry'] = options['Expiry'].dt.strftime('%Y%m%d')
        self.option = options.head(1)

    def get_metrics(self):
        self.option['Start Price'] = (self.option['Ask'] + self.option['Bid'])/2
        self.option['Diff from Current'] = (self.option['Strike']-self.option['Underlying_Price'])/self.option['Underlying_Price']
        self.option['Closed'] = False
        self.option['Current Price'] = None

        self.option['_id'] = str(self.alert['Ticker'])+'_'+str(self.option['Expiry'].values[0])
        #self.option = pd.concat([self.option, self.alert[1:].T])
        for i in self.alert.keys():
            self.option[i] = self.alert[i]
        #self.option = self.option.T
        del self.option['No.']
        self.json_doc = self.option.to_json(orient='records')


    def store(self):
        self.coll = db.put_sales
        print(self.json_doc)
        try:
            self.coll.insert(json.loads(self.json_doc))
        except:
            pass



mongo_string = 'mongodb://192.168.1.24:27017/'
client = pymongo.MongoClient(mongo_string)
db = client.finance
collection = db.options

for change in ['u5', 'd5']:
    df = pd.read_html('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o300,sh_opt_option,ta_change_'+ change, header=0)
    df = df[len(df)-2]
    for row in df.iterrows():
        option_getter(row[1])
