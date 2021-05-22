import clickhouse_driver
import datetime
import pandas as pd
class QASingleFactorBase():
    def __init__(self, factor_name ="QAF_test_2"):
        self.client = clickhouse_driver.Client(host='localhost', database= 'factor')
        #self.tablelist = self.client.execute('show tables')
        
        
        
        self.client.execute("CREATE TABLE IF NOT EXISTS \
                `factor`.`regfactor` (　\
                factorname String, \
                create_date Date, \
                update_date Date, \
                version Float32, \
                description String DEFAULT 'None')\
            ENGINE=ReplacingMergeTree()\
            ORDER BY (factorname)\
            SETTINGS index_granularity=8192 ")
        self.factor_name =  factor_name
        
        self.description = 'None'
        
    

    
    
    
        if not self.check_if_exist():
            print('start register')
            self.register()
            self.init_database()
            
    def __str__(self):
        return "QAFACTOR {}".format(self.factor_name)
            
    def __repr__(self):
        return self.__str__()
    
    @property
    def tablelist(self):
        return self.client.query_dataframe('show tables').name.tolist()
        
    def check_if_exist(self):
        print(self.tablelist)
        return self.factor_name in self.tablelist
    
    
    def init_database(self):
        self.client.execute('CREATE TABLE IF NOT EXISTS \
                            `factor`.`{}` (\
                            date Date,\
                            code String,\
                            factor Float32\
                            )\
                            ENGINE = ReplacingMergeTree() \
                            PARTITION BY (toYYYYMMDD(`date`)) \
                            ORDER BY (date, code)\
                            SETTINGS index_granularity=8192'.format(self.factor_name))
    


    
    def register(self):
        self.client.execute("INSERT INTO regfactor VALUES",[{
            'factorname': self.factor_name,
            'create_date': datetime.date.today(),
            'update_date': datetime.date.today(),
            'version': 1.0,
            'description': self.description
        }])
        self.client.execute('OPTIMIZE TABLE regfactor FINAL')
        
        
    def insert_data(self, data:pd.DataFrame):
        
        ##check the data 
        data = data.assign(date= pd.to_datetime(data.date))
        columns =  data.columns 
        if 'date' not in columns or 'factor' not in columns:
            raise Exception('columns not exists')
        
        
        data =  data.to_dict('records')
        #print(data)
        
        self.client.execute("INSERT INTO  {}  VALUES".format(self.factor_name), data)
        self.client.execute('OPTIMIZE TABLE {} FINAL'.format(self.factor_name))
        
    def fetch_data(self, start=None, end=None):
        if start is None and end is None:
            res =  self.client.query_dataframe('SELECT * FROM {}'.format(self.factor_name))
            if res is not None:
                res.columns = ['date', 'code', self.factor_name]
                return res.set_index(['date', 'code']).sort_index()
    
    
    def calc(self):
        raise NotImplementedError
        
        
    def update_to_database(self):
        self.insert_data(self.calc())
        raw_reg_message = self.client.execute("select create_date from regfactor where factorname=='{}'".format(self.factor_name))[0][0]
        self.client.execute("INSERT INTO regfactor VALUES",[{
            'factorname': self.factor_name,
            'create_date': raw_reg_message,
            'update_date': datetime.date.today(),
            'version': 1.0,
            'description': self.description
        }])
