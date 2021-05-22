import clickhouse_driver
import datetime
import pandas as pd
import QUANTAXIS as QA


class QASingleFactor_DailyBase():
    def __init__(self, factor_name="QAF_test"):
        self.client = clickhouse_driver.Client(
            host='localhost', database='factor')
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
        self.factor_name = factor_name

        self.description = 'None'

        if not self.check_if_exist():
            print('start register')
            self.register()
            self.init_database()

        self.finit()

    def finit(self):

        pass

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
        self.client.execute("INSERT INTO regfactor VALUES", [{
            'factorname': self.factor_name,
            'create_date': datetime.date.today(),
            'update_date': datetime.date.today(),
            'version': 1.0,
            'description': self.description
        }])
        self.client.execute('OPTIMIZE TABLE regfactor FINAL')

    def insert_data(self, data: pd.DataFrame):

        # check the data
        data = data.assign(date=pd.to_datetime(data.date),
                           factor=data.factor.apply(float))
        columns = data.columns
        if 'date' not in columns or 'factor' not in columns:
            raise Exception('columns not exists')

        data = data.to_dict('records')
        rangex = 100
        for i in range(0, len(data), rangex):
            self.client.execute('INSERT INTO {} VALUES'.format(
                self.factor_name), data[i:i+rangex])

        self.client.execute('OPTIMIZE TABLE {} FINAL'.format(self.factor_name))

    def update_to_database(self):
        self.insert_data(self.calc())
        raw_reg_message = self.client.execute(
            "select create_date from regfactor where factorname=='{}'".format(self.factor_name))[0][0]
        self.client.execute("INSERT INTO regfactor VALUES", [{
            'factorname': self.factor_name,
            'create_date': raw_reg_message,
            'update_date': datetime.date.today(),
            'version': 1.0,
            'description': self.description
        }])

    def calc(self) -> pd.DataFrame:
        """

        the resulf of this function should be a dataframe with the folling columns


        ['date', 'code', 'factor']

        """
        raise NotImplementedError

    def fetch_data(self, start=None, end=None) -> pd.DataFrame:
        if start is None and end is None:
            res = self.client.query_dataframe(
                'SELECT * FROM {}'.format(self.factor_name))
            if res is not None:
                res.columns = ['date', 'code', self.factor_name]
                return res.set_index(['date', 'code']).sort_index()
            else:
                return pd.DataFrame([], columns=['date', 'code', self.factor_name])


class MA(QASingleFactor_DailyBase):
    def finit(self):
        pass

    def calc(self) -> pd.DataFrame:
        """

        the example is just a day datasource, u can use the min data to generate a day-frequence factor

        the factor should be in day frequence 
        """

        codellist = ['000001.XSHE', '000002.XSHE']
        start = '2020-01-01'
        end = '2021-05-22'
        data = QA.QA_fetch_stock_day_adv(codellist, start, end).to_qfq()
        res = data.add_func(QA.QA_indicator_MA, 5)
        res.columns = ['factor']
        return res.reset_index()


if __name__ == '__main__':
    exampleFactor = MA(factor_name='MA5')
    exampleFactor.update_to_database()

    exampleFactor.fetch_data()
