
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pytz
# from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
import pandas as pd
import requests
import json

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 10),
    'email': ['63606031@kmitl.ac.th'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}
with DAG('daily_oil_price', schedule_interval="@daily", default_args=default_args, tags=['Daily'], catchup=False) as dag:

    def transformData(allData):
        from dateutil.parser import parse
        oilType = {1:"Diesel B7 Premium",17:"Diesel B7",2:"Diesel",5:"Diesel B20",7:"Benzine",9:"Gasohol 95",10:"Gasohol 91",11:"Benzine E20",12:"Benzine E85",13:None}
        allOilPriceData = []
        for data in allData:
            oilPriceData = json.loads(data['priceData'])
            transformDateTime = lambda x: parse(x).strftime("%Y/%m/%d %H:%M:%S") # create lambda method for date transforming
            [k.update({'OilTypeId':oilType[k['OilTypeId']] # update data in dictionary
                    ,"Price":str(k['Price'])
                    ,"PriceDate":transformDateTime(k['PriceDate'])}) for k in oilPriceData]
            print(oilPriceData)
            allOilPriceData += oilPriceData
        return allOilPriceData

    def get_data(**kwargs):
        now = datetime.now(pytz.utc)
        allData = []
        print("Year:",now.year)
        print("- Month:",now.month)
        url = "https://orapiweb1.pttor.com/api/oilprice/search"
        payload = {"provinceId":1,"districtId":None,"year":now.year,"month":now.month,"pageSize":1000000,"pageIndex":0}
        payload = json.dumps(payload)
        header = {"user-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
                "content-type": "application/json;charset=UTF-8"}
        response = requests.post(url,data=payload,headers=header)
        allData += response.json()['data']
        return transformData(allData)

    def show_all_data(**kwargs):
        query = 'select * from oil_price'
        mysql_hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='testdb')
        df = mysql_hook.get_pandas_df(query)
        print(df)
            
    def check_table_exists(**kwargs):
        query = 'select count(*) from information_schema.tables where table_name="oil_price"'
        mysql_hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='testdb')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results

    def store_data(**kwargs):
        createdDate = datetime.now(pytz.utc).strftime("%Y/%m/%d %H:%M:%S")
        res = get_data()
        table_status = check_table_exists()
        mysql_hook = MySqlHook(mysql_conn_id='mysql_test_conn', schema='testdb')
        if table_status[0][0] == 0:
            print("----- table does not exists, creating it")    
            create_sql = 'create table oil_price(Created_Date DATETIME, Price_Date DATETIME, Oil_Type varchar(100), Oil_Price varchar(40))'
            mysql_hook.run(create_sql)
        else:
            print("----- table already exists")

        for data in res:
            sql = 'insert into oil_price (Created_Date, Price_Date ,Oil_Type, Oil_Price) values (%s ,%s, %s, %s)'
            val = (createdDate,data['PriceDate'],data['OilTypeId'],data['Price'])
            mysql_hook.run(sql, parameters=val)
            
    t_show_data = PythonOperator(
        task_id='show_data',
        dag=dag,
        python_callable=show_all_data,
    )
    t_etl = PythonOperator(
        task_id='etl_opt',
        dag=dag,
        python_callable=store_data,
    )
    t_prepare = BashOperator(
        task_id='prepare_opt',
        bash_command='echo "Phutares Task !! - Hello World"',
        dag=dag)


    t_prepare >> t_etl >> t_show_data