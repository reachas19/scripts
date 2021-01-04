import pandas as pd
import numpy as np
import pymysql
from google.cloud import bigquery
import sqlalchemy

con = pymysql.connect(host='10.0.80.16', port=3306, user='readdbuser',password='85B7DuE72pW2H123',db='bewakoof')
client = bigquery.Client.from_service_account_json('bewakoof_credentials.json')

def main():
	query = '''
	select p.id as product_id,m.name brand,c.subclass, c.name child_category_name,pc.name parent_category_name,c.gender
	from products as p
	left join categories as c on p.category_id = c.id
	left join categories as pc on c.parent_id = pc.id
	left join manufacturer_brands m on c.manufacturer_brand_id = m.id
	'''
	product_metadata = pd.read_sql(query, con)

	con.close()

	query = '''
	WITH
	  imprs AS (
	  SELECT
		DATE(rtc) AS date,
		product_id,
		CONCAT(anonymous_id,'_',EXTRACT(HOUR
		  FROM
			rtc)) AS sessionId
	  FROM
		`bewakoof-analytics.events.product_impression`
	  WHERE
		DATE(rtc) = "2020-12-21"),
	  clks AS (
	  SELECT
		DATE(rtc) AS date,
		product_id,
		CONCAT(anonymous_id,'_',EXTRACT(HOUR
		  FROM
			rtc)) AS sessionId
	  FROM
		`bewakoof-analytics.events.product_clicked`
	  WHERE
		DATE(rtc) = "2020-12-21"),
	  a2c AS (
	  SELECT
		DATE(rtc) AS date,
		product_id,
		CONCAT(anonymous_id,'_',EXTRACT(HOUR
		  FROM
			rtc)) AS sessionId
	  FROM
		`bewakoof-analytics.events.added_to_cart`
	  WHERE
		DATE(rtc) = "2020-12-21"),
		ords AS (
	  SELECT
		DATE(rtc) AS date,
		product_id,
		CONCAT(anonymous_id,'_',EXTRACT(HOUR
		  FROM
			rtc)) AS sessionId
	  FROM
		`bewakoof-analytics.events.order_completed`
	  WHERE
		DATE(rtc) = "2020-12-21")
	SELECT
	  imprs.date,
	  imprs.product_id,
	  COUNT(DISTINCT imprs.sessionId) AS impressions,
	  COUNT(DISTINCT clks.sessionId) AS clicks,
	  COUNT(DISTINCT a2c.sessionId) AS a2cs,
	  COUNT(DISTINCT ords.sessionId) AS orders
	FROM
	  imprs
	LEFT JOIN
	  clks
	ON
	  imprs.product_id=clks.product_id
	  AND imprs.date=clks.date
	  AND imprs.sessionId=clks.sessionId
	LEFT JOIN
	  a2c
	ON
	  clks.product_id=a2c.product_id
	  AND clks.date=a2c.date
	  AND clks.sessionId=a2c.sessionId
	LEFT JOIN
	  ords
	ON
	  a2c.product_id =ords.product_id
	  AND a2c.date=ords.date
	  AND a2c.sessionId=ords.sessionId
	GROUP BY
	  imprs.date,
	  imprs.product_id
	'''

	product_metrics = client.query(query)
	product_metrics = product_metrics.to_dataframe()
	product_metrics.fillna(0,inplace=True)
	product_metrics.product_id = product_metrics.product_id.astype(int)

	final_df = pd.merge(product_metrics,product_metadata,how='left',on='product_id')

	final_df = final_df[['date','product_id','subclass','brand','parent_category_name','child_category_name','gender','impressions','clicks','a2cs','orders']]

	database_username = 'analytics_ops'
	database_password = 'data123qwe123'
	database_ip       = '10.0.70.12'
	database_name     = 'demo_db'
	conn = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(database_username, database_password,database_ip, database_name))
	final_df.to_sql(con=conn, name='brand_category', chunksize=1000,if_exists='append',index=False)




if __name__ == '__main__':
	main()	
