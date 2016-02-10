import pymysql
import os
import datetime
import csv
import sys
import codecs
import zipfile
import pandas as pd
import numpy as np

#Next 2 items provide the way to load a file to a windows share from Linux
import urllib2
from smb.SMBHandler import SMBHandler

#set the current date for navigating directory structure
from datetime import date
import datetime

curTime  = datetime.datetime.time(datetime.datetime.now())
today = date.today()
y = str(today.year)
m = str(today.month)
if len(m) ==1:
    m = str('0' + m)

d = str(today.day)
if len(d) ==1:
    d = str('0' + d)

dirDTStem = (y + '/' + m + '/' + d + '/'),
fileDTStem = (y + '-' + m + '-' + d)

##################################################################
#Set Variables

#query dates
# startDate = '2015-10-01'
# endDate  =  '2015-10-31'

#db  Variables
dbVer =  '2170'
dbPreUrl  ='sandbox-insights-'
dbPostUrl  = '.c4wznujzgwrt.us-west-2.rds.amazonaws.com'
dbURL = dbPreUrl + dbVer + dbPostUrl

#fileOutput
#outLoc = 'S:\\eCommerce\\BAU\\P&G\\BOX\\dumps\\'
#outLoc = 'S:\\eCommerce\\BAU\\P&G\\BOX\dumps\\'
# outLoc = 'c:\\temp\\'
# os.chdir(outLoc)



conn = pymysql.connect(host=dbURL, port=3306, user='data_analyst', passwd='dataanalyst', db='squirrel')

cur = conn.cursor()


conn2 = pymysql.connect(host=dbURL, port=3306, user='data_analyst', passwd='dataanalyst', db='metadata')

cur2 = conn2.cursor()

df1 = pd.read_sql("""select distinct(customer_id) as customers_id from customers_reports where frequency = 'daily' UNION select distinct(customer_id) from customers_reports where frequency = 'weekly' and day = dayname(curdate()) GROUP BY customer_id""", conn2)
print df1





df3 = pd.read_sql("""SELECT RIGHT(customers.name,2) as region, products.customer_id as customers_id, online_stores.name as online_store, customers.name as brand_owner,  count(products.rpc) as 'Count of RPC', if (active = 1,'CPC','NOT IN CPC') as report_date
FROM products
JOIN online_stores
ON products.online_store_id = online_stores.id
JOIN customers
ON products.customer_id = customers.id
GROUP BY customer_id, online_store_id
ORDER BY online_store_id, customer_id""", conn2)



# print "running append"

# df3a = df2.append(df3)

# print df3a



#myd = df3a.pivot_table(rows=['region', 'online_store', 'customers_id'], cols=['report_date'], values='Count of RPC', fill_value=0)


df3["country"] = df3["brand_owner"].str[-2:]
df3 = df3.sort("country", ascending = True)
df4 = df3["country"]
region_id = df4.drop_duplicates().dropna()
remove_junk = ["al","rd","pe"]
region_id = [x for x in region_id if x not in remove_junk]




#rows should equal output of country from metadata table
#region_id = ["kr"]
print region_id
region_id = ["UK"]
#outfile = '/home/niall/projects/DigitalQA/"%s".html' % filename

#output= '<html><head><script type="text/JavaScript">function timeRefresh(timeoutPeriod){ setTimeout("location.reload(true);",timeoutPeriod); }</script></head><title>Regional Data Quality Testing</title><body onload="JavaScript:timeRefresh(1200000);"><h2>DAILY QA</h2>'
    

for j in region_id:
    fName = str(fileDTStem + '_ ' + str(j) + '_daily_qa_file.html')
    fPath = fName

    #UNKNOWNS  
    startTime = datetime.datetime.now()
    print("EXTRACTING PRODUCT COUNT DATA IN: %s") % str(j)

    query = "select region, customers_id, online_store, brand_owner, count(trusted_rpc) as 'Count of RPC', if (report_date = curdate(),'TDAY','YDAY') as report_date from rpt_product_hist where report_date  = curdate() and region = '" + str(j) + "'  GROUP BY brand_owner, online_store UNION select region, customers_id, online_store, brand_owner,  count(trusted_rpc) as 'Count of RPC', if (report_date = curdate(),'TDAY','YDAY') as report_date from rpt_product_hist where report_date  = curdate()-1 and region = '" + str(j) + "' GROUP BY brand_owner, online_store ORDER BY online_store, brand_owner"
    df = pd.read_sql(query , conn)

    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store','customers_id'], cols=['report_date'], values='Count of RPC', fill_value=0)
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd = myd[myd.NOM_DIFF != 0]

    
    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)
    output = '<html><head><script type="text/JavaScript">function timeRefresh(timeoutPeriod){ setTimeout("location.reload(true);",timeoutPeriod); }</script></head><title>Regional Data Quality Testing</title><body onload="JavaScript:timeRefresh(1200000);"><h2>DAILY QA INSIGHTS IN %s"</h2>' %str(j)
    output +='<h2>Change in RPCs:</h2>'

    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output += myd.to_html()  

    startTime = datetime.datetime.now()
    print("EXTRACTING UKNOWNS DATA IN: %s") % str(j)

    query = "select region, online_store, customers_id, brand_owner, count(trusted_rpc) as 'RPC' from rpt_product_hist where report_date  = curdate() and region = '" + str(j) + "' and availability = 'unknown' GROUP BY region, online_store, customers_id, brand_owner ORDER BY region, online_store, customers_id, brand_owner"

    df = pd.read_sql(query , conn)

    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)
    output +='<h2>Unknowns Found:</h2>'

    if df.empty:
        output += "NO CHANGE TODAY"
    else:
        output += df.to_html()


    startTime = datetime.datetime.now()
    print('EXTRACTING VOIDS DATA PER STORE IN: %s') % str(j)
    #query = "select if (report_date = curdate(),'TDAY',report_date) as report_date, region, online_store, count(trusted_rpc) as 'RPC' from rpt_product_hist where report_date >= DATE_SUB(curdate(),INTERVAL 7 DAY) AND report_date <= curdate() and region = '" + str(j) + "' GROUP BY report_date, region, online_store ORDER BY report_date, region, online_store"""
    query = "select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, brand_owner,  count(trusted_rpc) as 'RPC'  from  rpt_product_hist where report_date = (DATE_SUB(curdate(),INTERVAL 1 DAY)) and availability = 'void' and region = '" + str(j) + "'  GROUP BY region, online_store, report_date  UNION  select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, brand_owner,  count(trusted_rpc) as 'RPC'  from  rpt_product_hist where report_date = curdate() and availability = 'void' and region = '" + str(j) + "' GROUP BY region, online_store, report_date"

    # query = "select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, brand_owner,  count(trusted_rpc) as 'RPC' from  rpt_product_hist where report_date IN (DATE_SUB(curdate(),INTERVAL 1 DAY),curdate())and availability = 'void' and region = '" + str(j) + "' GROUP  BY region, online_store, report_date"
    df = pd.read_sql(query , conn)
    

    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)
    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store'], cols=['report_date'], values='RPC', fill_value=0)
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']

        myd  = myd[myd.DEC_DIFF > 0.05]
        myd  = myd[myd.NOM_DIFF > 20]

        myd = myd.drop("NOM_DIFF", 1)
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)

    #output = '<html><head><script type="text/JavaScript">function timeRefresh(timeoutPeriod){ setTimeout("location.reload(true);",timeoutPeriod); }</script></head><title>Regional Data Quality Testing</title><body onload="JavaScript:timeRefresh(1200000);"><h2>Stores Not Loaded Today:</h2>'
    output += '<h2>Void Exceptions per store</h2>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output += myd.to_html()


#*****************************************
    print("EXTRACTING VOIDS DATA PER CUSTOMER: %s") % str(j)
    startTime = datetime.datetime.now()

    query = "select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, brand_owner, customers_id,  count(trusted_rpc) as 'RPC' from  rpt_product_hist  where report_date = (DATE_SUB(curdate(),INTERVAL 1 DAY))  and availability = 'void'  and region = '" + str(j) + "'  GROUP BY region, online_store, brand_owner, report_date UNION select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, brand_owner, customers_id ,  count(trusted_rpc) as 'RPC'  from  rpt_product_hist  where report_date = curdate() and availability = 'void' and region = '" + str(j) + "' GROUP BY region, online_store, brand_owner, report_date"


    df = pd.read_sql(query , conn)

    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)

    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store'], cols=['report_date'], values='RPC', fill_value=0)
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']

        myd  = myd[myd.DEC_DIFF > 0.05]
        myd  = myd[myd.NOM_DIFF > 20]

        myd = myd.drop("NOM_DIFF", 1)
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)

    #output = '<html><head><script type="text/JavaScript">function timeRefresh(timeoutPeriod){ setTimeout("location.reload(true);",timeoutPeriod); }</script></head><title>Regional Data Quality Testing</title><body onload="JavaScript:timeRefresh(1200000);"><h2>Stores Not Loaded Today:</h2>'
    output += '<h2>Void Exceptions per Customer</h2>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output += myd.to_html()

#*************************************************
    startTime = datetime.datetime.now()
    print("CHECKING AVAILABILITY AT STORE LEVEL EXTRACT IN: %s") % str(j)

    query = "select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, availability, count(trusted_rpc) as 'RPC' from rpt_product_hist where report_date = (DATE_SUB(curdate(),INTERVAL 1 DAY)) and region = '" + str(j) + "' GROUP BY region, online_store, availability, report_date UNION select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, availability, count(trusted_rpc) as 'RPC' from rpt_product_hist where report_date = curdate() and region = '" + str(j) + "' GROUP BY region, online_store, availability, report_date ORDER BY region, online_store, availability,  report_date"

    df = pd.read_sql(query , conn)
    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)

    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store','availability'], cols=['report_date'], values='RPC', fill_value=0)
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']
        myd  = myd[myd.ABS_DIFF > 10]
        myd["Change"] = myd['TDAY'] / myd['YDAY']
        myd = myd.loc[(myd.Change <= 0.8) | (myd.Change >= 1.2)]
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)
        myd = myd.drop("Change", 1)

    output += '<h2>Store Level Availability Exceptions</h2>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output += myd.to_html()

    startTime = datetime.datetime.now()

    print("CHECKING AVAILABILITY AT CUSTOMER LEVEL EXTRACT IN: %s") % str(j)

    query = "select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, customers_id, brand_owner, online_store, availability, count(trusted_rpc) as 'RPC' from rpt_product_hist where report_date = (DATE_SUB(curdate(),INTERVAL 1 DAY)) and region = '" + str(j) + "' GROUP BY region, online_store, availability, report_date UNION select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, customers_id, brand_owner, online_store,  availability, count(trusted_rpc) as 'RPC' from rpt_product_hist where report_date = curdate() and region = '" + str(j) + "' GROUP BY region, online_store, availability, report_date ORDER BY region, online_store, availability,  report_date"
    df = pd.read_sql(query , conn)
    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)

    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store', 'customers_id', 'brand_owner','availability'], cols=['report_date'], values='RPC', fill_value=0)
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']
        myd  = myd[myd.ABS_DIFF > 10]
        myd["Change"] = myd['TDAY'] / myd['YDAY']
        myd = myd.loc[(myd.Change <= 0.8) | (myd.Change >= 1.2)]
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)
        myd = myd.drop("Change", 1)

    output += '<h2>Customer Level Availability Exceptions</h2>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output += myd.to_html()
#***************************************
    startTime = datetime.datetime.now()
    print("CHECKING SEARCH TERMS AT RPC LEVEL IN: %s") % str(j)

    query = "select market,  if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, customers_id,  brand_owner, search_term, count(distinct trusted_rpc) as 'values'  from  rpt_share_of_search_hist where report_date = date_sub(curdate(), interval 1 day) and market = '" + str(j) + "' GROUP BY report_date, market, online_store,customers_id, search_term UNION select market, if (report_date = curdate(),'TDAY','YDAY') as report_date,online_store, customers_id,  brand_owner,  search_term, count(distinct trusted_rpc) as 'values' from  rpt_share_of_search_hist where report_date = curdate() and market = '" + str(j) + "' GROUP BY report_date, market, online_store,customers_id, search_term"

    df = pd.read_sql(query , conn)
    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)

    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['market','online_store','customers_id','brand_owner','search_term'], cols=['report_date'], values='values', fill_value=0)
        myd["Change"] = myd['TDAY'] / myd['YDAY']
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']

        myd = myd.loc[(myd.Change <= 0.8) | (myd.Change >= 1.2)]

        myd = myd[(myd.ABS_DIFF > 10)]

        myd = myd.drop("NOM_DIFF", 1)
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)
        myd = myd.drop("Change", 1)
        # myd1 = myd.reset_index()
        # myd_list_temp = myd1["market"].tolist()
        # myd_list = myd_list + myd_list_temp
    output+= '<p><h2>Search Terms Exceptions per RPC </h2></p>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output += myd.to_html()
#*****************************************

    startTime = datetime.datetime.now()
    print("CHECKING SEARCH SCORES AT CUSTOMER LEVEL EXTRACT: %s") % str(j)
    query = "select market,  if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, customers_id,  brand_owner,  round(avg(score),0) as 'values' from  rpt_share_of_search_hist where report_date = date_sub(curdate(), interval 1 day) and market = '" + str(j) + "' GROUP BY report_date,market, online_store,customers_id, brand_owner UNION select market, if (report_date = curdate(),'TDAY','YDAY') as report_date,online_store, customers_id,  brand_owner,  round(avg(score),0) as 'values' from  rpt_share_of_search_hist where report_date = curdate() and market = '" + str(j) + "' GROUP BY report_date,market, online_store,customers_id,  brand_owner"
    df = pd.read_sql(query , conn)

    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)

    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['market','online_store','brand_owner',"customers_id"], cols=['report_date'], values='values', fill_value=0)
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']
        myd  = myd[myd.NOM_DIFF < 1]
        myd  = myd[myd.DEC_DIFF > 0.05]
        myd  = myd[myd.ABS_DIFF>50]
        myd = myd.drop("NOM_DIFF", 1)
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)     

    output+= '<p><h2>Search Score Exceptions</h2></p>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output +=myd.to_html()

#*****************************************
    startTime = datetime.datetime.now()
    print("EXTRACTING REVIEW COUNT AT CUSTOMER LEVEL DATA IN: %s") % str(j)

    query = "select if (report_date = curdate(),'TDAY','YDAY') as report_date, region, online_store, customers_id, brand_owner, brand, round(sum(number_of_customer_reviews),0)  as 'values' from rpt_product_hist where report_date  = curdate() and region = '" + str(j) + "' GROUP BY report_date, region, online_store, customers_id, brand_owner UNION select if (report_date = curdate(),'TDAY','YDAY') as report_date, region, online_store, customers_id, brand_owner, brand, round(sum(number_of_customer_reviews),0)  as 'values' from rpt_product_hist where report_date = date_sub(curdate(), interval 1 day) and region = '" + str(j) + "' GROUP BY report_date, region, online_store, customers_id, brand_owner"
    df = pd.read_sql(query , conn)

    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)


    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store', 'brand_owner','customers_id'], cols=['report_date'], values='values', fill_value=0)
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']
        myd['TEN'] = myd['TDAY'] + 10
        myd  = myd[myd.TEN < myd.YDAY]
        myd = myd.drop("TEN", 1)
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)

    output+= '<p><h2>Store Level Review Count Exceptions</h2></p>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output +=myd.to_html()

#********************************************
    startTime = datetime.datetime.now()
    print("EXTRACTING RATINGS AT STORE LEVEL DATA IN: %s") % str(j)

    query ="select if (report_date = curdate(),'TDAY','YDAY') as report_date, region, online_store, customers_id, brand_owner, brand, avg(normalised_ratings)  as 'values' from rpt_product_hist where report_date  = curdate() and availability !='void' and region = '" + str(j) + "' GROUP BY report_date, region, online_store, customers_id, brand_owner UNION select if (report_date = curdate(),'TDAY','YDAY') as report_date, region, online_store, customers_id, brand_owner, brand, avg(normalised_ratings)  as 'values' from rpt_product_hist where report_date = date_sub(curdate(), interval 1 day)  and availability !='void' and region = '" + str(j) + "' GROUP BY report_date, region, online_store, customers_id, brand_owner"
    df = pd.read_sql(query , conn)

    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)

    #Create the pivot table
    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store', 'brand_owner', 'customers_id'], cols=['report_date'], values='values', fill_value=0)
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']
        myd  = myd[myd.ABS_DIFF > 0.8]
        myd = myd.drop("NOM_DIFF", 1)
        myd = myd.drop("ABS_DIFF", 1)
        myd = myd.drop("DEC_DIFF", 1)


    output+= '<p><h2>Store Level Average Ratings Exceptions</h2></p>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output +=myd.to_html()
#********************************************

    startTime = datetime.datetime.now()
    print("EXTRACTING PRICE CHANGE DATA IN: %s") % str(j)

    query = "select if (report_date = curdate(),'TDAY','YDAY') as report_date, region, online_store, customers_id, brand_owner, brand, avg(change_on_last_period)  as 'values' from rpt_product_hist  where report_date  = curdate() and availability != 'void' and region = '" + str(j) + "'  GROUP BY report_date, region, online_store, customers_id, brand_owner UNION select if (report_date = curdate(),'TDAY','YDAY') as report_date, region, online_store, customers_id, brand_owner, brand, avg(change_on_last_period)  as 'values' from rpt_product_hist where report_date = date_sub(curdate(), interval 1 day) and availability !='void'   and region = '" + str(j) + "' GROUP BY report_date, region, online_store, customers_id, brand_owner"

    df = pd.read_sql(query , conn)


    print "Time taken to run Query {}".format(datetime.datetime.now() - startTime)
    if df.empty:
        myd = pd.DataFrame()
    else:
        myd = df.pivot_table(rows=['region','online_store','customers_id','brand_owner'], cols=['report_date'], values='values', fill_value=0)
        myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
        myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
        myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']
        myd  = myd[myd.NOM_DIFF < 0]
        myd  = myd[myd.DEC_DIFF > 0.05]
        myd  = myd[myd.ABS_DIFF>5]

    output+= '<p><h2>Average Price Change on last Period Store Level Exceptions</h2></p>'
    if myd.empty:
        output += "NO CHANGE TODAY"
    else:
        output +=myd.to_html()


#*************************************************************

#GEO*******************************************************************

    # startTime =  datetimie.datetime.now()
    # print("EXTRACTING GEO DETAILS IN: %S") % str(J)

    # query = ""

    # df = pd.read_sql(query,conn)


    # print "Time taken to run Query {}".format(datetime.dateime.now() - startTime)

    # if "TDAY" and "YDAY" not in df.report_date:
    #     myd = pd.Dataframe()
    # else:
    #     myd = df.pivot_table()



    # output += "<p><h2> Change in GEO availability</h2></p>"
    # if myd.empty:
    #     output +=  "NO CAHNGE TODAY"
    # else:
    #     output += myd.to_html()






#********************************************8******************

#********************************************
    output +='</body></html>'
    output = output.encode('utf-8')
    with codecs.open(fName , mode='w') as f:
      f.write(output)
      f.close()



        
    


    

    
#     print('Querying ' +str(j[1]))
#     cur.execute(query)
#     data = cur.fetchall()



#     startTime = datetime.now()
#     print "EXTRACTING VOIDS DATA PER STORE"

# df = pd.read_sql("""select region, if (report_date = curdate(),'TDAY','YDAY') as report_date, online_store, brand_owner,  count(trusted_rpc) as 'RPC'  from  rpt_product_hist
# where report_date IN (DATE_SUB(curdate(),INTERVAL 1 DAY),curdate())
# and availability = %(test)s
# and region !='cn'
# GROUP  BY region, online_store, report_date""", conn1, params = "test": str(j[0]))

# print "Time taken to run Query {}".format(datetime.now() - startTime)


# myd = df.pivot_table(rows=['region','online_store'], cols=['report_date'], values='RPC', fill_value=0)

# #Add the DIFF column
# myd['NOM_DIFF'] = (myd['TDAY'] - myd['YDAY'])
# myd['ABS_DIFF'] = abs(myd['YDAY'] - myd['TDAY'])
# myd['DEC_DIFF'] = myd['ABS_DIFF']/myd['YDAY']

# # filter for >5% ignore decreases
# myd  = myd[myd.DEC_DIFF > 0.05]
# myd  = myd[myd.NOM_DIFF>20]

# myd = myd.drop("NOM_DIFF", 1)
# myd = myd.drop("ABS_DIFF", 1)
# myd = myd.drop("DEC_DIFF", 1)

# #myd1 = myd.reset_index()
# #myd_list_temp = myd1["region"].tolist()
# #myd_list = myd_list + myd_list_temp
# #HTML output
# output+= '<h2>Void Exceptions per store</h2>'

# #HTML output
# output +=myd.to_html()

#     f = csv.writer(codecs.open(fPath, 'w',encoding='utf-8', errors='ignore'),delimiter=',', lineterminator='\n')





    # for n in headers:
    #     head += str(headers[0]).strip('(\'').strip('\',)') + ','
    # head.strip(',')
    # head+='\n'
    # f.writerow(headers)
    # print('Writing ' + fPath)
    # for row in data:
    #     f.writerow(row)
    # head=''
    # print('Writing ', zFile)
    # zFile.write(fPath)
    # zFile.close()
    

# # Need to add headers and show time frames
# #Search
# cur = conn.cursor()


# query = "SELECT column_name FROM information_schema.columns  WHERE table_schema = DATABASE() AND table_name='rpt_share_of_search_hist' ORDER BY ordinal_position"
# cur.execute(query)
# tmp = cur.fetchall()
# headers = list(tmp)
# head=''

# # for j in rows:
#     fName = str(fileDTStem + '_' + str(j[1]) + '_NOVEMBER_search_table_dump.csv')
#     fPath = fName
#     zFile = zipfile.ZipFile((fName + '.zip'),'w', compression = zipfile.ZIP_DEFLATED)
#     query = "select * from rpt_share_of_search_hist where customers_id = '" + str(j[0]) + "' and  report_date >= '" + str(startDate) + "' and report_date <= '" + str(endDate) + "'"
#     print('Querying ' +str(j[1]))
#     cur.execute(query)
#     data = cur.fetchall()
#     f = csv.writer(codecs.open(fPath, 'w',encoding='utf-8', errors='ignore'),delimiter=',', lineterminator='\n')
#     for n in headers:
#         head += str(headers[0]).strip('(\'').strip('\',)') + ','
#     head.strip(',')
#     head+='\n'
#     f.writerow(headers)
#     print('Writing ' + fPath)
#     for row in data:
#         f.writerow(row)
#     head=''
#     print('Writing ', zFile)
#     zFile.write(fPath)
#     zFile.close()
    
# # #Integrity
