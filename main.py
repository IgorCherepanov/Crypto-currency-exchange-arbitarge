import pandas as pd
import numpy as np
import time
import datetime as datetime
import requests as req
import telebot
import ibm_db
import ibm_db_dbi

#Binance API
from binance.client import Client

f1 = open("key.txt", "r") # Read the Binance API key
f2 = open("secret.txt", "r") # Read the Binance Secret key
f3 = open("bot_key.txt", "r") # Read the Telegram Bot key
f4 = open("ibm_cloud_pass.txt", "r") # Read the credentials to the DB on IBM Cloud
api_key = f1.readline()
api_secret = f2.readline()
api_bot = f3.readline()
cloud_pass = f4.readline()
client = Client(api_key, api_secret) # Connection to the Binance Server
chatID = 361222436 # ID of the telegram chat

bot = telebot.TeleBot(api_bot, parse_mode = None) # Creating a Telebot class object

# Connecting to the DB
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BLUDB"
dsn_port = "50000"
dsn_hostname = "dashdb-txn-sbox-yp-dal09-10.services.dal.bluemix.net" 
dsn_protocol = "TCPIP"
dsn_uid = "cbn78182"
dsn_pwd = cloud_pass

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd)
conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

#####Functions we will need###
def get_forex_price(): # The function to get EUR/USD rate
    try:
        resp = req.get("https://webrates.truefx.com/rates/connect.html?f=html") # Requesting EUR/USD exchange rate
        price = float(resp.text[53:69].replace("</td><td>",""))
        output = True
    # This data are not avaliable on weekends. In this case let's find the last avaliable value in the DB
    except:
        pconn = ibm_db_dbi.Connection(conn)
        query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 limit 1"
        price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
        output = False
    return price, output
    
def get_binance_price(x): # The function to get prices from Binance
    req = client.get_order_book(symbol=x) # Request EUR/BUSD rate
    price = (float(req['bids'][0][0])+float(req['asks'][0][0]))/2 # Price as an average between bid and ask 
    return price

threshold_def = 0.4 # minimum arbitrage we are interested in
threshold = threshold_def # initial threshold
threshold_0 = threshold_def # axilary variable
delta = 0.4 # step in minimum arbitrage to send the next notification
time_cond = datetime.datetime.now().timestamp() # further it will be the time when the last notification was sent.  

try:
    while True:
        
        forex_price, forex_output = get_forex_price()
        binance_price = get_binance_price('EURBUSD') # get EUR_BUSD price
        binance_btc_price = get_binance_price('BTCEUR') # get BTC_EUR price (just to have more data for analysis)
        imbalance = binance_price/forex_price*100-100 # the arbitrage difference in %
        # Here we update the threshold, becasuse we don't want the bot to send us messages every second if the throshold is exceded
        # Only if the arbitrage diff got gain more or eq than delta. The threshold decays linearly with time (in 7200sec fully recoveres).  
        threshold = max(threshold_def, threshold_0 - (threshold_0-threshold_def)*(datetime.datetime.now().timestamp()-time_cond)/7200)
        timestamp = int(datetime.datetime.now().timestamp()) #timestamp up to seconds
        
        
        if imbalance >= threshold: # did we hit the threshold and should sell EUR?
            text = "Sell EUR: " + "% 1.2f " % imbalance +"%. " + "EUR/USD:" + "% 1.4f " % forex_price
            bot.send_message(chatID, text)
            time_cond = datetime.datetime.now().timestamp() # Datestamp for threshold decay
            threshold = threshold + delta
            threshold_0 = threshold

        if imbalance <= -threshold: #did we hit the threshold and should buy EUR?
            text = "Buy EUR: " + "% 1.2f " % imbalance +"%. " + "EUR/USD:" + "% 1.4f " % forex_price
            bot.send_message(chatID, text)
            time_cond = datetime.datetime.now().timestamp() # Datestamp for threshold decay
            threshold = threshold + delta
            threshold_0 = threshold
            
        if forex_output:
            query = "insert into crypto_db (TIME_STAMP, EUR_BUSD, EUR_USD_REAL, EUR_USD_USED, DIFF_IN_PERC, BIT_EUR) values ("
            query += str(timestamp) + ","
            query += str(binance_price) + ","
            query += str(forex_price) + ","
            query += str(forex_price) + ","
            query += str(imbalance) + ","
            query += str(binance_btc_price) +")"
            ibm_db.exec_immediate(conn, query)
        else:
            query = "insert into crypto_db (TIME_STAMP, EUR_BUSD, EUR_USD_USED, DIFF_IN_PERC, BIT_EUR) values ("
            query += str(timestamp) + ","
            query += str(binance_price) + ","
            query += str(forex_price) + ","
            query += str(imbalance) + ","
            query += str(binance_btc_price) +")" 
            ibm_db.exec_immediate(conn, query)
        
        print(text)    
        time.sleep(10)
except KeyboardInterrupt:
    print('interrupted!')