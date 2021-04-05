import pandas as pd
import numpy as np
import time
import datetime as datetime
import requests as req
import telebot
import ibm_db
import ibm_db_dbi

#Connecting to Binance API
from binance.client import Client

api_key = open("key.txt", "r").readline() # Read the Binance API key
api_secret = open("secret.txt", "r").readline() # Read the Binance Secret key
client = Client(api_key, api_secret) # Connection to the Binance Server

#Telegram bot initialization
chatID = 361222436 # ID of the telegram chat
bot_token = open("bot_key.txt", "r").readline() # Read the Telegram Bot Token
bot = telebot.TeleBot(bot_token, parse_mode = None) # Creating a Telebot class object

# Connecting to the DB
cloud_pass = open("ibm_cloud_pass.txt", "r").readline() # Read the credentials to the DB on IBM Cloud
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
def get_forex_price(prev_value): # The function to get EUR/USD rate
    try:
        resp = req.get("https://webrates.truefx.com/rates/connect.html?f=html") # Requesting EUR/USD exchange rate
        price = float(resp.text[53:69].replace("</td><td>",""))
        output = True # Output defines whether the forex data is availible and looking believable 
        
        # Sometimes it happens that the source of forex data otputs inadequate data. Let's put a barrier.
        if (price < 0.5) and (price > 2.0):
            pconn = ibm_db_dbi.Connection(conn)
            query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 limit 1"
            price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
            output = False    
            
        # The same kind of protection against fake jumps
        if (price/prev_value < 0.9985) or (price/prev_value > 1.0015) or (prev_value == None):
            pconn = ibm_db_dbi.Connection(conn)
            query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 order by TIME_STAMP desc limit 1"
            price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
            output = False
            
    # This data are not avaliable on weekends. In this case let's find the last avaliable value in the DB
    except:
        pconn = ibm_db_dbi.Connection(conn)
        query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 order by TIME_STAMP desc limit 1"
        price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
        output = False
    return price, output
    
def get_binance_price(x): # Getting prices from Binance as average 
    req = client.get_order_book(symbol=x) # Request the rate of pair x
    price_avg = (float(req['bids'][0][0])+float(req['asks'][0][0]))/2 # Price as an average ^ min bid and max ask  
    price_bid = float(req['bids'][0][0]) # Max bid 
    price_ask = float(req['asks'][0][0]) # Min ask
    return pd.DataFrame([[price_avg, price_bid, price_ask]], columns = ['AVG', 'BID', 'ASK'])

# Check if there are any open positions. Outputs the boolean varible (TRUE if there are open positions and FALSE if not) and the opening price.
def check_open_positions(conn):
    pconn = ibm_db_dbi.Connection(conn)
    query = "select TYPE, OPEN_PRICE from TRADE_HISTORY where STATUS = 'OPEN'"
    answer =pd.read_sql(query, pconn)
    if len(answer) == 0:
        return False, False
    elif len(answer) == 1:
        return answer["TYPE"][0].strip(), answer["OPEN_PRICE"][0]

# Sends a request to Binance to checks whether there are open oreders 
def check_open_positions_binance():
    open_orders = client.get_open_orders(symbol='EURBUSD')
    return len(open_orders)

def open_position(conn, chatID, imbalance, open_threshold, timestamp, binance_price, forex_price):
    current_pos, open_price = check_open_positions(conn)
    if (imbalance > open_threshold) and (current_pos == False):
        pos_type = "SELL EUR"
        text = "Short position is opened. Current imbalance:" + "% 1.2f" % imbalance + "%."
        print(text)
        bot.send_message(chatID, text)
        open_position_query(conn, timestamp, pos_type, binance_price, imbalance,forex_price)
        return "SELL"
        
    elif (imbalance < - open_threshold) and (current_pos == False):
        pos_type = "BUY EUR"
        text = "Long position is opened. Current imbalance:" + "% 1.2f" % imbalance + "%."
        print(text)
        bot.send_message(chatID, text)
        open_position_query(conn, timestamp, pos_type, binance_price, imbalance,forex_price)
        return "BUY"
    else:
        return None

def open_position_query(conn, timestamp, pos_type, binance_price, imbalance,forex_price):
    query = "insert into TRADE_HISTORY (TIME_STAMP_OPEN, TYPE, STATUS, OPEN_PRICE, OPEN_IMBALANCE, OPEN_FOREX)"
    query += " values("+str(timestamp)+",'"+pos_type+"', 'OPEN',"+str(binance_price)+","+str(imbalance)+","+str(forex_price)+")"
    ibm_db.exec_immediate(conn, query)
    return None
    

def close_position(conn, chatID, imbalance, close_threshold, timestamp, binance_price, forex_price):
    current_pos, open_price = check_open_positions(conn)
    if (current_pos == "SELL EUR") and (imbalance < close_threshold):
        profit = -100*(binance_price - open_price)/open_price
        pconn = ibm_db_dbi.Connection(conn)
        query = "UPDATE trade_history SET TIME_STAMP_CLOSE="
        query += str(timestamp)
        query += ",STATUS = 'CLOSED',"
        query += "CLOSE_PRICE ="+str(binance_price)
        query += ", CLOSE_IMBALANCE ="+str(imbalance)
        query += ", CLOSE_FOREX ="+str(forex_price)
        query += ", PROFIT ="+str(profit)
        query += " WHERE STATUS = 'OPEN'"
        ibm_db.exec_immediate(conn, query)
        text = "Short position is closed. Net PnL:" + "% 1.3f" % profit + "%."
        print(text)
        bot.send_message(chatID, text)
        return "SELL"
        
    elif (current_pos == "BUY EUR") and (imbalance > -close_threshold):
        profit = 100*(binance_price - open_price)/open_price
        pconn = ibm_db_dbi.Connection(conn)
        query = "UPDATE trade_history SET TIME_STAMP_CLOSE="
        query += str(timestamp)
        query += ",STATUS = 'CLOSED',"
        query += "CLOSE_PRICE ="+str(binance_price)
        query += ", CLOSE_IMBALANCE ="+str(imbalance)
        query += ", CLOSE_FOREX ="+str(forex_price)
        query += ", PROFIT ="+str(profit)
        query += " WHERE STATUS = 'OPEN'"
        ibm_db.exec_immediate(conn, query)
        text = "Long position is closed. Net PnL:" + "% 1.3f" % profit + "%."
        print(text)
        bot.send_message(chatID, text)
        return "BUY"
    
    else:
        return None
    
threshold_def = 0.4 # minimum arbitrage we are interested in
threshold = threshold_def # initial threshold
threshold_0 = threshold_def # axilary variable
delta = 0.4 # step in minimum arbitrage to send the next notification
time_cond = datetime.datetime.now().timestamp() # further it will be the time when the last notification was sent.
pconn = ibm_db_dbi.Connection(conn)
prev_value_forex = None

try:
    while True:
        
        forex_price, forex_output = get_forex_price(prev_value_forex)
        prev_value_forex = forex_price
        binance_price = get_binance_price('EURBUSD') # get EUR_BUSD price
        binance_btc_price = get_binance_price('BTCEUR')["AVG"][0] # get BTC_EUR price (just to have more data for analysis)
        imbalance = binance_price["AVG"][0]/forex_price*100-100 # the arbitrage difference in %
        # Here we update the threshold, becasuse we don't want the bot to send us messages every second if the throshold is exceded
        # Only if the arbitrage diff got gain more or eq than delta. The threshold decays linearly with time (in 7200sec fully recoveres).  
        threshold = max(threshold_def, threshold_0 - (threshold_0-threshold_def)*(datetime.datetime.now().timestamp()-time_cond)/7200)
        timestamp = int(datetime.datetime.now().timestamp()) #timestamp up to seconds
        
        open_positions = check_open_positions(conn)
        # If there is an open position, check whether we have to close it, and if there is not, check whether we have to open it 
        if open_positions[0]:
            action = close_position(conn, chatID, imbalance, 0.05, timestamp, binance_price["AVG"][0], forex_price)
            if action == "SELL":
                #quantity = float("%.2f" % float(client.get_asset_balance(asset='BUSD')['free']))-0.5
                client.order_limit_buy(
                    symbol ='EURBUSD',
                    quantity = 20,
                    price = "{:0.0{}f}".format(binance_price["BID"][0], 4))
            if action == "BUY":
                #quantity = float("%.2f" % float(client.get_asset_balance(asset='EUR')['free']))-0.5
                client.order_limit_sell(
                    symbol ='EURBUSD',
                    quantity = 20,
                    price = "{:0.0{}f}".format(binance_price["ASK"][0], 4))
        else:
            action = open_position(conn, chatID, imbalance, 0.2, timestamp, binance_price["AVG"][0], forex_price)
            if action == "SELL":
                #quantity = float("%.2f" % float(client.get_asset_balance(asset='EUR')['free']))-0.5
                client.order_limit_sell(
                    symbol ='EURBUSD',
                    quantity = 20,
                    price = "{:0.0{}f}".format(binance_price["ASK"][0], 4))
            if action == "BUY":
                #quantity = float("%.2f" % float(client.get_asset_balance(asset='BUSD')['free']))-0.5
                client.order_limit_buy(
                    symbol ='EURBUSD',
                    quantity = 20,
                    price = "{:0.0{}f}".format(binance_price["BID"][0], 4))
                 
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
        
        # Sends data to the db for further analysis  
        if forex_output:
            query = "insert into crypto_db (TIME_STAMP, EUR_BUSD, EUR_USD_REAL, EUR_USD_USED, DIFF_IN_PERC, BIT_EUR) values ("
            query += str(timestamp) + ","
            query += str(binance_price["AVG"][0]) + ","
            query += str(forex_price) + ","
            query += str(forex_price) + ","
            query += str(imbalance) + ","
            query += str(binance_btc_price) +")"
            ibm_db.exec_immediate(conn, query)
        else:
            query = "insert into crypto_db (TIME_STAMP, EUR_BUSD, EUR_USD_USED, DIFF_IN_PERC, BIT_EUR) values ("
            query += str(timestamp) + ","
            query += str(binance_price["AVG"][0]) + ","
            query += str(forex_price) + ","
            query += str(imbalance) + ","
            query += str(binance_btc_price) +")" 
            ibm_db.exec_immediate(conn, query)
        
        print("% 1.2f " % imbalance +"%. " + "EUR/USD:" + "% 1.4f " % forex_price)    
        time.sleep(10)
except KeyboardInterrupt:
    print('interrupted!')