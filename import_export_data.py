import pandas as pd
import numpy as np
import time
import datetime as datetime
import requests as req
import telebot
import ibm_db
import ibm_db_dbi
import telebot
from binance.client import Client

def get_forex_price(prev_value, conn): # The function to get EUR/USD rate
    try:
        resp = req.get("https://webrates.truefx.com/rates/connect.html?f=html") # Requesting EUR/USD exchange rate
        price = float(resp.text[53:69].replace("</td><td>",""))
        output = True # Output defines whether the forex data is availible and looking believable 
        
        # Sometimes it happens that the source of forex data outputs inadequate data. Let's put some measure what's the allowed range of the forex price.
        if (price < 0.5) and (price > 2.0):
            pconn = ibm_db_dbi.Connection(conn)
            query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 limit 1"
            price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
            output = False    
            
        # The same kind of protection against fake jumps
        if ((price/prev_value < 0.9985) or (price/prev_value > 1.0015)) and (prev_value != None):
            pconn = ibm_db_dbi.Connection(conn)
            query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 order by TIME_STAMP desc limit 1"
            price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
            output = False
            
        # Activated at the first itteration after launching the script   
        if (prev_value == None):
            return price, True
            
    # The forex data is not avaliable on weekends. In this case let's find the last avaliable value in the DB
    except:
        pconn = ibm_db_dbi.Connection(conn)
        query = "select EUR_USD_REAL from crypto_db where EUR_USD_REAL > 0 order by TIME_STAMP desc limit 1"
        price = pd.read_sql(query, pconn)["EUR_USD_REAL"][0]
        output = False
    return price, output
    
def get_binance_price(x, client): # Getting prices from Binance as average 
    req = client.get_order_book(symbol=x) # Request the rate of pair x
    price_avg = (float(req['bids'][0][0])+float(req['asks'][0][0]))/2 # Price as an average ^ min bid and max ask  
    price_bid = float(req['bids'][0][0]) # Max bid 
    price_ask = float(req['asks'][0][0]) # Min ask
    return pd.DataFrame([[price_avg, price_bid, price_ask]], columns = ['AVG', 'BID', 'ASK'])

def get_data(prev_value_forex, threshold_def, threshold_0, time_cond, client, conn):
    forex_price, forex_output = get_forex_price(prev_value_forex, conn)
    prev_value_forex = forex_price
    binance_price = get_binance_price('EURBUSD', client) # get EUR_BUSD price
    binance_btc_price = get_binance_price('BTCEUR', client)["AVG"][0] # get BTC_EUR price to have more data for analysis
    imbalance = binance_price["AVG"][0]/forex_price*100-100 # the arbitrage difference in %
    # Here we update the threshold, becasuse we don't want the bot to send us notifications at every step.
    # Only if the arbitrage diff gained more or eq than delta. The threshold decays linearly with time (in 7200 sec fully recoveres).  
    threshold = max(threshold_def, threshold_0 - (threshold_0-threshold_def)*(datetime.datetime.now().timestamp()-time_cond)/7200)
    timestamp = int(datetime.datetime.now().timestamp()) #timestamp up to seconds
    
    return forex_price, forex_output, prev_value_forex, binance_price, binance_btc_price, imbalance, threshold, timestamp

# Checks if there are open positions and outputs a boolean variable (TRUE if there are open positions and FALSE if not) and the opening price.
def check_open_positions(conn):
    pconn = ibm_db_dbi.Connection(conn)
    query = "select TYPE, OPEN_PRICE_ACTUAL, STATUS, ORDER_ID_OPEN, ORDER_ID_CLOSE from TRADE_HISTORY where STATUS in ('OPEN','PLACED_OPEN','PLACED_CLOSE')"
    answer = pd.read_sql(query, pconn)
    
    if len(answer) == 0:
        return False, False, False, False
    
    elif (len(answer) == 1) and (answer["STATUS"][0].strip() == "PLACED_OPEN"):
        return answer["TYPE"][0].strip(), answer["OPEN_PRICE_ACTUAL"][0], answer["STATUS"][0].strip(), answer["ORDER_ID_OPEN"][0]
    
    elif (len(answer) == 1) and (answer["STATUS"][0].strip() == "PLACED_CLOSE"):
        return answer["TYPE"][0].strip(), answer["OPEN_PRICE_ACTUAL"][0], answer["STATUS"][0].strip(), answer["ORDER_ID_CLOSE"][0]
    
    elif (len(answer) == 1) and (answer["STATUS"][0].strip() == "OPEN"):
        return answer["TYPE"][0].strip(), answer["OPEN_PRICE_ACTUAL"][0], answer["STATUS"][0].strip(), answer["ORDER_ID_OPEN"][0]
    
# Sends a request to Binance to check whether there are open oreders 
def check_open_positions_binance(client):
    open_orders = client.get_open_orders(symbol='EURBUSD')
    return len(open_orders)

# Sends data to the db for further analysis     
def send_data(forex_output, timestamp, binance_price, forex_price, imbalance, binance_btc_price, conn):
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
    print("% 1.2f " % imbalance + "%. " + "EUR/USD:" + "% 1.4f " % forex_price + "EUR/BUSD:" + "% 1.4f " % binance_price["AVG"][0])
    
    return None

# Sends a notification when large imbalance is observed 
def send_not(imbalance, threshold, binance_price, chatID, bot, delta):

    if imbalance >= threshold: # did we hit the threshold to send a notification?
        text = "\U0001F4A5 Large deviation up! \n Current imbalance: " + "% 1.2f " % imbalance +"%. " + "EUR/BUSD:" + "% 1.4f " % binance_price["ASK"][0] + "."
        bot.send_message(chatID, text)
        time_cond = datetime.datetime.now().timestamp() # Datestamp for threshold decay
        
        return time_cond, threshold + delta, threshold + delta

    if imbalance <= -threshold: #did we hit the threshold to send a notification?
        text = "\U0001F4A5 Large deviation down! \n Imbalance: " + "% 1.2f " % imbalance +"%. " + "EUR/BUSD:" + "% 1.4f " % binance_price["BID"][0] + "."
        bot.send_message(chatID, text)
        time_cond = datetime.datetime.now().timestamp() # Datestamp for threshold decay
            
        return time_cond, threshold + delta, threshold + delta
    
    else:
        
        return False

# Makes a request to Binance and returns free amount (balance) of the selected asset
def get_balance(client, asset, convert_to_eur, price):

    info = client.get_margin_account()['userAssets']
    
    for el in info:
        if el['asset'] == asset:
            quantity = float('%.2f' % (float(el['free'])  - 0.01))
            break
            
    if convert_to_eur == False:
        return quantity
    
    if convert_to_eur == True:
        return float('%.2f' % (quantity/price))