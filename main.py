import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import datetime as datetime
import requests as req
import telebot

#Binance API
from binance.client import Client

f1 = open("key.txt", "r") # Read the Binance API key
f2 = open("secret.txt", "r") # Read the Binance Secret key
f3 = open("bot_key.txt", "r") # Read the Telegram Bot key
api_key = f1.readline()
api_secret = f2.readline()
api_bot = f3.readline()
client = Client(api_key, api_secret) # Connection to the Binance Server
chatID = 361222436 # ID of the telegram chat

bot = telebot.TeleBot(api_bot, parse_mode=None) # Creating a Telebot class object

def get_forex_price(): # The function to get EUR/USD rate
    try:
        resp = req.get("https://webrates.truefx.com/rates/connect.html?f=html") # Requesting EUR/USD exchange rate
        price = float(resp.text[53:69].replace("</td><td>",""))
    # This data are not avaliable on weakends. So I input the actual value on 16.01.21. Later I will attach the database and read the last available value
    except:
        price = 1.2128
    
    return price
    
def get_binance_price(): # The function to get EUR/BUSD rate from Binance
    req = client.get_order_book(symbol='EURBUSD') # Request EUR/BUSD rate
    price = (float(req['bids'][0][0])+float(req['asks'][0][0]))/2 # Price as an average between bid and ask 
    return price

threshold_def = 0.2 # minimum arbitrage we are interested in
threshold = threshold_def # initial threshold
threshold_0 = threshold_def # axilary variable
delta = 0.2 # step in minimum arbitrage to send the next notification
time_cond = datetime.datetime.now().timestamp() # further it will be the time when the last notification was sent.  

try:
    while True:
        
        forex_price = get_forex_price()
        binance_price = get_binance_price()
        imbalance = binance_price/forex_price*100-100 # the arbitrage difference in %
        # Here we update the threshold, becasuse we don't want the bot to send us messages every second if the throshold is exceded
        # Only if the arbitrage diff got gain more or eq than delta. The threshold decays linearly with time (in 7200sec fully recoveres).  
        threshold = max(threshold_def, threshold_0 - (threshold_0-threshold_def)*(datetime.datetime.now().timestamp()-time_cond)/7200)
        
        
        if imbalance >= threshold: # did we hit the threshold and should sell EUR?
            text = "Sell EUR: " + "% 1.2f " % imbalance +"%. " + "EUR/USD:" + "% 1.4f " % forex_price
            bot.send_message(chatID, text)
            time_cond = datetime.datetime.now().timestamp() # Datestamp for threshold decay
            threshold = threshold + delta
            threshold_0 = threshold

        if imbalance <= -threshold: #did we hit the threshold and should buy EUR?
            text = "Buy EUR: " + "% 1.2f " % imbalance +"%. " + "EUR/USD:" + "% 1.4f " % forex_price
            bot.send_message(chatID, text)
            time_cond = datetime.datetime.now().timestamp()
            threshold = threshold + delta
            threshold_0 = threshold
            
except KeyboardInterrupt:
    print('interrupted!')