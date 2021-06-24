import pandas as pd
import numpy as np
import time
import datetime as datetime
import requests as req
import telebot
import ibm_db
import ibm_db_dbi
import import_export_data as ied
import open_position as op
import close_position as cp
from binance.enums import *

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


# Initial parameters:

# The following 5 threshold parameters are used only for sending notifications: 

threshold_def = 0.5 # minimum arbitrage to send a notification
threshold = threshold_def # threshold which will be uptadet at every step
threshold_0 = threshold_def # axilary threshold variable which will be updated only if the send_not function trigers it
delta = 0.5 # step in minimum arbitrage to send the next notification
time_cond = datetime.datetime.now().timestamp() # further it will be the time when the last notification was sent.

# The rest threshold parameters are used only for placing, updating, and canceling orders:
close_arbitrage = 0.06 # condition for abs(imbalance) to close a position 
open_arbitrage = 0.09 # condition for abs(imbalance) to open a position 
allowed_imblance_change = 0.02 # max allowed imbalance change when updating an order
allowed_price_change = 0.0001 # max allowed gap between order price and market offers change when updating an order

pconn = ibm_db_dbi.Connection(conn)
prev_value_forex = ied.get_forex_price(None, conn)

try:
    while True:
        
        forex_price, forex_output, prev_value_forex, binance_price, binance_btc_price, imbalance, threshold, timestamp = ied.get_data(prev_value_forex, threshold_def, threshold_0, time_cond, client, conn)
        
        open_positions = ied.check_open_positions(conn)
        # If there is an open position, check whether we have to close it, and if there is not, check whether we have to open it 
        if open_positions[2] == 'OPEN':
            action = cp.place_close_order(conn, open_positions[0], chatID, bot,
                                       imbalance, close_arbitrage,
                                       timestamp, binance_price, forex_price,
                                       open_positions[1], client)

        elif open_positions[2] == False:
            action = op.place_open_order(conn = conn, chatID = chatID, bot = bot,
                                      imbalance = imbalance, open_threshold = open_arbitrage,
                                      timestamp = timestamp,
                                      binance_price = binance_price,
                                      forex_price = forex_price,
                                      client = client)
        
        # If there is an open order, check whether it's filled or not. If not, check whteher we have to update it.
        elif open_positions[2] == 'PLACED_OPEN':
            
            # Sometimes Binance returns an error when requesting an order status. So we keep requesting it till success
            while True:
                try:
                    order = client.get_margin_order(symbol = 'EURBUSD',
                                        orderId = str(open_positions[3]))
                    
                except:
                    continue
                
                break
            
            if order['status'] == 'NEW':
                op.update_open_order(conn = conn, chatID = chatID, bot = bot,
                                  imbalance = imbalance, open_threshold = open_arbitrage, 
                                  binance_price = binance_price, forex_price = forex_price, 
                                  al_imb_ch = allowed_imblance_change,
                                  al_price_ch = allowed_price_change,
                                  open_price = float(order['price']),
                                  open_positions = open_positions,
                                  client = client)
                
                
            if order["status"] == 'FILLED':
                op.confirm_opened_order(open_positions[3], chatID, bot, conn,
                                    float(order["price"]), imbalance, forex_price, timestamp)
        
        # If there is a open order, check whether it's filled or not. If not, check whteher we have to update it.     
        elif open_positions[2] == 'PLACED_CLOSE':     
            # Sometimes Binance returns an error when requesting an order status. So we keep requesting it till success
            while True:
                try:
                    order = client.get_margin_order(symbol = 'EURBUSD',
                                    orderId = str(open_positions[3]))
                except:
                    continue
                break
            
            if order['status'] == 'NEW':
                cp.update_close_order(conn = conn, chatID = chatID, bot = bot, client = client,
                                  imbalance = imbalance, close_threshold = close_arbitrage, 
                                  binance_price = binance_price, forex_price =forex_price, 
                                  al_imb_ch = allowed_imblance_change, al_price_ch = allowed_price_change,
                                  close_price = float(order['price']), open_positions = open_positions)
                
            if order["status"] == 'FILLED':
                cp.confirm_closed_order(order_id = open_positions[3], chatID = chatID, bot = bot, conn = conn,
                                    timestamp = timestamp,
                                    pos_type = open_positions[0], close_price_actual = float(order["price"]),
                                    open_price_actual = open_positions[1],
                                    close_imbalance_actual = imbalance, close_forex_actual = forex_price)
            
        ied.send_data(forex_output, timestamp, binance_price, forex_price, imbalance, binance_btc_price, conn)
        
        send_notif = ied.send_not(imbalance, threshold, binance_price, chatID, bot, delta)
        
        if send_notif:
            time_cond, threshold, threshold_0 = send_notif
           
        time.sleep(1)
except KeyboardInterrupt:
    print('interrupted!')