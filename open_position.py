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
from binance.enums import *
import import_export_data as ied

# Places an open order if conditions are met
def place_open_order(conn, chatID, bot, imbalance, open_threshold, timestamp, binance_price, forex_price, client):
    if (imbalance > open_threshold):
        pos_type = "SELL EUR"
        quantity = ied.get_balance(client, 'EUR', False, None)
        order = client.create_margin_order(symbol = 'EURBUSD',
                                          side = SIDE_SELL,
                                          type = ORDER_TYPE_LIMIT,
                                          timeInForce = TIME_IN_FORCE_GTC,
                                          quantity = quantity,
                                          price = "{:0.0{}f}".format(binance_price["ASK"][0], 4)
                                          )
        text = "\U0001F53D Sell order is placed. \n Imbalance: " + "% 1.2f " % imbalance +"%. " + "EUR/BUSD:" + "% 1.4f " % binance_price["ASK"][0]
        bot.send_message(chatID, text)
        placed_open_order_query(conn, timestamp, pos_type, binance_price["ASK"][0], imbalance, forex_price, order['orderId'])
        print(text)
        return "SELL", order['orderId']
        
    elif (imbalance < - open_threshold):
        pos_type = "BUY EUR"
        quantity = ied.get_balance(client, 'BUSD', True, float("{:0.0{}f}".format(binance_price["BID"][0], 4)) )
        order = client.create_margin_order(symbol='EURBUSD',
                                          side=SIDE_BUY,
                                          type=ORDER_TYPE_LIMIT,
                                          timeInForce=TIME_IN_FORCE_GTC,
                                          quantity = quantity,
                                          price = "{:0.0{}f}".format(binance_price["BID"][0], 4)
                                          )
        text = "\U0001F53C Buy order is placed. \n Imbalance: " + "% 1.2f " % imbalance +"%. " + "EUR/BUSD:" + "% 1.4f " % binance_price["BID"][0]
        bot.send_message(chatID, text)
        placed_open_order_query(conn, timestamp, pos_type, binance_price["BID"][0], imbalance, forex_price, order['orderId'])
        print(text)
        return "BUY", order['orderId']
    else:
        return None

def placed_open_order_query(conn, timestamp, pos_type, binance_price, imbalance,forex_price, order_id):
    query = "insert into TRADE_HISTORY (TIME_STAMP_OPEN, TYPE, STATUS, OPEN_PRICE, OPEN_IMBALANCE, OPEN_FOREX, ORDER_ID_OPEN)"
    query += " values("+str(timestamp)+",'"+pos_type+"', 'PLACED_OPEN',"+str(binance_price)+","+str(imbalance)+","+str(forex_price) + "," + str(order_id) + ")"
    ibm_db.exec_immediate(conn, query)
    return None

# Checks if an open order needs to be updated
def update_open_order(conn, chatID, bot, imbalance, open_threshold,
                      binance_price, forex_price, al_imb_ch, al_price_ch,
                      open_price, open_positions, client):
    
    if open_positions[0] == "SELL EUR":
        
        # If arbitrage opportinity is gone, cancel the order
        if (imbalance + al_imb_ch < open_threshold):
            
            try:
                order = client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
            except:
                return
            
            cancel_open_order_query(conn, open_positions[3])
            text = "Open order is canceled. Imbalance got too low."
            bot.send_message(chatID, text)
            print(text)
            
        # If it's not, check that we are not too far (within 'al_price_ch') away from market offers
        else:
            
            if binance_price["ASK"][0] + al_price_ch < open_price:
                
                try:
                    client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
                except:
                    return
                
                quantity = ied.get_balance(client, 'EUR', False, None)
                order = client.create_margin_order(symbol='EURBUSD',
                                          side=SIDE_SELL,
                                          type=ORDER_TYPE_LIMIT,
                                          timeInForce=TIME_IN_FORCE_GTC,
                                          quantity = quantity,
                                          price = "{:0.0{}f}".format(binance_price["ASK"][0] + al_price_ch + 0.0001, 4)
                                          )
                
                update_open_order_query(conn = conn, order_id = open_positions[3],
                                        imbalance = imbalance,
                                        new_open_price = binance_price["ASK"][0] + al_price_ch,
                                        forex_price = forex_price,
                                        new_order_id = order['orderId'])
                
                text = "Open order has been updated. Open price: "+ "% 1.4f " % open_price
                text += "->" + "% 1.4f " % (binance_price["ASK"][0] + al_price_ch)
                bot.send_message(chatID, text)
                print(text)
        
    if open_positions[0] == "BUY EUR":
        
        # If arbitrage opportinity is gone, cancel the order
        if (imbalance - al_imb_ch > - open_threshold):
            
            try:
                order = client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
            except:
                return
            
            cancel_open_order_query(conn, open_positions[3])
            text = "Open order is canceled. Imbalance got too low."
            bot.send_message(chatID, text)
            print(text)
            
        # If it's not, check that we are not too far (within 'al_price_ch') away from market offers
        else:
            
            if binance_price["BID"][0] - al_price_ch > open_price:
                
                try:
                    client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
                except:
                    return
                quantity = ied.get_balance(client, 'BUSD', True, float("{:0.0{}f}".format(binance_price["BID"][0], 4)) )
                order = client.create_margin_order(symbol='EURBUSD',
                                          side=SIDE_BUY,
                                          type=ORDER_TYPE_LIMIT,
                                          timeInForce=TIME_IN_FORCE_GTC,
                                          quantity = quantity,
                                          price = "{:0.0{}f}".format(binance_price["BID"][0] - al_price_ch, 4)
                                          )
                
                update_open_order_query(conn = conn, order_id = open_positions[3],
                                        imbalance = imbalance,
                                        new_open_price = binance_price["BID"][0] - al_price_ch - 0.0001,
                                        forex_price = forex_price,
                                        new_order_id = order['orderId'])
                
                text = "Open order has been updated. Open price: "+ "% 1.4f " % open_price
                text += "->" + "% 1.4f " % (binance_price["BID"][0] - al_price_ch)
                bot.send_message(chatID, text)
                print(text)
    return None
        

def cancel_open_order_query(conn, order_id):
    query = "UPDATE trade_history SET STATUS = 'CANCELED' where ORDER_ID_OPEN = " + str(order_id)
    ibm_db.exec_immediate(conn, query)
    return None

def update_open_order_query(conn, order_id, imbalance, new_open_price, forex_price, new_order_id):
    query = "UPDATE trade_history SET OPEN_IMBALANCE_ACTUAL = " + str(imbalance)
    query += ", OPEN_PRICE_ACTUAL = " + str(new_open_price)
    query += ", OPEN_FOREX_ACTUAL = " + str(forex_price)
    query += ", ORDER_ID_OPEN = "+ str(new_order_id)
    query += " where ORDER_ID_OPEN = " + str(order_id)
    ibm_db.exec_immediate(conn, query)
    return None

def confirm_opened_order(order_id, chatID, bot, conn, open_price_actual, open_imbalance_actual, open_forex_actual, timestamp):
    pconn = ibm_db_dbi.Connection(conn)
    query = "update TRADE_HISTORY SET STATUS = 'OPEN', "
    query += "OPEN_PRICE_ACTUAL = " + str(open_price_actual)
    query += ", OPEN_IMBALANCE_ACTUAL = " + str(open_imbalance_actual)
    query += ", OPEN_FOREX_ACTUAL = " + str(open_forex_actual)
    query += ", TIME_STAMP_OPEN_ACTUAL = " + str(timestamp)
    query += " where ORDER_ID_OPEN = " + str(order_id)
    ibm_db.exec_immediate(conn, query)
    text = "Open order is filled. \n" + "Open price: " + "% 1.4f" % open_price_actual + ", imbalance: " + "% 1.2f " % open_imbalance_actual + "%."
    print(text)
    bot.send_message(chatID, text)
    return None