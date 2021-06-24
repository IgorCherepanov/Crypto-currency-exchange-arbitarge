import pandas as pd
import numpy as np
import time
import datetime as datetime
import requests as req
import telebot
import ibm_db
import ibm_db_dbi
import telebot
import import_export_data as ied
from binance.client import Client
from binance.enums import *

# Places a close order if conditions are met
def place_close_order(conn, current_pos, chatID, bot, imbalance, close_threshold, timestamp, binance_price, forex_price, open_price, client):
    if (current_pos == "SELL EUR") and (imbalance < close_threshold):
        
        quantity = ied.get_balance(client, 'BUSD', True, float("{:0.0{}f}".format(binance_price["BID"][0], 4)) )
        order = client.create_margin_order(symbol='EURBUSD',
                                          side=SIDE_BUY,
                                          type=ORDER_TYPE_LIMIT,
                                          timeInForce=TIME_IN_FORCE_GTC,
                                          quantity = float('%.2f' % (quantity/2)),
                                          price = "{:0.0{}f}".format(binance_price["BID"][0], 4)
                                          )
        
        profit = -100*(binance_price["BID"][0] - open_price)/open_price
        pconn = ibm_db_dbi.Connection(conn)
        query = "UPDATE trade_history SET TIME_STAMP_CLOSE="
        query += str(timestamp)
        query += ",STATUS = 'PLACED_CLOSE',"
        query += "CLOSE_PRICE ="+str(binance_price["BID"][0])
        query += ", CLOSE_IMBALANCE ="+str(imbalance)
        query += ", CLOSE_FOREX ="+str(forex_price)
        query += ", PROFIT ="+str(profit)
        query += ", ORDER_ID_CLOSE ="+str(order['orderId'])
        query += " WHERE STATUS = 'OPEN'"
        ibm_db.exec_immediate(conn, query)
        text = "Close order is placed. Please wait till it's filled."
        print(text)
        bot.send_message(chatID, text)
        query_2 = "update trade_history set DURATION_IN_MINS = (TIME_STAMP_CLOSE - TIME_STAMP_OPEN) / 60"
        ibm_db.exec_immediate(conn, query_2)
        
        return "SELL", order['orderId']
        
    elif (current_pos == "BUY EUR") and (imbalance > - close_threshold):
        
        quantity = ied.get_balance(client, 'EUR', False, None)
        order = client.create_margin_order(symbol='EURBUSD',
                                side=SIDE_SELL,
                                type=ORDER_TYPE_LIMIT,
                                timeInForce=TIME_IN_FORCE_GTC,
                                quantity = float('%.2f' % (quantity/2)),
                                price = "{:0.0{}f}".format(binance_price["ASK"][0], 4)
                                )
        
        profit = 100*(binance_price["ASK"][0] - open_price)/open_price
        pconn = ibm_db_dbi.Connection(conn)
        query = "UPDATE trade_history SET TIME_STAMP_CLOSE="
        query += str(timestamp)
        query += ",STATUS = 'PLACED_CLOSE',"
        query += "CLOSE_PRICE ="+str(binance_price["ASK"][0])
        query += ", CLOSE_IMBALANCE ="+str(imbalance)
        query += ", CLOSE_FOREX ="+str(forex_price)
        query += ", PROFIT ="+str(profit)
        query += ", ORDER_ID_CLOSE ="+str(order['orderId'])
        query += " WHERE STATUS = 'OPEN'"
        ibm_db.exec_immediate(conn, query)
        text = "Close order is placed. Please wait till it's filled."
        print(text)
        bot.send_message(chatID, text)
        query_2 = "update trade_history set DURATION_IN_MINS = (TIME_STAMP_CLOSE - TIME_STAMP_OPEN) / 60"
        ibm_db.exec_immediate(conn, query_2)
        
        return "BUY", order['orderId']
    
    else:
        
        return None
    
# Checks if a close order needs to be updated
def update_close_order(conn, chatID, bot, client, imbalance, close_threshold,
                      binance_price, forex_price, al_imb_ch, al_price_ch,
                      close_price, open_positions):
    
    if open_positions[0] == "SELL EUR":
        
        # If we are not hitting the close_thresghold, cancel the order
        if (imbalance - al_imb_ch > close_threshold):
            order = client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
            cancel_close_order_query(conn, order['orderId'])
            text = "Close order is canceled. Imbalance is still high."
            bot.send_message(chatID, text)
            print(text)
            
        # If it's not, check that we are not too far (within 'al_price_ch') away from market offers
        else:
            
            if binance_price["BID"][0] - al_price_ch > close_price:
                client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
                
                quantity = ied.get_balance(client, 'BUSD', True, float("{:0.0{}f}".format(binance_price["BID"][0], 4)) )
                order = client.create_margin_order(symbol='EURBUSD',
                                          side=SIDE_BUY,
                                          type=ORDER_TYPE_LIMIT,
                                          timeInForce=TIME_IN_FORCE_GTC,
                                          quantity = float('%.2f' % (quantity/2)),
                                          price = "{:0.0{}f}".format(binance_price["BID"][0] - al_price_ch, 4)
                                          )
                
                update_close_order_query(conn = conn, order_id = open_positions[3],
                                        imbalance = imbalance,
                                        new_close_price = binance_price["BID"][0] - al_price_ch,
                                        forex_price = forex_price,
                                        new_order_id = order['orderId'])
                
                text = "Close order has been updated. Closing price: "+ "% 1.4f " % close_price
                text += "->" + "% 1.4f " % (binance_price["BID"][0] - al_price_ch)
                bot.send_message(chatID, text)
                print(text)
        
    if open_positions[0] == "BUY EUR":
        
        # If we are not hitting the close_thresghold, cancel the order
        if (imbalance + al_imb_ch < - close_threshold):
            order = client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
            cancel_close_order_query(conn, order['orderId'])
            text = "Close order is canceled. Imbalance is still high."
            bot.send_message(chatID, text)
            print(text)
            
        # If it's not, check that we are not too far (within 'al_price_ch') away from market offers
        else:
            
            if binance_price["ASK"][0] + al_price_ch < close_price:
                client.cancel_margin_order(
                                    symbol='EURBUSD',
                                    orderId=str(open_positions[3]))
                
                quantity = ied.get_balance(client, 'EUR', False, None)
                order = client.create_margin_order(symbol='EURBUSD',
                                          side=SIDE_SELL,
                                          type=ORDER_TYPE_LIMIT,
                                          timeInForce=TIME_IN_FORCE_GTC,
                                          quantity = float('%.2f' % (quantity/2)),
                                          price = "{:0.0{}f}".format(binance_price["ASK"][0] + al_price_ch, 4)
                                          )
                
                update_close_order_query(conn = conn, order_id = open_positions[3],
                                        imbalance = imbalance,
                                        new_close_price = binance_price["ASK"][0] + al_price_ch,
                                        forex_price = forex_price,
                                        new_order_id = order['orderId'])
                
                text = "Close order has been updated. Closing price: "+ "% 1.4f " % close_price
                text += "->" + "% 1.4f " % (binance_price["ASK"][0] + al_price_ch)
                bot.send_message(chatID, text)
                print(text)
    return None

def update_close_order_query(conn, order_id, imbalance, new_close_price, forex_price, new_order_id):
    query = "UPDATE trade_history SET CLOSE_IMBALANCE_ACTUAL = " + str(imbalance)
    query += ", CLOSE_PRICE_ACTUAL = " + str(new_close_price)
    query += ", CLOSE_FOREX_ACTUAL = " + str(forex_price)
    query += ", ORDER_ID_CLOSE = "+ str(new_order_id)
    query += " where ORDER_ID_CLOSE = " + str(order_id)
    ibm_db.exec_immediate(conn, query)
    return None

def cancel_close_order_query(conn, order_id):
    query = "UPDATE trade_history SET STATUS = 'OPEN' where ORDER_ID_CLOSE = " + str(order_id)
    ibm_db.exec_immediate(conn, query)
    return None

def confirm_closed_order(order_id, chatID, bot, conn, timestamp, pos_type, close_price_actual, open_price_actual, close_imbalance_actual, close_forex_actual):
    if pos_type == "SELL EUR":
        profit = -(close_price_actual - open_price_actual) * 100 / open_price_actual
    if pos_type == "BUY EUR":
        profit = (close_price_actual - open_price_actual) * 100 / open_price_actual
        
    pconn = ibm_db_dbi.Connection(conn)
    query = "update TRADE_HISTORY SET STATUS = 'CLOSED', "
    query += "CLOSE_PRICE_ACTUAL = " + str(close_price_actual)
    query += ", CLOSE_IMBALANCE_ACTUAL = " + str(close_imbalance_actual)
    query += ", CLOSE_FOREX_ACTUAL = " + str(close_forex_actual)
    query += ", PROFIT_ACTUAL = " + str(profit)
    query += ", TIME_STAMP_CLOSE_ACTUAL = " + str(timestamp)
    query += " where ORDER_ID_CLOSE = " + str(order_id)
    ibm_db.exec_immediate(conn, query)
    text = "Close order is filled. Profit: " + "% 1.3f" % profit + "%."
    print(text)
    bot.send_message(chatID, text)
    query_2 = "update trade_history set DURATION_IN_MINS_ACTUAL = (TIME_STAMP_CLOSE_ACTUAL - TIME_STAMP_OPEN_ACTUAL) / 60"
    ibm_db.exec_immediate(conn, query_2)
    return None