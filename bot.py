import telebot
import ibm_db
import ibm_db_dbi
import pandas as pd
import datetime

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

token = open("bot_key.txt", "r").readline() # Read the Telegram Bot key
bot = telebot.TeleBot(token, parse_mode=None) # An instance of teh TeleBot class

@bot.message_handler(commands=['start'])
def send_welcome(message):
	bot.reply_to(message, "Hey, how are you? Let's earn some money! Sincerely Yours, Igor Che" + "\U0001F609")
    
@bot.message_handler(commands=['help'])
def send_help(message):
	bot.reply_to(message, "The bot compares the forex EUR/USD exchange rate with the rate of the similar EUR/BUSD pair on Binance. The difference in these rates opens up arbitrage opportunities.")

@bot.message_handler(commands=['prices'])
def send_prices(message):
    pconn = ibm_db_dbi.Connection(conn)
    query = "select * from crypto_db order by TIME_STAMP desc LIMIT 1"
    data = pd.read_sql(query, pconn)
    datetime_time = datetime.datetime.fromtimestamp(data["TIME_STAMP"][0])
    text = "\U0001F55C"+ "Last update: "+ str(datetime_time)
    text += "\n" + "\U0001F4B5" + "EUR/USD: " + "% 1.4f" % data["EUR_USD_REAL"][0]
    text +="\n"+ "\U0001F4B3" + "EUR/BUSD: " + "% 1.4f" % data["EUR_BUSD"][0]
    text +="\n"+ "\U0001F680" + "Imbalance: " + "% 1.2f" % data["DIFF_IN_PERC"][0]
    bot.reply_to(message, text)

bot.get_updates()
    
bot.polling()