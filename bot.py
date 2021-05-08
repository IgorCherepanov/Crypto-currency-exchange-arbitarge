import telebot
import ibm_db
import ibm_db_dbi
import pandas as pd
from datetime import datetime, timedelta
from datetime import timezone
import matplotlib.pyplot as plt
from io import BytesIO
import time

chatID = 361222436

# Converts time from UTC to CET 
def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=timezone.CET())

def last_trades_text(data, i):
    datetime_time = datetime.fromtimestamp(data["TIME_STAMP_OPEN"][i])
    
    if time.tzname[0] == "UTC":
        datetime_time = datetime_time + timedelta(hours=2)
    
    if data["PROFIT"][i] > 0:
        text = "\U0001F525"
    else:
        text = "\U0001F4A9"
    text += str(datetime_time.strftime("%b %d %H:%M")) + " / "
    text += "% 1.2f" % data["PROFIT"][i] + "% / "
    text += "% 1.0f" % data["DURATION_IN_MINS"][i] +" mins \n"
    return text

def accumulative_list(input_list):
    output_list = [input_list[0]/2]
    for i in range(1,len(input_list)):
        output_list.append( (100 + input_list[i]/2) * (100 + output_list[i-1]) / 100 - 100 )
    return output_list

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

@bot.message_handler(commands = ['start'])
def send_welcome(message):
    text = "Hey, how are you? Let's earn some money! Sincerely Yours, Igor Che" + "\U0001F609 \n"
    text += "Send /commands to see the list of currently avaliable commands"
    bot.reply_to(message, text)
    
@bot.message_handler(commands = ['commands'])
def send_commands(message):
    text = "The list of currenlty avaliable commands: \n"
    text += "\U0001F4CA /chart: the chart with the net PnL since the beginning \n"
    text += "\U0001F4B0 /last5: info about 5 last trades including profit and lose (PnL) \n"
    text += "\U0001F4C8 /prices: the current arbitrage status \n"
    text += "\U0001F4DC /commands: the list of avaliable commands \n"
    text += "\U00002753 /help: a couple of words about the bot \n"
    text += "\U0001F4EA /contact: write me or visit the page of the bot on GitHub \n"
    text += "\U000027A1 /start: the welcome page \n"
    bot.reply_to(message, text)
    
@bot.message_handler(commands = ['help'])
def send_help(message):
    bot.reply_to(message, "The bot compares the forex EUR/USD exchange rate with the rate of the similar EUR/BUSD pair on Binance. The difference in these rates opens up arbitrage opportunities.")

@bot.message_handler(commands = ['last5'])
def send_last5(message):
    pconn = ibm_db_dbi.Connection(conn)
    query = "select * from trade_history where STATUS = 'CLOSED' order by TIME_STAMP_CLOSE desc LIMIT 5"
    data = pd.read_sql(query, pconn)
    
    text = "Last 5 completed trades:\n"
    text += "Opened / PnL / Duration \n"
    for i in range(len(data)):
        text += last_trades_text(data, i)
    bot.reply_to(message, text)
    
@bot.message_handler(commands = ['prices'])
def send_prices(message):
    pconn = ibm_db_dbi.Connection(conn)
    query = "select * from crypto_db order by TIME_STAMP desc LIMIT 1"
    data = pd.read_sql(query, pconn)
    datetime_time = datetime.fromtimestamp(data["TIME_STAMP"][0])
    
    if time.tzname[0] == "UTC":
        datetime_time = datetime_time + timedelta(hours=2)
    
    text = "\U0001F55C"+ "Last update: " + str(datetime_time.strftime("%b %d %H:%M"))
    text += "\n" + "\U0001F4B5" + "EUR/USD: " + "% 1.4f" % data["EUR_USD_USED"][0]
    text +="\n"+ "\U0001F4B3" + "EUR/BUSD: " + "% 1.4f" % data["EUR_BUSD"][0]
    text +="\n"+ "\U0001F680" + "Imbalance: " + "% 1.2f" % data["DIFF_IN_PERC"][0]
    bot.reply_to(message, text)

@bot.message_handler(commands = ['chart'])
def send_chart(message):
    
    bio = BytesIO() #buffer for storing a figure w/o saving it in the disk
    
    pconn = ibm_db_dbi.Connection(conn)
    query = "select * from trade_history WHERE STATUS = 'CLOSED' order by TIME_STAMP_OPEN"
    data = pd.read_sql(query, pconn)

    datetime_start = datetime.fromtimestamp(min(data["TIME_STAMP_OPEN"]))
    datetime_now = datetime.now()
    delta = datetime_now - datetime_start

    net_pnl = accumulative_list(data["PROFIT"])[-1]

    est_pnl_pa = net_pnl * 31622400 / delta.total_seconds()

    plt.plot(accumulative_list(data["PROFIT"]), "go-", linewidth = 3.0, ms = 10)
    plt.ylabel('Net PnL (%)', fontsize = 20)
    plt.xlabel('Trades', fontsize = 20)
    plt.xticks(fontsize = 20)
    plt.yticks(fontsize = 20)
    plt.annotate("Start: " + str(datetime_start.strftime("%b %d %Y")) +"\n"
             "Net PnL:" + "% 1.2f" % net_pnl +"%" + "\n"
             "Est. Annual PnL:" + "% 1.2f" % est_pnl_pa +"%" + "\n"
             , xy=(0.02, 0.7), xycoords='axes fraction', fontsize = 14)

    plt.savefig(bio, bbox_inches = 'tight')
    
    plt.clf()
    
    bio.seek(0)
    
    bot.send_photo(chatID, bio)
    
@bot.message_handler(commands = ['contact'])
def send_contact(message):
    text = "\U0001F4F1 Telegram (preferred): @igor_c \n"
    text += "\U0001F4E7 E-mail: rain.princess@gmail.com \n"
    text += "\U0001F310 GitHub page of the project: https://github.com/IgorCherepanov/Crypto-currency-exchange-arbitarge.git" 
    bot.reply_to(message, text)

bot.get_updates()
    
bot.polling()