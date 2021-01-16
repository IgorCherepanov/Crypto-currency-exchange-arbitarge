import telebot
f3 = open("bot_key.txt", "r") # Read the Telegram Bot key
api_bot = f3.readline()
bot = telebot.TeleBot(api_bot, parse_mode=None) # An instance of teh TeleBot class

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
	bot.reply_to(message, "Hey, how are you? Let's earn some money! Sincerely Yours, Igor Che")
    
bot.get_updates()
    
bot.polling()