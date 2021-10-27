

api_id = 8487685
api_hash = 'b0e81650a39f65fc038b8a182581d428'
# 2058616638:AAGOp7JqhzalJga69mP_7-vuOGvnJ9dOVZE

import logging

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    ConversationHandler,
    CallbackContext,
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# The API Key we received for our bot
API_KEY = "2058616638:AAGOp7JqhzalJga69mP_7-vuOGvnJ9dOVZE"
# Create an updater object with our API Key
updater = telegram.ext.Updater(API_KEY)
# Retrieve the dispatcher, which will be used to add handlers
dispatcher = updater.dispatcher
updater.start_polling()
updater.idle()


class registeration:
    def start(self, update: Update, context: CallbackContext) -> int:
        pass
    
    def private_policy(self):
        pass
        
    def location_confirmation(self):
        pass
    
    def completion(self):
        pass
    
class user_setting:
    def request(self) -> int:
        pass
    def confirmation(self) -> int:
        pass
    
def location(self)
# it will process all the command of /start than take an action 
