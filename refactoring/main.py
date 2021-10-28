

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



class registeration:
    def start(update: Update, context: CallbackContext) -> int:
        """
        recieve the start command,
            check the existing user_id,
                if yes, 
                    end this conversation
                    start to ask him need to change the location setting? 
                if no,
                    continue -> private_policy
                    with the keyboard button or inline keyboard
                    to send the location or cancel registration
        """
        print(update.message.from_user)
        pass
    
    def distance_confirmation():
        pass
    
    def private_policy():
        # print the private policy
        pass
        
    def location_confirmation():
        pass
    
    def completion():
        pass
    
    def cancel(update: Update, context: CallbackContext) -> int:
        """cancels and ends the conversation"""
        user = update.message.from_user
        logger.info(f'user {user.first_name} canceleed the conversation.')
        update.message.reply_text(
            'the registration is cancelled', reply_markup = ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    
    def conversation_flow():
        """
        this is conversation handler,
            timeout = 3mins
            fallback by 'cancel'
        """
        return Conversationhandler(
            entry_points=[CommandHandler('start', registration.start)],
            states={
                location: [MessageHandler(Filters.location, registration.location_confirmation)],
                confirm: [CommandHandler('accept', registration.completion)]
            },
            fallbacks=[CommandHandler('cancel', registration.cancel),
            allow_reentry = False
            )
                       
    
class user_setting:
    def start() -> int:
        pass
                       
    def confirmation() -> int:
        pass
                       
    def conversation_flow():
        """
        this is conversation handler,
            timeout = 1min
            fallback by 'cancel'
        """
        return Conversationhandler(
            endtry_points=[CommandHandler('setting', user_setting.start)],
            states={
                confirmation: [MessageHandler(Filter.location, user_setting.confirmation)]
            },
            fallbacks=[CommandHandler('cancel', usersetting.cancel),
            allow_reentry = False
            )
                       
def match():
    pass
    
def locaiton():
    pass

def route_genertion():
    pass

dispatcher.add_handler(registration.conversation_flow)
updater.start_polling()
updater.idle()
