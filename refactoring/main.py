

api_id = 8487685
api_hash = 'b0e81650a39f65fc038b8a182581d428'
# 2058616638:AAGOp7JqhzalJga69mP_7-vuOGvnJ9dOVZE

import telegram
import telegram.ext
import re
from random import randint
import logging
from telegram.ext import MessageHandler, Filters
from telegram.ext import CommandHandler
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

def start(bot, update):
    """
    in the start command, will perform the following tasks
    #1 : query the user_id to identify the sender
        - if exists in database, 
            @ seek the next command
                ! /setting
                ! /location
                
        - if not exists in database,
            @ welcome letter
            @ introduction
            @ private policy
            @ agreement
            - if no, 
                reply text, and reminder
            - if yes, 
                reply the inline button 
                change the status of user
                wait to receive the location
    """
    print(update)

def location(bot, update):
    """
    in the location, will perform the following tasks
    #1 : check the user_id status
        if in setting mode,
            ask for confirming the changes
        if in execution mode,
            matching
    """
    pass 
    
dispatcher.add_handler(CommandHandler('start', start))

# it will process all the command of /start than take an action 
