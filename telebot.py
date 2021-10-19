import telepot
import time
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from pprint import pprint
import back_end

keyboard = [
        KeyboardButton(
            text='click to send locaiton',
            request_location=True
            )
        ]


def handle(msg):
    # | this function will handle the message which is sent from the user
    type, _, chat_id = telepot.glance(msg)
    bot.deleteMessage((chat_id, msg['message_id']))
    
    if type == 'location':
        geo = (
            msg['location']['latitude'], 
            msg['location']['longitude']
        )
        
        m = back_end.match(user=geo)
        data = m.get_match_result()
        
        if data:
            output = ''
            for i in range(len(data['route'])):
                for columns in ['route', 'orig_name_tc', 'dest_name_tc', '1']:
                    output += data[columns][i] + ' '
                output += '\n'
            bot.sendMessage(chat_id, output)
            
        else:
            
            bot.sendMessage(chat_id, '冇車去希慎')
            
    else:
        
        bot.sendMessage(
            chat_id, 
            text ='showing button', 
            reply_markup = ReplyKeyboardMarkup(
                keyboard = [keyboard]
                )
            )

        

bot = telepot.Bot('2058616638:AAGOp7JqhzalJga69mP_7-vuOGvnJ9dOVZE')

# this messageLoop is able to 
MessageLoop(bot, handle).run_as_thread()
# telepot.loop.Webhook(bot, test).run_as_thread()

while True:
    time.sleep(30)
