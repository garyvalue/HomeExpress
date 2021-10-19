import telepot
import time
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from pprint import pprint
#import back_end

get_location_keyboard = [
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
                keyboard = [get_location_keyboard]
                )
            )

def debug(msg):
    type, _, chat_id = telepot.glance(msg)
    bot.deleteMessage((chat_id, msg['message_id']))
    
    message = \
"""
    <b><i>route</i></b> - COM
    <u>time</u><pre>
Orig: orig_name_tc
Dest: dest_name_tc
 ETA: eta[1]
</pre>
"""

    markup = InlineKeyboardMarkup()

    bot.sendMessage(
        chat_id = chat_id, 
        text = message, 
        parse_mode='HTML'
        )
pass


def upload_file(chat_id):
    with open("test.html", "r", encoding='utf-8') as file:
        bot.sendDocument(chat_id, document=file, caption='this_is_for_tg_name.html')

bot = telepot.Bot('2058616638:AAGOp7JqhzalJga69mP_7-vuOGvnJ9dOVZE')

# this messageLoop is able to 
MessageLoop(bot, debug).run_as_thread()
# telepot.loop.Webhook(bot, test).run_as_thread()

while True:
    time.sleep(30)
