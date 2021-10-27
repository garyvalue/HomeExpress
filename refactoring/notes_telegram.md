# python-telegram-bot learning notes

To initiate the python bot, I need to prepare the "token"

    updater = Updater("TOKEN", workers= 2, arbitrary_callback_data = True)
    # arbitrary_callback_data = 1
        this is for InLineKeyBoardButton, can allow the callback data be the arbitrary objects
        the integer value is controling the maximum number of cached objects
    
next step is create the dispatcher for processing new updates <br>
*(should pay attention, new message is part of updates. telegram bot has many updates in there.)*

    dispatcher = updater.dispatcher
    
Then dispatcher allows to insert kinds of handler to process different type updates
    1.
