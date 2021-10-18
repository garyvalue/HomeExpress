# Development Journey

## Pevious working Preview
I have started this project for a month so most of the back end features are completed before I upload this file to Github. However, I would like to write this journey to record the front-end development using Telegram Bot. Before starting this journey, I want to list out all the front-end alternatives I have considered.
<br>
1.  HTML + CSS

    HTML and CSS are the most popular and flexible front-end tool for web development. I have completed a alpha version of web app, which is able to run smoothly without bugs. However, the biggest challenge of this alternative is 'HOW TO PUT IT TO INTERNET?'. I have tried Azure, Google and PythonAnyWhere, but all of them need the user to pay for their service with SQL server.
    
2.  Kivy

    At the very begining of development stage, I designed this application is an andriod app or ios app which is able to download in GooglePlay or AppStore... Kivy is a greate tool to develop mobile app in python. However, I met the challenge of accessing geolocation in user device, and packaging the app as apk version. Although there are several method to deal with this problem, those are time consuming, complicated and not fitting my major learning objective. 

## Why Telegram Bot?
Regarding the piror development experience, I clearify and listed out the following requirements of front-end app:
- user geolocation accessible
- SQL Server accessible
- remember user information
- work in python

Telegram is able to connect my local host SQL Server throught the chat bot which is ran by flask. Therefore, if the flask is able to receive the user location from the chatroom, we can run the code in our local server instead of cloud server.

Basically, I will follow the tutorial from https://zaoldyeck.medium.com/%E6%89%8B%E6%8A%8A%E6%89%8B%E6%95%99%E4%BD%A0%E6%80%8E%E9%BA%BC%E6%89%93%E9%80%A0-telegram-bot-a7b539c3402a to develop the basic ooperation of chatbot. Then, depending on the requirements, I will design a new user experience to let user use my app in one click button.
