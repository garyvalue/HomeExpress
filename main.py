from flask import Flask, render_template, request, jsonify, redirect, url_for
import back_end
# from turbo_flask import Turbo
# import threading
import time
# import geopy

app = Flask(__name__)


@app.route('/')
def redir():
    return render_template('select_location.html')


@app.route('/home')
def new():
    return render_template('select_location.html')


@app.route('/dashboard', methods=['POST'])
def access_data():
    if 'home' in request.form:
        home = ''
        user = ''
        home = [float(i) for i in request.form['home'].split(',')]
        user = [float(i) for i in request.form['location'].split(',')]
        print(home)
        print(user)
        server = back_end.match(home=home, user=user)
        data = server.get_match_result()
        return render_template('select_location.html', load=data)
    else:
        return redirect(url_for('redir'))


app.run(debug=True)

# <link rel="stylesheet" type = 'text/css' href="{{ url_for('static', filename='style.css') }}">
