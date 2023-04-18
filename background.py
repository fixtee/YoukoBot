import threading
import datetime
import pytz
from threading import Thread
from flask import Flask, request
from waitress import serve
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask('')
app.wsgi_app = ProxyFix(app.wsgi_app)

@app.after_request
def log_response(response):
  now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
  print(f"\033[38;2;46;149;211m{now.strftime('%d.%m.%Y %H:%M:%S')} | {request.remote_addr} | {request.method} | {response.status_code}\033[0m")
  return response

@app.route('/')
def home():
  return "Bot is alive"

def run():
  app.run(host='0.0.0.0', port=80)
    
def keep_alive():

  Dev = False

  if not Dev:
    now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
    serve_thread = threading.Thread(target=serve, args=(app,), kwargs={'host': '0.0.0.0', 'port': 80})
    serve_thread.start()
    print(f"\033[38;2;0;166;125m{serve_thread.name} ID:{serve_thread.ident} is started at {now.strftime('%d.%m.%Y %H:%M:%S')}\033[0m")
  else:
    t = Thread(target=run)
    t.start()