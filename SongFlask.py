import time, threading
import pyodbc
from flask import Flask
from flask import render_template

import SongCrawl 

server = 'inshalla.database.windows.net'
database = 'pydb'
username = 'inshalla'
password = '123qwe!@#'
driver= '{ODBC Driver 17 for SQL Server}'

odbcConnectInfo = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password


def PeriodicallyCall() :    
    crawl = SongCrawl.CrawlClass(odbcConnectInfo)
    crawl.ParseRss()            
    threading.Timer(10, PeriodicallyCall).start()    



if __name__ == '__main__':            
    PeriodicallyCall()       

    con = pyodbc.connect(odbcConnectInfo)
    cur = con.cursor()

    app = Flask(__name__)
    @app.route("/")
    def home():
        return "Hello, Flask!"

    @app.route('/print_items')
    def print_items():
        cur.execute("SELECT * FROM RssTable;")
        rows = cur.fetchall()
        return render_template('print_items.html', items=rows)

    app.run(debug=True)  
   