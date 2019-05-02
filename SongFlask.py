import time, threading
import pymysql
from flask import Flask
from flask import render_template

import SongCrawl 


def PeriodicallyCall() :    
    crawl = SongCrawl.CrawlClass('pydb')
    crawl.ParseRss()            
    threading.Timer(10, PeriodicallyCall).start()    



if __name__ == '__main__':            
    PeriodicallyCall()       

    con = pymysql.connect(host='localhost', user='tester', password=None,
                    db='pydb', charset='utf8mb4')
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
   