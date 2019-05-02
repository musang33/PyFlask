#https://codelabs.developers.google.com/codelabs/build-your-first-android-app-kotlin/#7

import requests
from bs4 import BeautifulSoup
import pymysql
import sys

class CrawlClass:
        # Sql query
        sqlQuery = { 
                'existMaster' : "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{0}';",
                'createLatestInfoTbl' : "CREATE TABLE LatestInfoTbl(FileName text, Date text);",
                'updateLatestInfo' : "UPDATE LatestInfoTbl SET Date = '{0}' WHERE FileName = '{1}';",
                'insertLatestInfo' :  "INSERT INTO LatestInfoTbl (FileName, Date) VALUES ('{0}','{1}');",
                'selectDateLatestInfo' :  "SELECT Date FROM LatestInfoTbl WHERE FileName='{0}';",
                'createRssTbl'  : "CREATE TABLE RssTable(No INT PRIMARY KEY, Title text, Link text, Category text, Author text, PubDate text, Article text);",
                'selectTop1RssTbl'  : "SELECT No FROM pydb.RssTable ORDER BY No DESC limit 1;"
                }

         # DB Setting. './test.db'
        def __init__( self, mysqlDbPath ):               
                self.con = pymysql.connect(host='localhost', user='tester', password=None,
                       db=mysqlDbPath, charset='utf8mb4')
                self.cur = self.con.cursor()

        # Create LatestInfoTbl if not exist
        def __CreateLatestInfoTblIfNotExist(self):             
                self.cur.execute( self.sqlQuery['existMaster'].format('LatestInfoTbl',))
                if self.cur.fetchone()[0] == 0:
                        self.cur.execute(self.sqlQuery['createLatestInfoTbl'])
                        self.con.commit()

        def __CreateRssTblIfNotExist(self):             
                self.cur.execute( self.sqlQuery['existMaster'].format('RssTable',))
                if self.cur.fetchone()[0] == 0:
                        self.cur.execute(self.sqlQuery['createRssTbl'])
                        self.con.commit()

        def __InsertRss(self, bsObject):
                dictRss = {}
                for item in bsObject.findAll('item'):        
                        dictRss[item.no.string] = {
                                item.title.name : item.title.string, 
                                item.link.name : item.link.string,
                                item.category.name : item.category.string,
                                item.author.name : item.author.string,
                                item.pubDate.name : item.pubDate.string,
                                item.description.name : item.description.string }

                self.cur.execute(self.sqlQuery['selectTop1RssTbl'])
                top1Entry = self.cur.fetchone()
                top1Value = 0
                if top1Entry is not None:
                        top1Value = int(top1Entry[0])

                for dictRssKey in dictRss:
                        if int(dictRssKey) > top1Value :
                                self.cur.execute(
                                        'INSERT INTO RssTable VALUES(%s, %s, %s, %s, %s, %s, %s);', (
                                                int(dictRssKey), 
                                                dictRss[dictRssKey]['title'], 
                                                dictRss[dictRssKey]['link'], 
                                                dictRss[dictRssKey]['category'], 
                                                dictRss[dictRssKey]['author'], 
                                                dictRss[dictRssKey]['pubDate'], 
                                                dictRss[dictRssKey]['description']
                                                )
                                        )

                self.con.commit()        

        def ParseRss(self):           
                self.__CreateLatestInfoTblIfNotExist()

                # Request RSS
                html = requests.get("http://file.mk.co.kr/news/rss/rss_30000001.xml")
                bsObject = BeautifulSoup(html.content, 'xml') 
                lastBuildData = bsObject.find('lastBuildDate')

                self.cur.execute(self.sqlQuery['selectDateLatestInfo'].format('rss_30000001.xml',))
                latestDate = self.cur.fetchone()                                
                if latestDate == lastBuildData.string :
                        return
                
                self.cur.execute(self.sqlQuery['selectDateLatestInfo'].format('rss_30000001.xml',))
                entry = self.cur.fetchone()

                if entry is None:                
                        self.cur.execute(self.sqlQuery['insertLatestInfo'].format('rss_30000001.xml', lastBuildData.string))
                        self.con.commit()
                        print ('Insert %s' % 'rss_30000001')        
                else :
                        self.cur.execute(self.sqlQuery['updateLatestInfo'].format(lastBuildData.string, 'rss_30000001.xml'))
                        self.con.commit()
                        print ('Update %s' % lastBuildData.string)                        

                #print( lastBuildData.string );

                self.__CreateRssTblIfNotExist()
                self.__InsertRss(bsObject)
                
        def ShowRssTable(self):
                self.cur.execute("SELECT * FROM RssTable;")
                rows = self.cur.fetchall()
                for row in rows:
                        print(row)
                        print("\n")
