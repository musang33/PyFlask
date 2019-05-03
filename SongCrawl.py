#https://codelabs.developers.google.com/codelabs/build-your-first-android-app-kotlin/#7

import requests
from bs4 import BeautifulSoup
import pyodbc
import sys

class CrawlClass:
        # Sql query
        sqlQuery = { 
                'existMaster' : "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?;",
                'createLatestInfoTbl' : "CREATE TABLE LatestInfoTbl(FileName nvarchar(100) PRIMARY KEY, Date nvarchar(100));",
                'updateLatestInfo' : "UPDATE LatestInfoTbl SET Date = ? WHERE FileName = ?;",
                'insertLatestInfo' :  "INSERT INTO LatestInfoTbl (FileName, Date) VALUES (?,?);",
                'selectDateLatestInfo' :  "SELECT Date FROM LatestInfoTbl WHERE FileName=?;",
                'createRssTbl'  : "CREATE TABLE RssTable(No INT PRIMARY KEY, Title nvarchar(MAX), Link nvarchar(MAX), Category nvarchar(MAX), Author nvarchar(MAX), PubDate nvarchar(100), Article nvarchar(MAX));",
                'selectTop1RssTbl'  : "SELECT Top 1 No FROM RssTable ORDER BY No DESC;"
                }

         # DB Setting. './test.db'
        def __init__( self, odbcConnectInfo ):               
                self.con = pyodbc.connect(odbcConnectInfo)
                self.cur = self.con.cursor()

        # Create LatestInfoTbl if not exist
        def __CreateLatestInfoTblIfNotExist(self):             
                self.cur.execute( self.sqlQuery['existMaster'], 'LatestInfoTbl')
                if self.cur.fetchone()[0] == 0:
                        self.cur.execute(self.sqlQuery['createLatestInfoTbl'])
                        self.con.commit()

        def __CreateRssTblIfNotExist(self):             
                self.cur.execute( self.sqlQuery['existMaster'], 'RssTable')
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
                                        "INSERT INTO RssTable VALUES(?, ?, ?, ?, ?, ?, ?);", (
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

                self.cur.execute(self.sqlQuery['selectDateLatestInfo'], 'rss_30000001.xml')
                latestDate = self.cur.fetchone()                                
                if latestDate == lastBuildData.string :
                        return
                
                self.cur.execute(self.sqlQuery['selectDateLatestInfo'], 'rss_30000001.xml')
                entry = self.cur.fetchone()

                if entry is None:                
                        self.cur.execute(self.sqlQuery['insertLatestInfo'], 'rss_30000001.xml', lastBuildData.string)
                        self.con.commit()
                        print ('Insert %s' % 'rss_30000001')        
                else :
                        self.cur.execute(self.sqlQuery['updateLatestInfo'], lastBuildData.string, 'rss_30000001.xml')
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
