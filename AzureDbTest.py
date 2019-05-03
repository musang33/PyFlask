import pyodbc
server = 'inshalla.database.windows.net'
database = 'pydb'
username = 'inshalla'
password = '123qwe!@#'
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
#cursor.execute("CREATE TABLE LatestInfoTbl(FileName nvarchar(30) PRIMARY KEY, Date nvarchar(20));")
#cursor.execute("INSERT INTO LatestInfoTbl (FileName, Date) VALUES (?, ?);", 'esdfabc','twf123')
cursor.commit()

cursor.execute("SELECT Date FROM LatestInfoTbl WHERE FileName=?;", 'esdfabc')

row = cursor.fetchone()
while row:
    print (str(row[0]))
    row = cursor.fetchone()

