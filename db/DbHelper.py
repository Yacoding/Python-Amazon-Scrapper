__author__ = 'Rabbi-Tuly'
import sqlite3 as lite


class DbHelper:
    dbName = None

    def __init__(self, dbName):
        self.dbName = dbName

    def createTable(self, tableName):
        connection = None
        try:
            connection = lite.connect(self.dbName)
            cursor = connection.cursor()
            cursor.execute('CREATE TABLE IF NOT EXISTS %s(id INT PRIMARY KEY, pid TEXT);' % tableName)
            cursor.close()
        except Exception, x:
            print 'Db error: ', x
            if connection:
                connection.rollback()
        finally:
            if connection:
                connection.commit()
                connection.close()

    def saveProduct(self, productName, tableName):
        connection = None
        try:
            connection = lite.connect(self.dbName)
            cursor = connection.cursor()
            cursor.execute('INSERT INTO %s(pid) VALUES(?)' % tableName, [productName])
            cursor.close()
        except Exception, x:
            print 'Db error insert: ', x
            if connection:
                connection.rollback()
        finally:
            if connection:
                connection.commit()
                connection.close()

    def searchProduct(self, productName, tableName):
        connection = None
        try:
            connection = lite.connect(self.dbName)
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM %s WHERE pid=?" % tableName, [productName])
            isExists = True if cursor.fetchone()[0] > 0 else False
            cursor.close()
            return isExists
        except Exception, x:
            print 'Db error select: ', x
            if connection:
                connection.rollback()
        finally:
            if connection:
                connection.commit()
                connection.close()
        return False

    def getTotalProduct(self, tableName):
        connection = None
        try:
            connection = lite.connect(self.dbName)
            cursor = connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM %s' % tableName)
            count = cursor.fetchone()[0]
            print count
            cursor.close()
            return count
        except Exception, x:
            print 'Db error select: ', x
            if connection:
                connection.rollback()
        finally:
            if connection:
                connection.commit()
                connection.close()
        return 0