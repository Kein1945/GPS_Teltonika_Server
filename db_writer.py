import _mssql

class remoteDB():
    def __init__(self, config):
        self.errors = []
        self.host = config.get('remote_db', 'host')
        self.user = config.get('remote_db', 'user')
        self.password = config.get('remote_db', 'password')
        self.database = config.get('remote_db', 'database')

        self.connectGPS()

    def save(self, sensorsBlocksData):
        '''
        Save list of sensorsBlock data. Return is all blocks saved
        '''
        count = 0
        for block in sensorsBlocksData:
            if self.saveBlock(block):
                count+=1
        return len(sensorsBlocksData) == count

    def saveBlock(self, sensorData):
        IMEI = sensorData['imei']
        Date = sensorData['date']
        if self.TerminalDataInsert( IMEI, Date, sensorData['Lon'], sensorData['Lat'], sensorData['GpsSpeed'], sensorData['GpsCourse'] ):
            return self.SensorDataInsert(IMEI, Date, sensorData['sensorData'])
        else:
            return False

    def TerminalDataInsert(self, IMEI, Date, Lon, Lat, GpsSpeed, GpsCourse):
        TerminalInsertQuery = 'TerminalDataInsert @IMEI=%s, @UnixTime=%d, @lon=%s, @lat=%s, @Speed=%d, @Course=%d' % (
        IMEI, Date, Lon, Lat, GpsSpeed, GpsCourse )
        try:
            self.getGPS().execute_query( TerminalInsertQuery )
        except _mssql.MssqlDatabaseException, e:
            self.errors.append('MsSql error terminal data insert: %s' % TerminalInsertQuery)
            return False
        return True

    def SensorDataInsert(self, IMEI, Date, Data):
        insertQuery = '''insert into @Table values(%s,%d,%d,%d);'''
        query = ''
        for IOCode in Data.keys():
            query += insertQuery % ( IMEI, Date, IOCode, Data[IOCode] )
            pass
        SensorDataInsertQuery = '''SensorDataInsert @ListString='%s' ''' % query
        try:
            self.getGPS().execute_query(SensorDataInsertQuery )
        except _mssql.MssqlDatabaseException, e:
            self.errors.append('MsSql error terminal data insert %s' % SensorDataInsertQuery)
            return False
        return True

    def getGPS(self):
        if not self.GPSConnection.connected:
            self.connectGPS()
        return  self.GPSConnection
    def connectGPS(self):
        self.GPSConnection = _mssql.connect(server=self.host, user=self.user, password=self.password, database=self.database)
