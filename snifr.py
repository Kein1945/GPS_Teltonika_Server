import socket
import threading
import redis

from time import gmtime, strftime
import cPickle

import ConfigParser
from optparse import OptionParser

from gps import GPSTerminal

class ClientThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None, *args, **kwargs):
        threading.Thread.__init__(self)
        self.socket = kwargs['socket']
        self.config = kwargs['config']
        self.logTime = strftime("%d %b %H:%M:%S", gmtime())
        self.identifier = "None"
        r_host = self.config.get('redis', 'host')
        r_port = int(self.config.get('redis', 'port'))
        r_db = int(self.config.get('redis', 'db'))
        self.rcli = redis.Redis(host=r_host, port=r_port, db=r_db)
        self.channel = self.config.get('redis', 'channel')
    
    def log(self, msg):
        print "%s\t%s\t%s"%(self.logTime, self.identifier, msg)
        pass

    def run(self):
        client = self.socket
        if client:
            terminalClient = GPSTerminal(self.socket)
            self.identifier = terminalClient.getIp()
            terminalClient.startReadData()
            if terminalClient.isSuccess():
                self.saveData(terminalClient.getSensorData())
                terminalClient.sendOKClient()
                self.log('Client %s'%terminalClient.getImei())
            else:
                terminalClient.sendFalse()
                pass
            terminalClient.closeConnection()
        else: 
            self.log('Socket is null.')

    def saveData(self, sensorData):
        self.rcli.rpush(self.channel, cPickle.dumps(sensorData))

def get_config(config_file):

    config = ConfigParser.RawConfigParser()
    config.add_section('redis')
    config.set('redis', 'channel', 'GPSSensorsData')
    config.set('redis', 'host', 'localhost')
    config.set('redis', 'port', '6379')
    config.set('redis', 'db', '0')

    config.add_section('server')
    config.set('server', 'port', '9980')
    config.read(config_file)
    return config

if __name__ == "__main__":

    optParser = OptionParser()
    optParser.add_option("-c", "--config", dest="conf_file", help="Config file", default="gps.conf")
    (options, args) = optParser.parse_args()

    config = get_config(options.conf_file)

    print "Gps sensors server. %s"%strftime("%d %b %H:%M:%S", gmtime())
    print "Config: %s" % options.conf_file
    print "Sensor data db: %s:%s/%s" % (config.get('redis', 'host'), config.get('redis', 'port'), config.get('redis', 'channel'))
    print "Server started at port %d" % int(config.get('server', 'port'))

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('', int(config.get('server', 'port'))))
    server.listen(5)

    while True:
        ClientThread(socket=server.accept(), config = config).start()

