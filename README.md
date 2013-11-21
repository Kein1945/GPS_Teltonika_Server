GPS_Teltonika_Server
====================

Server for GPS trackers Teltonika FM4200

Required for proxify data from gps terminals to mssql(or other) database.
Actually this is a special case of usage, but you can modify it at your convenience.

There is two parts.
Snifr.py - script that receive connections from trackers, parse data, and write it to redis-db
db_daemon.py - script that waiting new data from redis, and send it to database.
