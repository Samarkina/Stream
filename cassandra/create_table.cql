DROP KEYSPACE IF EXISTS StreamingDB;
CREATE KEYSPACE StreamingDB
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE StreamingDB;

DROP TABLE IF EXISTS BotsStructured;
CREATE TABLE BotsStructured (
ip TEXT,
PRIMARY KEY (ip)
);

DROP TABLE IF EXISTS BotsDStream;
CREATE TABLE BotsDStream (
ip TEXT,
PRIMARY KEY (ip)
);

