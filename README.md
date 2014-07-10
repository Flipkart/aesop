aesop
=====

A keen observer of changes that can also relay change events reliably to interested parties. Provides useful infrastructure for 
building Eventually Consistent data sources and systems.

## Releases

| Release | Date | Description |
|:------------|:----------------|:------------|
| Version 1.0.7-SNAPSHOT    | July 2014      |    First GA release

## Changelog

Changelog can be viewed in CHANGELOG.md file (https://github.com/Flipkart/aesop/blob/master/CHANGELOG.md)

## Design

aesop uses the log mining approach of detecting data changes as described by the [LinkedIn Databus](https://github.com/linkedin/databus) 
project. It also uses the infrastructure components of Databus, mostly for serving change events. The concept of Event Producer,
Relay, Event Buffer, Bootstrap, Event Consumer and System Change Number(SCN) are quite appealing and used mostly as-is in aesop.

aesop extends support for change detection on HBase data store by implementing an Event Producer based off 
[NGDATA hbase-sep](https://github.com/NGDATA/hbase-indexer/tree/master/hbase-sep). For MySQL, aesop builds on the producer implementation
available in Databus.

The log mining producer implementations leverage master-slave replication support available in databases. Such producers may be called "Push" producers 
where changes are pushed from the master to slave (the aesop Event Producer).

The log mining approach has limitations if data is distributed across database tables (when updates are not part of a single transaction)
where-in it is hard to correlate multiple updates to a single change. It also has limitations when data is distributed across types of 
data stores - for e.g. between an RDBMS and a Document database. aesop addresses this problem by implementing a "Pull" producer that
uses an application-provided "Iterator" API to periodically scan the entire datastore and detect changes between scan cycles. This
implementation is based off [Netflix Zeno](https://github.com/Netflix/zeno).

Change propagation employing both "Push" and "Pull" producers:

```
Pull Producer                        Streaming Client 1       Slow/Catchup client1
(Zeno based)    \                   /                        /
                 \_____ Relay _____/___ Bootstrap __________/
                 /    (Databus)    \    (Databus)           \
                /                   \                        \
Push Producer                        Streaming Client 2       Slow/Catchup client 2  
(e.g. HBase WAL edits listener,
 e.g. MySQL Replication listener)
```

## Aesop Consoles
![Relays](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Relays.png)

![Relay Metrics](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Metrics.png)

## Documentation and Examples
Aesop project modules that start with "sample" - for e.g. sample-memory-relay, sample-client are example implementations.

## Getting help
For discussion, help regarding usage, or receiving important announcements, subscribe to the Aesop users mailing list: http://groups.google.com/group/aesop-users

## License
Aesop is licensed under : The Apache Software License, Version 2.0. Here is a copy of the license (http://www.apache.org/licenses/LICENSE-2.0.txt)

## Core contributors
* Chandan Bansal ([@chandanbansal](http://twitter.com/chandanbansal))
* Jagadeesh Huliyar ([@jagadeesh-huliyar](https://github.com/jagadeesh-huliyar))
* Kartik B Ukhalkar ([@kartikssj](https://github.com/kartikssj))
* Regunath B ([@regunathb](http://twitter.com/RegunathB))
* Yogesh Dahiya ([@yogeshdfk](http://twitter.com/yogeshdfk))
