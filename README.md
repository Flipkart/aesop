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

## Why aesop
Data movement from source to consumer is a fairly common requirement in distributed systems. An example is inventory updates on a warehousing system
reflecting on product pages of an eCommerce portal. Another is price updates on a hot selling item across multiple sellers in an on-line marketplace.
Both these examples are instances that require the data updates to propagate with low latency and reliably. Few broad options exist:
* Application publishes changes asynchronously to a queue before/after writing data to persistent store - this is usually fire-and-forget in most implementations.
* Variants include those with retries, back-off and queue sidelining. Reliability can be enhanced using local transactions (not distributed) and message relaying.
* Batch extraction(snapshots) and load every few hours - affects data freshness in the consumer systems.
* Database supported replication - very common approach when source and consumer systems use the same data store technology(e.g. MySQL master-slave replication) 
and share the same data model / schema.

aesop provides reliable, low-latency data change propagation for source and consumer systems that optionally use different data stores. It also supports
snapshot based change detection. For consumers, it provides a unified interface for change data consumption that is independent of how the data change is
sourced.

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

## aesop Consoles
![Relays](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Relays.png)

![Relay Metrics](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Metrics.png)

## Documentation and Examples
aesop project modules that start with "sample" - for e.g. sample-memory-relay, sample-client are example implementations.

## Getting help
For discussion, help regarding usage, or receiving important announcements, subscribe to the aesop users mailing list: http://groups.google.com/group/aesop-users

## License
aesop is licensed under : The Apache Software License, Version 2.0. Here is a copy of the license (http://www.apache.org/licenses/LICENSE-2.0.txt)

## Core contributors
* Chandan Bansal ([@chandanbansal](https://github.com/chandanbansal))
* Jagadeesh Huliyar ([@jagadeesh-huliyar](https://github.com/jagadeesh-huliyar))
* Kartik B Ukhalkar ([@kartikssj](https://github.com/kartikssj))
* Regunath B ([@regunathb](http://twitter.com/RegunathB))
* Shoury ([@Shoury](https://github.com/Shoury))
* Yogesh Dahiya ([@yogeshdfk](https://github.com/yogeshdfk))
