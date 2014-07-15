Aesop
=====
A keen observer of changes that can also relay change events reliably to interested parties. Provides useful infrastructure for 
building Eventually Consistent data sources and systems.

## Releases
| Release | Date | Description |
|:------------|:----------------|:------------|
| Version 1.0.7-SNAPSHOT    | July 2014      |    First GA release

## Changelog
Changelog can be viewed in CHANGELOG.md file (https://github.com/Flipkart/aesop/blob/master/CHANGELOG.md)

## Why Aesop
Data movement from source to consumer is a fairly common requirement in distributed systems. An example is inventory updates on a warehousing system
reflecting on product pages of an eCommerce portal. Another is price updates on a hot selling item across multiple sellers in an on-line marketplace.
Both these examples are instances that require the data updates to propagate with low latency and reliably. Few broad options exist:
* Application publishes changes asynchronously to a queue before/after writing data to persistent store - this is usually fire-and-forget in most implementations.
* Variants include those with retries, back-off and queue sidelining. Reliability can be enhanced using local transactions (not distributed) and message relaying.
* Batch extraction(snapshots) and load every few hours - affects data freshness in the consumer systems.
* Database supported replication - very common approach when source and consumer systems use the same data store technology(e.g. MySQL master-slave replication) 
and share the same data model / schema.

Aesop provides reliable, low-latency data change propagation for source and consumer systems that optionally use different data stores. It also supports
snapshot based change detection. For consumers, it provides a unified interface for change data consumption that is independent of how the data change is
sourced.

## Aesop Consoles
![Relays](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Relays.png)
![Relay Metrics](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Metrics.png)

## Getting Started
The [Getting Started](https://github.com/Flipkart/aesop/wiki/Getting-started-and-Examples) page has "5 minute" examples to help you start using Aesop.

## Documentation and Examples
Aesop project modules that start with "sample" - for e.g. sample-memory-relay, sample-client are example implementations. Documentation is 
continuously being added to the Wiki page of Aesop (https://github.com/Flipkart/aesop/wiki)

## Getting help
For discussion, help regarding usage, or receiving important announcements, subscribe to the Aesop users mailing list: http://groups.google.com/group/aesop-users

## License
Aesop is licensed under : The Apache Software License, Version 2.0. Here is a copy of the license (http://www.apache.org/licenses/LICENSE-2.0.txt)

## Core contributors
* Chandan Bansal ([@chandanbansal](https://github.com/chandanbansal))
* Jagadeesh Huliyar ([@jagadeesh-huliyar](https://github.com/jagadeesh-huliyar))
* Kartik B Ukhalkar ([@kartikssj](https://github.com/kartikssj))
* Regunath B ([@regunathb](http://twitter.com/RegunathB))
* Shoury ([@Shoury](https://github.com/Shoury))
* Yogesh Dahiya ([@yogeshdfk](https://github.com/yogeshdfk))
