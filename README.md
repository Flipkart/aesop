Aesop
=====
A keen observer of changes that can also relay change events reliably to interested parties. Provides useful infrastructure for 
building Eventually Consistent data sources and systems.
Growing list of change data source and destinations:

| Sources | Destinations | Notes |
|:------------|:----------------|:------------|
| MySQL            | MySQL, HBase, Elastic Search, Kafka       |   [MySQL source ](https://github.com/Flipkart/aesop/wiki/MySQL-change-event-producer)
| HBase            | MySQL, HBase, Elastic Search, Kafka       |   [HBase source] (https://github.com/Flipkart/aesop/wiki/HBase-change-event-producer) 
| Any Data source  | Any destination       |   [Pull Producer] (https://github.com/Flipkart/aesop/wiki/Pull-change-event-producer) 

## Releases
| Release | Date | Description |
|:------------|:----------------|:------------|
| Version 1.2.1-SNAPSHOT    | Aug 2015       |    Feature enhancements and bug fix release
| Version 1.2.0             | Jul 2015       |    Bug fix release
| Version 1.1.0             | Apr 2015       |    Feature enhancements and bug fix release
| Version 1.1.0-SNAPSHOT    | Feb 2015       |    Second GA release

## Changelog
Changelog can be viewed in CHANGELOG.md file (https://github.com/Flipkart/aesop/blob/master/CHANGELOG.md)

## Why Aesop
Data change propagation from source to consumers is a fairly common requirement in systems that automate business processes. 
An example is inventory updates on a warehousing system reflecting on product pages of an eCommerce portal. 
This example is an instance that requires the data updates to propagate with low latency and reliably. Few broad options exist:
* Application publishes changes asynchronously to a queue before/after writing data to persistent store - this is usually fire-and-forget in most implementations
and therefore unreliable.
* Variants include those with retries, back-off and queue sidelining. Reliability can be enhanced using local transactions (not distributed) and message relaying.
* Batch extraction(snapshots) and load every few hours - affects data freshness in the consumer systems.
* Database supported replication - very common approach when source and consumer systems use the same data store technology(e.g. MySQL master-slave replication) 
and share the same data model / schema.

Aesop provides reliable, low-latency data change propagation for source and consumer systems that optionally use different data stores. It also supports
snapshot based change detection. For consumers, it provides a unified interface for change data consumption that is independent of how the data change is
sourced. More on the [Change Propagation Approach](https://github.com/Flipkart/aesop/wiki/Change-Propagation-Approach)

## Aesop Consoles
Console showing the relay and all connected consumers with SCN information per partition
![Relays](https://github.com/Flipkart/aesop/raw/master/docs/Aesop_Relay_Dashboard_Relays.png)

Live metrics on producer,consumers progress highlighting best and worst performing partition per consumer against producer SCN
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
* Prakhar Jain ([@jprakhar77](https://github.com/jprakhar77))
* Regunath B ([@regunathb](http://twitter.com/RegunathB))
* Shoury ([@Shoury](https://github.com/Shoury))
* Yogesh Dahiya ([@yogeshdfk](https://github.com/yogeshdfk))
* Arya Ketan ([@aryaKetan](https://github.com/aryaKetan))

## Aesop users
* Flipkart Payments : The payment processing system uses a MySQL data store for holding transactional data. This data is
needed in de-normalized form in the reconciliation data store(MySQL again) and an archive data store (HBase). 
Data changes need to reflect in the reconciliation and archive stores almost instantaneously. Aesop is used to do this.
This system is in production and has processed thousands of events per second with insert rates of about 30K per second on HBase.
* Flipkart User data : The Aesop relay is used to propagate changes on Flipkart user accounts to the central analysis data system. 
* Flipkart User Wishlist : The Aesop relay is used to propagate changes on Flipkart user wishlists to an Elastic Search secondary index used in searches.

