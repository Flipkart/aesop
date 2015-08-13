## 1.2.1-SNAPSHOT (Jul 10, 2015)
- **New features:**
  - Support for Kafka Data Layer and Client Consumer
- **Bug Fixes:**
  - Fixes for Hbase Relay
  - Fix for Sample Mysql Relay

## 1.2.0 (Jul 10, 2015)
- Bugfix release

## 1.1.0 (Apr 2, 2015)
- Upgrading snapshot to release

## 1.1.0-SNAPSHOT (Feb 15, 2015)
- **New features:**
  - Support for transforming source event before mapping
  - Support for transforming destination event after mapping
  - Support for nesting columns during the mapping phase
  - Support for Adapters to adapt the Destination Event to your custom object
  - Support for Blocking Bootstrap Mysql Producer
  - Support for Elastic Search Data Layer
  - Sample Mapper, Processor and Nesting module with example configs
  - Sample Elastic Search Consumer Data Layer module with example configs
  - Partition view for Dashboard UI
- **Re Org:**
  - Renamed destination data layer to DestinationEventProcessor
  - Grouped modules under appropriate directories
  - Better Wiki and Java Docs
- **Bug Fixes**
  - Aesop Dashboard UI Bug fix for Consumer SCN
  - MySql producer TX interpretation across tables
<br />

## 1.0.7-SNAPSHOT (July 10, 2014)
- **New features:**
  - Support for Push(bin log replication) and Pull(snapshot) Relay producers
  - Support to deploy multiple Relays, Producers and Consumers (via configuration) inside a single runtime 
  - Supported data stores include MySQL(deployed in production) and HBase (code ready and work-in-progress)
  - Relay dashboard with metrics
  - Support for Cluster Load balanced partitioned clients for High Availability and Load balancing
  - Support for Relay client to automatically fallback to Bootstrap database (if configured) and switch to on-line consumption
  - MySQL producer enhanced to support MySQL 5.6 version 
  - Support for automated AVRO Schema generation. Currently supported when source is MySql
  - Support for MOD based default Partition handler. Any other partition handler can be plugged in
  - Sample Producers, Consumers for getting started
<br />
