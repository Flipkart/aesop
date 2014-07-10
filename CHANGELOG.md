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
