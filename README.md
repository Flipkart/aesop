aesop
=====

A keen Observer of changes that can also relay change events reliably to interested parties. Provides useful infrastructure for 
building Eventually Consistent data sources and systems.

## Releases

| Release | Date | Description |
|:------------|:----------------|:------------|
| Alpha release    | December 2013      |    Proof of concept code in "sandbox"

## Getting Started

aesop aims to leverage capabilities from a at least a couple of good projects that already exist in this space viz.

* [LinkedIn Databus](https://github.com/linkedin/databus) 
* [Netflix Zeno](https://github.com/Netflix/zeno)
* [NGDATA hbase-sep](https://github.com/NGDATA/hbase-indexer/tree/master/hbase-sep)
  
The "sandbox" module currently contains proof of concept code to do the following:

* [Relay](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/relay/RelayMain.java) : An implementation of the Databus Relay that uses a Producer to create change events of the sample 'Person' type. 
    * Runtime dependencies : None
* [Relay Client](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/relay/RelayClientMain.java) : An implementation of a Consumer that connects to the sample Relay and consumes change events of the sample 'Person' type.
    * Runtime dependencies : A running 'Relay' 
* [Bootstrap Producer](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/bootstrap/GenericBootstrapProducerMain.java) : A Bootstrap i.e. special Consumer that is used for creating snapshots for change events of the sample 'Person' type. Stores these snapshots in a MySQL DB.
    * Runtime dependencies : A running 'Relay' 
* [Bootstrap Server](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/bootstrap/GenericBootstrapHttpServerMain.java) : A Bootstrap server that provides a Http interface for slow Consumers to run catch-up queries on. Returns snapshots stored by the 'Bootstrap Producer' for change events of the sample 'Person' type.
    * Runtime dependencies : None
* [Bootstrap Client](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/bootstrap/PersonBootstrapClientMain.java) : A sample bootstrap consumer that consumes snapshots in catch-up mode for change events of the sample 'Person' type. 
    * Runtime dependencies : A running 'Bootstrap Server' 
    
### HBase examples

The "sandbox" module also has some samples for consuming WAL edits from HBase writes and making these
available for consumption via a Databus Relay:

* [WALEditRelayMain](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/relay/hbase/WALEditRelayMain.java) : An implementation of the Databus Relay that uses a [WALEditPersonEventProducer](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/relay/hbase/WALEditPersonEventProducer.java) that listens to HBase WAL edits and creates change events of the sample 'Person' type.
    * Runtime dependencies : None
* [SchemaSetupMain](https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/relay/hbase/SchemaSetupMain.java) and [PersonDataInjesterMain] (https://github.com/regunathb/aesop/blob/master/sandbox/src/org/aesop/relay/hbase/PersonDataInjesterMain.java) : Classes to create a sample HBase table schema and push data to it. Useful in testing the WAL edits Relay.
    * Runtime dependencies : None
