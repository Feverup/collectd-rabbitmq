collectd-rabbitmq
================= 

A collected plugin, written in python, to collect statistics from RabbitMQ. Requires that the rabbitmq management plugin is installed. 

Installation
============

Place the rabbitmq.py file in the plugins directory for collect. 
Each of these statistics have thier own custom entery in collectd's type database. 
To add these types run:

``` cat config/types.db.custom >> /usr/share/collectd/types.db ```


Configuration 
=============

This plugin supports a small amount of configuration options:

* ```Username```: The rabbitmq user. Defaults to ```guest```.

* ```Password```: The rabbitmq user password. Defaults to ```guest```.

* ```Realm```: The http realm for authentication. Defaults to ```RabbitMQ Management```. 

* ```Host```: The hostname that the rabbitmq server running on. Defaults to ```localhost```.

* ```Port```: The port that the rabbitmq server is listening on. Defaults to ```15672```.

* ```Ignore```: The queue to ignore, matching by Regex.  See example.

Example Configuration
=====================

```
LoadPlugin python
<Plugin python>
  ModulePath "/mnt/projects/collectd-rabbitmq/"
  LogTraces true
  Interactive false
  Import rabbitmq
  <Module rabbitmq>
    Username "guest"
    Password "guest"
    Realm "RabbitMQ Management"
    Host "localhost"
    Port "15672"
	<Ignore "queue">
	  Regex "amq-gen-.*"
	  Regex "tmp-.*"
	</Ignore>
  </Module>
</Plugin>
```

Nodes
=====

For each node the following statistics are gathered:

#### OS stats

* disk_free

* disk_free_limit

* fd_total

* fd_used 

* mem_limit

* mem_used

* proc_total 

* proc_used

* processors

* run_queue

* sockets_total

* sockets_used

#### IO stats

* io_seek_count
* io_seek_avg_time
* io_sync_count
* io_sync_avg_time
* io_read_bytes
* io_read_count
* io_read_avg_time
* io_write_bytes
* io_write_count
* io_write_avg_time
* queue_index_read_count
* queue_index_write_count
* queue_index_journal_write_count
* mnesia_ram_tx_count
* mnesia_disk_tx_count
* msg_store_read_count
* msg_store_write_count

Queues
=======

For each queue in each vhost the following statistics are gathered:
> NOTE: The ```/``` vhost name is sent as ```default```

* message_stats
    * deliver_get
    * deliver_get_details 
    	* rate 
    * get
    * get_details
        * rate
    * publish
    * publish_details
        * rate
    * redeliver  
    * redeliver_details
        * rate
* messages
* messages_ready
* messages_unacknowledged
* memory
* consumers

Exchanges
=========

For each exchange in each vhost the following statistics are gathered: 
> NOTE: The ```/``` vhost name is sent as ```default```

* publish_in

* publish_out


Developing
==========

Whenever a new stat is to be added, it must be added to the config/types.db.custom file. 
