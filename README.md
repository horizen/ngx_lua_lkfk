this is a lua module for write message to kafka base on ngx_lua

required
==========
* openresty (>=0.9.9 feature of full-duplex)
* kafka 0.8.x
* libuuid-devel (yum install libuuid-devel for centos)


Example
==========

Step 1
===
1. add a directive `init_worker_by_lua_file init.lua` in http block

2. you need config `lua_socket_log_errors off` if you want't see cosocket timeout err

3. add lua shared dict config. for example: `lua_shared_dict kfk 50m`

4. paste follow code in `init.lua`

```
local lkfk = require "lkfk.kfk"

local ok, err = lkfk.init()
if not ok then
    error(err)
end
```

Step 2
===
edit config for kafka producer `lib/lkfk/conf.lua`

| config | default | description |
|--------|--------|
| failed_cb    | kfk_failed_handle       |msg send failed callback，this is use for backup, you can defind your fail handle if necessary|
| backpath    | ngx.config.prefix() .. "/backup/"       |backup path for failed msg necessary|
| client_id    | lkfk       ||
| metadata_broker_list    |        |metadata broker list, recommend at least two node, for example: {"127.0.0.1:9092,127.0.0.1:9093"} |
| topics    |        |topic for kafka|
| request_required_acks    | 1       |msg send failed callback，this is use for backup, you can defind your fail handle if necessary|
| request_timeout_ms    | 5000       |timeout for kafka|
| message_timeout_ms    | 5000       |msg timeout, the different from request_timeout_ms is msg timeout add network transfer time|
| partitioner    | default_partitioner       |partitioner algorithm, you can define your partitioner if necessary|
| message_send_max_retries    | 2       |max number try to send failure msg necessary|
| retry_backoff_ms    | 200       ||
| metadata_refresh_interval_ms    | 60000       |force pull metadata interval|
| queue_buffering_max_ms    | 2000       ||
| queue_buffering_max_messages    | 50000       ||
| batch_num_messages    | 500       ||
| conn_retry_limit    | 2       |max number try to connect broker, if fail, after conn_retry_timeout time, we retry|
| conn_retry_timeout    | 200       |connect retry interval|
| kfk_status    | trye       |for statistics, use ngx_lua shared dict|

-----------------



**failed_cb**=kfk_failed_handle

*msg send failed callback，this is use for backup, you can defind your fail handle if necessary*


**backpath**=ngx.config.prefix() .. "/backup/"

*backup path for failed msg*


**client_id**="lkfk"

*client id*


**metadata_broker_list**={"host1:port1,host2:port2"}

*metadata broker list, recommend at least two node*


**topics**={"wanliu_order_basic", "wanliu_driver"}

*topic for kafka*


**request_required_acks**=1


**request_timeout_ms**=5000

*timeout for kafka*


**message_timeout_ms**=5000
*msg timeout, the different from request_timeout_ms is msg timeout add network transfer time*


**partitioner**=default_partitioner

*partitioner function, you can define your partitioner if necessary*


**message_send_max_retries**=2

*max number try to send failure msg*

**retry_backoff_ms**=100

**metadata_refresh_interval_ms**=60000

**queue_buffering_max_ms**=2000

**queue_buffering_max_messages**=50000

**batch_num_messages**=5

**conn_retry_limit**=3

**conn_retry_timeout**=60

*connect retry interval*


**kfk_status**=true
*for statistics, use ngx_lua shared dict*


Step 3
======
use lkfk in your code
```
local kfk = require "lfkf.kfk"

kfk.log(topic, key, playlod)
```