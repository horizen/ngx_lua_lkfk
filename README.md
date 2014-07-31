this is a lua module for write message to kafka base on ngx_lua

required
==========
* ngx_lua module version >= v0.9.5


Example
==========
Notice: you need add a directive `init_worker_by_lua_file init.lua` in http block

    local global = require "global"

    local lkfk = global.kfk
    lkfk:log(level, topic, key, playlod)
