this is a lua module for write message to kafka base on ngx_lua

Example
==========
1. please check ngx lua module version v0.9.5 or later
2. you need add a directive `init_worker_by_lua_file init.lua` in http block
---

###and then in your code

local global = require "global"

local lkfk = global.kfk

lkfk:log(level, topic, key, playlod)
