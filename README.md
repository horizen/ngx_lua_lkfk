this is a lua module for kafka base on ngx_lua

Example
==========

local global = require "global"
local lkfk = global.kfk
lkfk:log(level, topic, key, playlod)
