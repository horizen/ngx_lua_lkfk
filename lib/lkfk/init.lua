local lkfk = require "lkfk.kfk"

local ok, err = lkfk.init()
if not ok then
    error(err)
end
