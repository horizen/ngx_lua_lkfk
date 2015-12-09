local ffi = require "ffi"

ffi.cdef[[
	typedef unsigned char uuid_t[16];
    void uuid_generate(uuid_t out);
    void uuid_generate_random(uuid_t out);
    void uuid_generate_time(uuid_t out);
	void uuid_unparse(const uuid_t uu, char *out);
]]

local uuid = ffi.load("libuuid")

local _M = {}

function _M.generate(type)
    local type = type or "random"
	local uuid_t = ffi.new("uuid_t");
	local uuid_out = ffi.new("char[64]");
	if type == "random" then
		uuid.uuid_generate_random(uuid_t);
	elseif type == "time" then
		uuid.uuid_generate_time(uuid_t);
	else 
		uuid.uuid_generate(uuid_t);
	end

	uuid.uuid_unparse(uuid_t, uuid_out);
	return ffi.string(uuid_out);
end

return _M
