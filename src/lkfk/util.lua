local bit = require "bit";

local strbyte = string.byte;
local strchar = string.char;
local strsub = string.sub;
local strlen = string.len;
local strfind = string.find;
local strgfind = string.gfind;

local band = bit.band
local bxor = bit.bxor
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex

local ok, new_tab = pcall(require, "table.new");
if not ok then
    new_tab = function(narr, nrec) return {} end
end

local _M = new_tab(0, 19);

_M.new_tab = new_tab

_M.debug = true

function _M.split(str, delim, max_nb)   
    -- Eliminate bad cases...
    if delim == nil then delim = "," end

    if strfind(str, delim, 1, true) == nil then  
        return { str };
    end  
    if max_nb == nil or max_nb < 1 then  
        max_nb = 0    -- no limit   
    end  
    local result = {}  
    local pat = "(.-)" .. delim .. "()"   
    local nb = 0  
    local last_pos   
    for part, pos in strgfind(str, pat) do  
        nb = nb + 1  
        result[nb] = part   
        last_pos = pos   
        if nb == max_nb then break end  
    end  
    -- Handle the last field   
    if nb ~= max_nb then  
        result[nb + 1] = strsub(str, last_pos)   
    end  
    return result
end

function _M.get_byte2(data, i)
    local a, b = strbyte(data, i, i + 1)
    return bor(lshift(a, 8), b), i + 2
end

function _M.get_byte3(data, i)
    local a, b, c = strbyte(data, i, i + 2)
    return bor(c, lshift(b, 8), lshift(a, 16)), i + 3
end


function _M.get_byte4(data, i)
    local a, b, c, d = strbyte(data, i, i + 3)
    return bor(d, lshift(c, 8), lshift(b, 16), lshift(a, 24)), i + 4
end


function _M.get_byte8(data, i)
    local a, b, c, d, e, f, g, h = strbyte(data, i, i + 7)

    -- XXX workaround for the lack of 64-bit support in bitop:
    local lo = bor(h, lshift(g, 8), lshift(g, 16), lshift(e, 24))
    local hi = bor(d, lshift(c, 8), lshift(b, 16), lshift(a, 24))
    return lo + hi * 4294967296, i + 8

    -- return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24), lshift(e, 32),
               -- lshift(f, 40), lshift(g, 48), lshift(h, 56)), i + 8
end



function _M.set_byte2(n)
    return strchar(band(rshift(n, 8), 0xff), band(n, 0xff))
end


function _M.set_byte3(n)
    return strchar( band(rshift(n, 16), 0xff),
    			    band(rshift(n, 8), 0xff),
    			    band(n, 0xff)
                   )
end


function _M.set_byte4(n)
    return strchar(band(rshift(n, 24), 0xff),
    			   band(rshift(n, 16), 0xff),
    			   band(rshift(n, 8), 0xff),
    			   band(n, 0xff)
                   )
end

function _M.get_kfk_string(data, i)
	local size, i = _M.get_byte2(data, i);
	if size == -1 then 
		return nil, i;
	end
	return strsub(data, i, i + size - 1), i + size;
end

function _M.pack_kfk_string(str)
	if not str then
		return _M.set_byte2(-1);
	end
	return _M.set_byte2(strlen(str)) .. str;
end

function _M.get_kfk_bytes(data, i)
	local size, i = _M.get_byte4(data, i);
	if size == -1 then
		return nil, i;
	end
	return strsub(data, i, i + size -1), i + size;
end

function _M.pack_kfk_bytes(str)
	if not str then
		return _M.set_byte4(-1);
	end
	return _M.set_byte4(strlen(str)) .. str;
end

_M.zero1 = "\000";
_M.zero2 = "\000\000";
_M.zero4 = "\000\000\000\000";
_M.zero8 = "\000\000\000\000\000\000\000\000";



return _M;
