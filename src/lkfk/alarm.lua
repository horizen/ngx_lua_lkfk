local concat = table.concat
local strlen = string.len
local ipairs = ipairs
local ngxlog = ngx.log
local ERR = ngx.ERR

local req = {
    "POST /yingeng/api/idle_alert.php HTTP/1.1\r\n",
    "Content-Length: ",
    "",
    "Host: 10.75.6.48\r\n",
    "Accept: */*\r\n",
    "Content-Type: application/x-www-form-urlencoded\r\n\r\n",
    "alertid=3500002&service=Kafka&object=Monitor&mailto=yaowei2&msgto=yaowei2&subject=kafka&content=",
}
local content = {};

local alarm = false;

local _M = {};

function _M.add(str)
    if #content > 50 then
        ngxlog(ERR, "[kafka] too many alarm monitor pending to send");
        return;
    end
    for _, c in ipairs(content) do
        if c == str then
            return;
        end
    end
    content[#content + 1] = str;
    alarm = true;
end


-- this function is not safe for mutithread, we need lock
function _M.send(tcp)
    if not alarm then
        return true;
    end
    alarm = false;

    tcp:settimeout(3000);
    local ok, err = tcp:connect("10.75.6.48", 80);
    if not ok then
        alarm = true;
        return nil, err;
    end
    
    req[8] = concat(content, "\n");
    req[3] = (strlen(req[7]) + strlen(req[8])) .. "\r\n";

    local bytes, err = tcp:send(concat(req));
    if not bytes then
        alarm = true;
        return nil, err;
    end
    
    alarm = false;
    content = {};
    --[[
    local data, err = tcp:receive("*a");
    if not data then
        tcp:close();
        return nil, err;
    end
    --]]
    tcp:close();

    return true;
end

return _M;
