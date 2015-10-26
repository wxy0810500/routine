local resty_mysql = require("resty.mysql")
local tableutil = require("acid.tableutil")

local repr = tableutil.str


local _M = {}
function _M.query(addr, sql)
    addr = {
        host = "54.199.237.44",
        port = 3306,
        database = "dbcool_user",
        user = "user",
        password = "4M87ai2P",
        max_packet_size = 1024 * 1024
    }

    local db, errmes = resty_mysql:new()
    if not db then
        return nil, 'MysqlError', errmes
    end

    db:set_timeout(1000)

    local ok, err, errno, sqlstate = db:connect(addr)

    if not ok then
        return nil, 'MysqlConnectionError', {err, errno, sqlstate}
    end

    ngx.log(ngx.INFO, "sql: ", repr(sql))

    local res, err, errno, sqlstate = db:query(sql)

    ngx.log(ngx.INFO, repr({'sql-rst:', res, err=err, errno=errno, sqlstate=sqlstate}))

    if not res then
        return nil, 'MysqlQueryError', {err, errno, sqlstate}
    end

    local ok, err = db:set_keepalive(10000, 100)
    if not ok then
        ngx.log(ngx.ERR, 'failure to set mysql keepalive: ', err)
    end

    return res, nil, nil
end

return _M
