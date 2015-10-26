local sql_sql = require( "sql.sql" )
local sql_field = require( "sql.field" )

local add_chk = sql_sql.generic_chk
local add_sql = sql_sql.generic_add_sql

local get_chk = sql_sql.generic_chk
local get_sql = sql_sql.generic_get_sql

local set_chk = sql_sql.generic_chk
local set_sql = sql_sql.generic_set_sql

local remove_chk = sql_sql.generic_chk
local remove_sql = sql_sql.generic_remove_sql

local ls_chk = sql_sql.generic_chk
local ls_sql = sql_sql.indexed_ls_sql

local _match = {
    op_seq=false,
}

local get_rst = {
    user_id = true,
    account = true,
    password = true,
    username = true,
}

local fld = sql_field.fields

local _M = {
    keys = {},

    fields_dic= sql_field.make_fields_dic( {
        fld.str( "account", "account", 0 ),
        fld.num( "id", "id", 0 ),
        fld.str( "password", "password", 0 ),
        fld.str( "username", "username", 0 ),

        fld.num( "_", "nlimit", 0 ),
    } ),

    acts={
--[[       
		add={
            rw='w', check=add_chk, make_sql=add_sql,
            args={
                fields=true,
                dir=true,
                name=true,
            },
        },
]]
        get={
            rw='r', check=get_chk, make_sql=get_sql,
            ident={
                account=true,
            },
            rst=get_rst,
        },
 --[[
        set={
            rw='w', check=set_chk, make_sql=set_sql,
            ident={
                db_id=true,
            },
            args={
                fields=false,
                dir=false,
                name=false
            },
        },

        remove={
            rw='w', check=remove_chk, make_sql=remove_sql,
            ident={
                db_id=true,
            },
        },

        ls={
            rw='r', check=ls_chk, make_sql=ls_sql,
            index={'name'},
            lim={
                nlimit=true,
            },
            rst=get_rst,
            gen=function( sess )
                local a = sess.args
                if a.nlimit == nil then
                    a.nlimit = 1024
                end
                return true
            end,
        },
--]]
    },
}

return _M
