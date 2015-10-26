local json = require("cjson")
local dbshard = require( "dbshard" )
local strutil = require( "strutil" )
local apierr = require( "apierr" )
local tableutil = require("acid.tableutil")
local s2json = require("s2json")
local sql_field = require("sql.field")
local sql_sql = require( "sql.sql" )
local dbfield = require("dbfield")
local tableutil = require("acid.tableutil")

local repr = tableutil.str

local apis = {
    db = require("api.db"),
    row = require("api.row"),
    idx = require("api.idx"),
    dir = require("api.dir"),
    journal = require("api.journal"),
    users = require("api.users"),
}

local _rest = {
    db = {},
    row = {},
    idx = {},
    dir = {},
    users = {},
}

local _M = {}

-- NOTE: m3_ts is 64bit int and is too long to fit into a lua number so that
-- we can only use int-string to store m3_ts

local function dieerr(r, err, errmes)
    if err then
        _M.quiterr(err, errmes)
    end
    return r
end

function _M.rest_api()

    local uris = _M.get_uris()
    local subject, act = uris[3], uris[4]

    local args = dieerr(_M.get_rest_args())

    if _rest[subject] and _rest[subject][act] then
        return _rest[subject][act](args)
    end

    if subject == 'db' then
        return _M.rest_db(act, args)
    else
        return _M.quiterr('InvalidArgument', { 'invalid subject', subject })
    end
end

function _M.rest_db(act, args)

    if act == 'add' then
        local r = dieerr(_M.api('db.add', args))
        ngx.log(ngx.ERR, repr(r))
        return _M.output_json(200, {}, {db_id=r.insert_id})

    elseif act == 'get' then
        local r = _M.db_get_or_die(args.db_id)
        return _M.output_json(200, {}, r)

    elseif act == 'ls' then
        -- add empty name to force to use index
        if args.name == nil then
            args.name = ''
        end
        local r = dieerr(_M.api('db.ls', args))
        return _M.output_json(200, {}, r)

    else
        return _M.quiterr('InvalidArgument', { 'invalid subject and act', 'db', act })
    end
end

function _rest.users.get(args)

	if args.account == nil then 
		return _M.quiterr("InvalidArgument", { 'Account must not be empty' })
	else
		args.conditions = { { '=', { account=args.account} } }
		local rst = dieerr(_M.api('users.get', args))
	end
	return _M.output_json(200,{},rst)
end
function _rest.row.add(args)

    local db = _M.db_get_or_die(args.db_id)
    dieerr( _M.check_row_field(db, args.body))

    local j = {
        act = 'add',
        row = {
            db_id = db.db_id,
            is_del = 0,
            body = args.body,
        },
    }

    local rst = dieerr(_M.api('journal.add', j))
    j.journal_id = rst.insert_id

    dieerr( _M.journal_apply( db, j ) )

    _M.output_json(200, {}, {
        db_id = j.row.db_id,
        key = j.row.key,
        ver = j.row.ver
    })
end
function _rest.row.update(args)

    local db = _M.db_get_or_die(args.db_id)
    dieerr(_M.check_row_field(db, args.body))

    local key = args.key
    if key == nil then
        return _M.quiterr("InvalidArgument", {'key must not be empty'})
    end

    local j = {
        act = 'update',
        row = {
            db_id = db.db_id,
            is_del = 0,
            key = key,
            body = args.body,
        },
    }

    local rst = dieerr(_M.api('journal.add', j))
    j.journal_id = rst.insert_id

    local rst = dieerr(_M.journal_apply(db, j))

    _M.output_json(200, {}, {db_id=j.row.db_id, key=j.row.key, ver=j.row.ver})
end
function _rest.row.get(args)

    local db = _M.db_get_or_die(args.db_id)

    if args.key == nil then
        return _M.quiterr("InvalidArgument", { 'key must not be empty' })
    end

    args.nlimit = 1
    args.conditions = { { '=', { db_id=args.db_id, key=args.key, ver=args.ver } } }

    local rst = dieerr(_M.api('row.ls', args))

    _M.output_json(200, {}, rst[1] or json.null)
end
function _rest.row.ls(args)
    -- list by key

    -- db_id, key, is_del, ver
    local db = _M.db_get_or_die(args.db_id)

    local resp_field_names = dieerr(_M.get_fieldnames(args))
    local multiversion = tonumber(args.multiversion) == 1

    local ls_args = {
        nlimit = args.nlimit,
        conditions = {
            args.left or { '>=', {} },
            args.right or { '<=', {} },
        }
    }
    for i, cond in ipairs(ls_args.conditions) do
        cond[2].db_id = args.db_id
    end

    if not multiversion then
        -- group by key thus ignores smaller version with the same key
        ls_args.group = {'db_id', 'key'}
    end

    local rst = dieerr(_M.api('row.ls', ls_args))
    local rows = rst

    if resp_field_names ~= nil then
        for i, r in ipairs(rows) do
            r.body = tableutil.sub(r.body, resp_field_names)
        end
    end

    _M.output_json(200, {}, rows)
end
function _rest.row.search(args)

    -- db_id, key, is_del, ver
    local db = _M.db_get_or_die(args.db_id)

    local resp_field_names = dieerr(_M.get_fieldnames(args))
    local field_dict = dbfield.to_field_dict(db.fields)

    args.nlimit = args.nlimit or 1024

    local condition = dieerr( _M.cond_norm( field_dict, args ) )
    if tableutil.nkeys(condition) == 0 and args.dir_id == nil then
        -- no condition, search by primary key
        return _rest.row.ls(tableutil.sub(args, {'db_id', 'nlimit', 'fieldnames'}))
    end

    local idx_req = {
        nlimit = 1024,
        db_id = db.db_id,
        conditions = {},
    }

    -- TODO combine condition to optimize search
    local idx_conditions = {}
    local prev_k = ''
    for k, cond in pairs(condition) do
        prev_k = k
    end

    ngx.log(ngx.ERR, repr(condition))

    for k, cond in pairs(condition) do

        local field = field_dict[k]

        local op, val = cond[1], cond[2]
        local idx_key = dieerr(dbfield.make_idx(args.dir_id, field, val))

        local cc = {
            op, {
                db_id = args.db_id,
                idx_key = idx_key,
            },
        }
        table.insert(idx_req.conditions, cc)

        -- add left boundary
        if op:sub(1, 1) == '<' then
            table.insert(idx_req.conditions, {
                '>', {
                    db_id = args.db_id,
                    idx_key = dbfield.make_start_idx(args.dir_id, field)
                }})
        end

        -- add right boundary
        if op:sub(1, 1) == '>' then
            table.insert(idx_req.conditions, {
                '<', {
                    db_id = args.db_id,
                    idx_key = dbfield.make_end_idx(args.dir_id, field)
                }})
        end

        condition[k] = nil
        break
    end

    local rst = dieerr(_M.api('idx.ls', idx_req))

    local exist = {}
    local filtered = {}
    for _, idx in ipairs(rst) do

        local key = idx.key
        local ver = idx.ver
        local uniqkey = key ..'/' .. ver

        if exist[uniqkey] == nil then

            exist[uniqkey] = true

            local row = dieerr(_M.api('row.ls', {
                nlimit = 1,
                conditions = {
                    { '=', {db_id=idx.db_id, key=key, id_del=0, ver=ver} }
                }
            }))
            local rr = row[1]

            local matched = true
            for k, cond in pairs(condition) do
                if rr.body[k] == nil then
                    matched = false
                    break
                end
                local field = field_dict[k]

                local op, val = cond[1], cond[2]
                local idx_key = dbfield.make_idx(args.dir_id, field, rr.body[k])
                local cond_key = dbfield.make_idx(args.dir_id, field, val)

                matched = dbfield.compare(op, idx_key, cond_key)
                if not matched then
                    break
                end
            end

            if matched then
                if resp_field_names ~= nil then
                    rr.body = tableutil.sub(rr.body, resp_field_names)
                end
                table.insert(filtered, rr)
            end
        end
    end

    _M.output_json(200, {}, filtered)
end
function _rest.row.lsdir(args)

    -- db_id, key, is_del, ver
    local db = _M.db_get_or_die(args.db_id)

    local resp_field_names = dieerr(_M.get_fieldnames(args))
    local field_dict = dbfield.to_field_dict(db.fields)

    args.nlimit = args.nlimit or 1024

    local dirreq = {db_id = db.db_id, dir_id = args.dir_id}
    local dir = dieerr( _M.api('dir.getbyid', dirreq) )
    dir = dir[1]
    if dir == nil then
        dieerr(nil, 'NoSuchDir', dirreq)
    end

    local root = dir.root
    local dirdef = db.dir[root]
    -- field for next level
    local field_name = dirdef[tonumber(dir.level) + 1]

    local field = field_dict[field_name]
    local val = args.start

    local left = dbfield.make_idx(dir.dir_id, field, val)
    local right = dbfield.make_end_idx(dir.dir_id, field)
    local idx_req = {
        nlimit = 1024,
        db_id = db.db_id,
        conditions = {
            { '>=', { db_id = args.db_id, idx_key = left } },
            { '<', { db_id = args.db_id, idx_key = right } },
        },
    }

    local rst = dieerr( _M.api( 'idx.ls', idx_req ) )

    local exist = {}
    local filtered = {}
    for _, idx in ipairs(rst) do

        local key = idx.key
        local ver = idx.ver
        local uniqkey = key ..'/' .. ver

        local row = dieerr(_M.api('row.ls', {
            nlimit = 1,
            conditions = {
                { '=', {db_id=idx.db_id, key=key, id_del=0, ver=ver} }
            }
        }))
        local rr = row[1]

        local matched = true

        if resp_field_names ~= nil then
            rr.body = tableutil.sub(rr.body, resp_field_names)
        end
        table.insert(filtered, rr)
    end

    _M.output_json(200, {}, filtered)
end

function _rest.dir.ls(args)
    local db = _M.db_get_or_die(args.db_id)
    local dir_id = args.dir_id or 0
    local req = {
        db_id = db.db_id,
        nlimit = args.nlimit,
        conditions = {
            {'>=', {db_id = db.db_id, parent_dir_id = dir_id, dir_name = args.dir_name}},
            {'<', {db_id = db.db_id, parent_dir_id = dir_id + 1}},
        },
    }
    local r = dieerr( _M.api('dir.ls', req) )
    _M.output_json(200, {}, r)
end

function _M.check_row_field(db, body)
    local field_dict = dbfield.to_field_dict(db.fields)

    for k, v in pairs(body) do
        if field_dict[k] == nil then
            return nil, 'InvalidField', {'no suck field', k}
        end
    end

end
function _M.cond_norm(field_dict, args)

    local condition = tableutil.dup(args.condition or {}, true)

    for k, cond in pairs(condition) do
        local field = field_dict[k]
        if field == nil then
            return nil, 'InvalidCondition', {'field not found', k}
        end

        if field.tp == 'ref' then
            if type(cond) ~= 'table' then
                return nil, 'InvalidCondition', {'invalid ref', cond}
            end
            condition[k] = {'=', cond}
        else
            if type(cond) ~= 'table' then
                condition[k] = { '=', cond }
            end
        end

    end
    ngx.log(ngx.ERR, repr(condition))
    return condition
end
function _M.get_fieldnames(args)
    local fns = args.fieldnames
    if fns ~= nil and type(fns) ~= 'table' then
        return nil, 'InvalidArgument', {'fieldnames must be a table', fns}
    end
    return fns, nil, nil
end

function _M.journal_apply(db, j)

    local act = j.act
    local row = j.row

    row.ver = j.journal_id

    if act == 'add' then

        row.key = _M.make_key(row.ver)

        local rst, err, errmes = _M.api('row.add', row)
        if err then
            return nil, err, errmes
        end

        local rst, err, errmes = _M.add_dirs(db, row)
        if err then
            return nil, err, errmes
        end

        return _M.journal_apply_idx(db, row, 'add')

    elseif act == 'update' then

        local rst, err, errmes = _M.api('row.add', row)
        if err then
            return nil, err, errmes
        end

        local rst, err, errmes = _M.journal_apply_idx(db, row, 'add')
        if err then
            return nil, err, errmes
        end

        local req = {
            nlimit = 1,
            db_id = row.db_id,
            conditions = {
                { '>', {db_id = row.db_id, key = row.key, ver=row.ver} }
            }
        }

        local r, err, errmes = _M.api('row.ls', req)
        if err then
            return nil, err, errmes
        end

        local prev = r[1]

        if prev ~= nil then
            return _M.journal_apply_idx(db, prev, 'remove_idx')
        end
    end
end
function _M.journal_apply_idx(db, row, act)

    local idxs, err, errmes = _M.create_idxs(db, row)
    if err then
        return nil, err, errmes
    end

    for k, idx_args in pairs(idxs) do
        local rst, err, errmes = _M.api('idx.' .. act, idx_args)
        if err then
            return nil, err, errmes
        end
    end

    return nil, nil, nil
end
function _M.create_idxs(db, row)

    local rst = {}
    local fields = db.fields

    for i, field in ipairs(fields) do
        local v = row.body[field.name]

        if v ~= nil then

            if field.comp == 1 then
                if type(v) ~= 'table' then
                    return nil, 'InvalidField', {'composite field is not table', field, v}
                end

                for ks, compv in tableutil.deep_iter(v) do

                    local idx_arg, err, errmes = _M.make_idx_arg(0, row, field, compv)
                    if err then
                        return nil, err, errmes
                    end

                    local compk = tableutil.dup(ks, true)

                    if rst[idx_arg.idx_key] == nil then
                        idx_arg.comp_key = {compk}
                        rst[idx_arg.idx_key] = idx_arg
                    else
                        table.insert(rst[idx_arg.idx_key].comp_key, compk)
                    end

                end
            else
                local idx_arg, err, errmes = _M.make_idx_arg(0, row, field, v)
                if err then
                    return nil, err, errmes
                end
                rst[idx_arg.idx_key] = idx_arg
            end
        end
    end

    return rst, nil, nil
end
function _M.add_dirs(db, row)

    local field_dict = dbfield.to_field_dict(db.fields)
    local dbdir = db.dir
    if dbdir == nil then
        return nil, nil, nil
    end

    for rootname, dir_field_keys in pairs(dbdir) do

        local root, err, errmes = _M.get_or_add_dir(db, rootname, 0, 0, rootname)
        if err then
            return nil, err, errmes
        end

        local node = root

        -- last field is is sorting key
        local last = dir_field_keys[#dir_field_keys]
        table.remove(dir_field_keys, #dir_field_keys)

        local err, errmes
        for i, k in ipairs(dir_field_keys) do
            local dirname = row.body[k]

            ngx.log(ngx.ERR, "to get dir:" .. repr({node, dirname}))

            node, err, errmes = _M.get_or_add_dir(db, rootname, i, node.dir_id, dirname)
            if err then
                return nil, err, errmes
            end
        end

        local sort_field = field_dict[last]
        local idx_arg, err, errmes = _M.make_idx_arg( node.dir_id, row, sort_field, row.body[last] )

        local r, err, errmes = _M.api('idx.add', idx_arg)
        if err then
            return nil, err, errmes
        end
    end

    return nil, nil, nil
end

function _M.get_or_add_dir(db, root, level, parent_dir_id, val)

    local req = {
        db_id = db.db_id,
        parent_dir_id = parent_dir_id,
        dir_name = val,
    }
    local parents, err, errmes = _M.api('dir.get', req)
    if err then
        return nil, err, errmes
    end

    if #parents > 0 then
        return parents[1], nil, nil
    end

    local req = {
        db_id = db.db_id,
        parent_dir_id = parent_dir_id,
        dir_name = val,
        root = root,
        level = level,
    }
    local r, err, errmes = _M.api('dir.add', req)
    if err then
        return nil, err, errmes
    end

    local req = {
        db_id = db.db_id,
        dir_id = r.insert_id,
    }

    local r, err, errmes = _M.api('dir.getbyid', req)
    if err then
        return nil, err, errmes
    end

    return r[1], nil, nil

end

function _M.make_idx_arg(dir_id, row, field, val)
    local idx_key, err, errmes = dbfield.make_idx(dir_id, field, val)
    if err then
        return nil, err, errmes
    end

    local idx_arg = {
        db_id   = row.db_id,
        idx_key = idx_key,
        key     = row.key,
        is_del  = row.is_del,
        ver     = row.ver,
    }
    return idx_arg
end
--get db info,if the particular db exsits
function _M.db_get_or_die(db_id)

    local rst = dieerr(_M.api("db.get", {db_id=db_id}))
    local db = rst[1]

    if db == nil then
        return dieerr(nil, "NoSuchDB", { db_id })
    end
    return db
end

function _M.api(supertable_and_act, args)

    ngx.log(ngx.INFO, "api-call: ", supertable_and_act, " ", repr(args))

    local elts = strutil.split(supertable_and_act, '[.]')
    local supertable, act = elts[1], elts[2]

    local mod = apis[ supertable ]
    if mod == nil then
        return nil, "UnknownSubject", { supertable }
    end

    local action = mod.acts[ act ]
    if action == nil then
        return nil, "UnknownCommand", { supertable, act }
    end

    local sess = {
        mod = mod,
        actname = act,
        args= args,
        sharding= {
            supertable= supertable,
            keys= mod.keys,
            rw= action.rw,
            dom=nil,
            table=nil,
            sql=nil,
        },
    }

    local ok = action.check( sess )
        and _M._tmp_get_shard(sess)
        -- and sql_sql.get_shard( sess )
        -- and _M.set_shard_header( sess )
        and action.make_sql( sess )

    if not ok then
        return nil, sess.err.err, sess.err.errmes
    end

    local rst, err, errmes = sql_sql.call_sql(sess.sharding.dom, sess.sharding.sql)
    if err then
        return nil, err, errmes
    end

    -- not a list o not need to convert
    if rst[1] == nil or action.rst == nil then
        return rst
    end

    local fields = mod.fields_dic
    for _, row in ipairs(rst) do

        for k, _ in pairs(action.rst) do

            if row[k] and fields[k].unquote then
                row[k] = fields[k].unquote(row[k])
            end

        end
    end

    ngx.log(ngx.INFO, 'api-rst: ', repr({rst=rst, err=err, errmes=errmes}))
    return rst
end

function _M._tmp_get_shard(sess)
    local sharding = sess.sharding
    sharding.dom = 'my-local'
    sharding.table = sharding.supertable
    sharding.next_from = nil
    return true
end
function _M.internal_api( urilist, args )
    local act = urilist[ 4 ]
    local rs
    if act == 'dbhosts' then
        rs = {dbshard.get_dbhosts()}
        return _M.output_json( 200, nil, rs )
    end
end
--code: response status code
--headers:response field:"rst/err/errmes"
--body:it's a lua table with all replied "field-value"
function _M.output_json( code, headers, body )
    ngx.status = code
    ngx.header[ 'Content-Type' ] = 'application/json'
    ngx.print( s2json.enc( {rst=body} ) )
    ngx.eof()
    ngx.exit( ngx.HTTP_OK )
    return true
end
function _M.set_shard_header( sess )
    ngx.header[ 'x-shard-next' ] = s2json.enc( sess.sharding.next_from )
    return true
end

function _M.make_key(journal_id)
    local s = string.format("%016x", journal_id)
    return s
end

function _M.get_uris()
    local uri = ngx.var.request_uri
    uri = strutil.split( uri, '?' )[ 1 ]

    local urilist = strutil.split( uri, '/' )
    return urilist
end
function _M.get_rest_args()
    local args = ngx.req.get_uri_args()

    ngx.req.read_body()
    local body = ngx.req.get_body_data()

    local reqbody, err
    if body ~= '' then
        reqbody, err = s2json.dec(body)
        if err then
            return nil, 'InvalidArgument', {'body is not json'}
        end

        tableutil.merge(args, reqbody)
    end
    return args, nil, nil
end

function _M.quiterr(err, errmes)
    local sess = {}
    apierr.set_err( sess, err, errmes )
    return apierr.quit_sess_err( sess )
end

return _M
