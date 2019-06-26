local lu = require "luaunit"
local raft = require "lraft"
local ldbstorage = require "ldbstorage"
local raftpb = raft.pb
raft.default_logger().level = raft.LOG_OFF

local function makeSnapshot(data, index, term, cs)
    local snap = raftpb.snapshot()
    snap.data = data
    snap.metadata.index = index
    snap.metadata.term = term
    snap.metadata.conf_state.nodes = cs
    return snap
end

local function makeEntry(index, term, data, type)
    return raftpb.entry(index, term, data, type)
end

local function equal_entrys(left, right)
    assert(#left == #right)
    for i = 1, #left do
        local e1 = left[i]
        local e2 = right[i]
        assert(e1.index == e2.index)
        assert(e1.term == e2.term)
    end
end

function TestStorageTerm()
    local ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
    local tests = {
        {i = 2, werr = raft.ErrCompacted, wterm = 0, wpanic = false},
        {i = 3, werr = raft.OK, wterm = 3, wpanic = false},
        {i = 4, werr = raft.OK, wterm = 4, wpanic = false},
        {i = 5, werr = raft.OK, wterm = 5, wpanic = false},
        {i = 6, werr = raft.ErrUnavailable, wterm = 0, wpanic = false}
    }

    for _, tt in ipairs(tests) do
        ldbstorage.delete_db("./testdb")
        local s = ldbstorage.new("./testdb", ents)
        local ok, err = pcall(function()
            local err, term = s:Term(tt.i)
            assert(err == tt.werr)
            assert(term, tt.wterm)
        end)
        s:close()
        s:delete_db()
        assert(ok == not tt.wpanic, err)
    end
end

function TestStorageEntries()
    local maxLimit = raft.noLimit
    local ents = {
        makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)
    }
    local tests = {
        {2, 6, maxLimit, raft.ErrCompacted, {}},
        {3, 4, maxLimit, raft.ErrCompacted, {}},
        {4, 5, maxLimit, raft.OK, {makeEntry(4, 4)}},
        {4, 6, maxLimit, raft.OK, {makeEntry(4, 4), makeEntry(5, 5)}},
        {
            4, 7, maxLimit, raft.OK,
            {makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}
        }, -- even if maxsize is zero, the first entry should be returned
        {4, 7, 0, raft.OK, {makeEntry(4, 4)}}, -- limit to 2
        {
            4, 7, ents[2]:byte_size() + ents[3]:byte_size(), raft.OK,
            {makeEntry(4, 4), makeEntry(5, 5)}
        }, -- limit to 2
        {
            4, 7,
            ents[2]:byte_size() + ents[3]:byte_size() + ents[4]:byte_size() / 2,
            raft.OK, {makeEntry(4, 4), makeEntry(5, 5)}
        }, {
            4, 7,
            ents[2]:byte_size() + ents[3]:byte_size() + ents[4]:byte_size() - 1,
            raft.OK, {makeEntry(4, 4), makeEntry(5, 5)}
        }, -- all
        {
            4, 7,
            ents[2]:byte_size() + ents[3]:byte_size() + ents[4]:byte_size(),
            raft.OK, {makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}
        }
    }

    for _, tt in ipairs(tests) do
        ldbstorage.delete_db("./testdb")
        local s = ldbstorage.new("./testdb", ents)
        local ok, err = pcall(function()
            local err, entries = s:Entries(tt[1], tt[2], tt[3])
            assert(err == tt[4])
            if err == raft.OK then
                equal_entrys(entries, tt[5])
            else
                assert(#tt[5] == 0)
            end
        end)
        s:close()
        s:delete_db()
        assert(ok, err)
    end
end

function TestStorageLastIndex()
    local ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
    ldbstorage.delete_db("./testdb")
    local s = ldbstorage.new("./testdb", ents)

    local ok, err = pcall(function()
        local err, last = s:LastIndex()
        assert(err == raft.OK)
        assert(last == 5)

        s:append({makeEntry(6, 5)})
        err, last = s:LastIndex()
        assert(err == raft.OK)
        assert(last == 6)
    end)
    s:close()
    s:delete_db()
    assert(ok, err)
end

function TestStorageFirstIndex()
    local ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
    ldbstorage.delete_db("./testdb")
    local s = ldbstorage.new("./testdb", ents)
    local ok, err = pcall(function()
        local err, first = s:FirstIndex()
        assert(err == raft.OK)
        assert(first == 4)

        s:compact(4)
        err, first = s:FirstIndex()
        assert(err == raft.OK)
        assert(first == 5)
    end)
    s:close()
    s:delete_db()
    assert(ok, err)
end

function TestStorageCompact()
    local ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
    local tests = {
        {2, raft.ErrCompacted, 3, 3, 3}, {3, raft.ErrCompacted, 3, 3, 3},
        {4, raft.OK, 4, 4, 2}, {5, raft.OK, 5, 5, 1}
    }

    for _, tt in ipairs(tests) do
        ldbstorage.delete_db("./testdb")
        local s = ldbstorage.new("./testdb", ents)
        local ok, err = pcall(function()
            local err = s:compact(tt[1])
            assert(err == tt[2])
            assert(s:getEntry(s.offset).index == tt[3])
            assert(s:getEntry(s.offset).term == tt[4])
            assert(s.count == tt[5])
            s:close()
            s = ldbstorage.new("./testdb")
            assert(s:getEntry(s.offset).index == tt[3])
            assert(s:getEntry(s.offset).term == tt[4])
            assert(s.count == tt[5])
        end)
        s:close()
        s:delete_db()
        assert(ok, err)
    end
end

function TestStorageCreateSnapshot()
    local ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
    local cs = {1, 2, 3}
    local data = "data"
    local tests = {
        {4, raft.OK, makeSnapshot(data, 4, 4, cs)},
        {5, raft.OK, makeSnapshot(data, 5, 5, cs)}
    }

    for _, tt in ipairs(tests) do
        ldbstorage.delete_db("./testdb")
        local s = ldbstorage.new("./testdb", ents)
        local ok, err = pcall(function()
            local err, snap = s:create_snapshot(tt[1], cs, data)
            assert(err, tt[2])
            assert(snap.metadata.index, tt[3].metadata.index)
            assert(snap.metadata.term, tt[3].metadata.term)
            assert(snap.data, tt[3].data)
            local conf1 = snap.metadata.conf_state
            local conf2 = tt[3].metadata.conf_state
            lu.assertEquals(conf1.nodes, conf2.nodes)
        end)
        s:close()
        s:delete_db()
        assert(ok, err)
    end
end

function TestStorageAppend()
    local ents = {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
    local tests = {
        {
            {makeEntry(1, 1), makeEntry(2, 2)}, raft.OK,
            {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
        }, {
            {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}, raft.OK,
            {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}
        }, {
            {makeEntry(3, 3), makeEntry(4, 6), makeEntry(5, 6)}, raft.OK,
            {makeEntry(3, 3), makeEntry(4, 6), makeEntry(5, 6)}
        }, {
            {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)},
            raft.OK,
            {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)}
        },
        -- truncate incoming entries, truncate the existing entries and append
        {
            {makeEntry(2, 3), makeEntry(3, 3), makeEntry(4, 5)}, raft.OK,
            {makeEntry(3, 3), makeEntry(4, 5)}
        }, -- truncate the existing entries and append
        {{makeEntry(4, 5)}, raft.OK, {makeEntry(3, 3), makeEntry(4, 5)}},
        -- direct append
        {
            {makeEntry(6, 5)}, raft.OK,
            {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)}
        }
    }
    for _, tt in ipairs(tests) do
        ldbstorage.delete_db("./testdb")
        local s = ldbstorage.new("./testdb", ents)
        local ok, err = pcall(function()
            local err = s:append(tt[1], true)
            assert(err == tt[2])
            local entries = s:range(s.offset, s.offset + s.count)
            equal_entrys(entries, tt[3])
        end)
        s:close()
        s:delete_db()
        assert(ok, err)
    end
end

function TestStorageApplySnapshot()
    local cs = {1, 2, 3}
    local data = "data"

    local tests = {makeSnapshot(data, 4, 4, cs), makeSnapshot(data, 3, 3, cs)}

    ldbstorage.delete_db("./testdb")
    local s = ldbstorage.new("./testdb")

    local ok, err = pcall(function()
        -- Apply Snapshot successful
        local tt = tests[1]
        local err = s:apply_snapshot(tt)
        assert(err == raft.OK)

        -- Apply Snapshot fails due to ErrSnapOutOfDate
        tt = tests[2]
        local err = s:apply_snapshot(tt)
        assert(err == raft.ErrSnapOutOfDate)
    end)
    s:close()
    s:delete_db()

    assert(ok, err)
end

local runner = lu.LuaUnit.new()
runner:setOutputType("TAP")
runner:runSuite()
