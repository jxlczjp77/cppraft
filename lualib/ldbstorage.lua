local leveldb = require "lualeveldb"
local raft = require "lraft"
local raftpb = raft.pb
local uint64 = raft.uint64
local insert = table.insert
local ErrOK = raft.OK
local ErrCompacted = raft.ErrCompacted
local ErrAppendOutOfData = raft.ErrAppendOutOfData
local ErrUnavailable = raft.ErrUnavailable
local ErrSnapOutOfDate = raft.ErrSnapOutOfDate

local KEY_HARD_STATE = "key_hard_state"
local KEY_SNAPSHOT = "key_snapshot"
local KEY_OFFSET = "key_offset"
local KEY_COUNT = "key_count"

local ldbstorage = {}
ldbstorage.__index = ldbstorage

local function make_key(i) return string.format("e:%08x", i) end

function ldbstorage.new(path, ents)
    local opt = leveldb.options()
    opt.createIfMissing = true
    local ldb = leveldb.open(opt, path)
    local batch = ldb:batch(path .. "ldb")
    local m =
        setmetatable({ldb = ldb, dbpath = path, batch = batch}, ldbstorage)
    m.base = raft.ldbstorage(m)
    m.offset = tonumber(ldb:get(KEY_OFFSET)) or 0
    m.count = tonumber(ldb:get(KEY_COUNT)) or 0
    local snap = raftpb.snapshot()
    local snap_data = ldb:get(KEY_SNAPSHOT)
    if snap_data and #snap_data > 0 then
        assert(snap:parser(snap_data), "parser snapshot failed")
    end
    m.snap = snap
    if ents and #ents > 0 then
        assert(m.count == 0)
        m.offset = ents[1].index
        m.count = #ents
        for _, ent in ipairs(ents) do
            m.batch:put(make_key(ent.index), ent:serialize())
        end
        m.batch:put(KEY_COUNT, tostring(m.count))
        m.batch:put(KEY_OFFSET, tostring(m.offset))
    elseif m.count == 0 then
        m.batch:put(make_key(0), raftpb.entry():serialize())
        m.count = 1
        m.batch:put(KEY_COUNT, tostring(m.count))
        m.batch:put(KEY_OFFSET, tostring(m.offset))
    end
    m:flush()
    return m
end

function ldbstorage.delete_db(self_or_dbpath)
    local path
    if type(self_or_dbpath) == "string" then
        path = self_or_dbpath
    else
        path = self_or_dbpath.dbpath
    end
    os.execute("rm -rf " .. path)
end

function ldbstorage:close()
    if self.ldb then
        self:flush()
        self.batch:close()
        self.ldb:close()
        self.ldb = nil
        self.batch = nil
        self.offset = 0
        self.count = 0
    end
end

function ldbstorage:append(ents, flush)
    if #ents > 0 then
        local new_first = ents[1].index
        local new_last = new_first + #ents - 1
        local old_first = uint64(self:firstIndex())
        if new_last >= old_first then
            local start_pos = old_first > new_first and
                                  #(old_first - new_first + 1) or 1
            local offset = #(uint64(ents[start_pos].index) - self.offset)
            if offset > self.count then
                return ErrAppendOutOfData
            elseif offset < self.count then
                self:remove(self.offset + offset, self:lastIndex() + 1)
            end
            local new_count = offset
            for i = start_pos, #ents do
                local ent = ents[i]
                self.batch:put(make_key(ent.index), ent:serialize())
                new_count = new_count + 1
                i = i + 1
            end
            self.count = new_count
            self.batch:put(KEY_COUNT, tostring(new_count))
        end
    end
    if flush then self:flush() end
    return ErrOK
end

function ldbstorage:firstIndex() return self.offset + 1 end

function ldbstorage:lastIndex() return self.offset + self.count - 1 end

function ldbstorage:empty() return self.offset == 0 and self.count == 1 end

function ldbstorage:remove(s, e)
    local i = uint64(s)
    while i < e do
        self.batch:delete(make_key(#i))
        i = i + 1
    end
end

function ldbstorage:flush() self.ldb:write(self.batch) end

function ldbstorage:getEntry(i)
    local data = self.batch:get(make_key(i))
    assert(data, string.format("getEntry(%d) failed", i))
    local ent = raftpb.entry()
    assert(ent:parser(data))
    return ent
end

function ldbstorage:range(lo, hi, cb)
    if uint64(lo) < self.offset then lo = self.offset end
    if uint64(hi) > self:lastIndex() + 1 then hi = self:lastIndex() + 1 end
    if cb then
        local i = uint64(lo)
        while i < hi do
            cb(self:getEntry(#i))
            i = uint64(i) + 1
        end
        return
    end
    local ents = {}
    local i = uint64(lo)
    while i < hi do
        insert(ents, self:getEntry(#i))
        i = i + 1
    end
    return ents
end

-- Compact discards all log entries prior to compactIndex.
-- It is the application's responsibility to not attempt to compact an index
-- greater than raftLog.applied.
function ldbstorage:compact(compactIndex)
    local cidx = uint64(compactIndex)
    if cidx <= self.offset then return ErrCompacted end
    if cidx > self:lastIndex() then
        error(string.format("compact %s is out of bound lastindex(%s)",
                            compactIndex, self:lastIndex()))
    end
    self:remove(self.offset, compactIndex)
    self.count = #(self.count - (cidx - self.offset))
    self.offset = compactIndex
    self.batch:put(KEY_COUNT, tostring(self.count))
    self.batch:put(KEY_OFFSET, tostring(self.offset))
    return ErrOK
end

-- CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
-- can be used to reconstruct the state at that point.
-- If any configuration changes have been made since the last compaction,
-- the result of the last ApplyConfChange must be passed in.
function ldbstorage:create_snapshot(i, cs, data)
    local pre_snap_idx = self.snap.metadata.index
    if uint64(i) <= pre_snap_idx then
        return ErrSnapOutOfDate,
               string.format("snapshot %s <= pre snapshot index %s",
                             uint64(i), uint64(pre_snap_idx))
    end
    if uint64(i) > self:lastIndex() then
        error(string.format("snapshot %s is out of bound lastindex(%s)",
                            uint64(i), uint64(self:lastIndex())))
    end

    local snap = raftpb.snapshot()
    local meta = snap.metadata
    meta.index = i
    meta.term = self:getEntry(i).term
    if cs then
        local conf_state = meta.conf_state
        conf_state.nodes = cs.nodes
        conf_state.learners = cs.learners
    end
    snap.data = data
    return ErrOK, snap
end

function ldbstorage:apply_snapshot(sn)
    local data = sn:serialize()
    local meta = sn.metadata
    local new_index = meta.index
    local new_term = meta.term
    local old_index = self.snap.metadata.index
    if uint64(new_index) < old_index then return ErrSnapOutOfDate end
    if not self:empty() then
        local remove_count = new_index - self.offset
        self:remove(self.offset, new_index)
        self.count = self.count - remove_count
    end
    local dumy_entry = raftpb.entry(new_index, new_term)
    self.offset = new_index
    assert(self.count >= 1)
    self.snap:parser(data)
    self.batch:put(make_key(new_index), dumy_entry:serialize())
    self.batch:put(KEY_COUNT, tostring(self.count))
    self.batch:put(KEY_OFFSET, tostring(self.offset))
    self.batch:put(KEY_SNAPSHOT, data)
    return ErrOK
end

function ldbstorage:set_hardstate(st)
    if st then
        self.batch:put(KEY_HARD_STATE, st:serialize())
        self.hardstate = st
    end
    return ErrOK
end

function ldbstorage:range_entry_data(cb)
    local i = uint64(0)
    local hi = self:lastIndex() + 1
    while i < hi do
        cb(self.batch:get(make_key(#i)))
        i = uint64(i) + 1
    end
end

-----------------------------
-- callback
function ldbstorage:InitialState()
    local hs = raftpb.hardstate()
    local hs_data = self.batch:get(KEY_HARD_STATE)
    if hs_data then assert(hs:parser(hs_data)) end
    return hs, self.snap
end

function ldbstorage:Entries(lo, hi, max_size)
    if uint64(lo) <= self.offset then
        return ErrCompacted, {}
    elseif uint64(hi) > self:lastIndex() + 1 then
        return ErrUnavailable, {}
    end
    if self.count == 1 then -- 仅包含dumy entry
        return ErrUnavailable, {}
    end

    local ent = self:getEntry(lo)
    local ents = {ent}
    local size = ent:byte_size()
    local i = uint64(lo) + 1
    while i < hi do
        ent = self:getEntry(#i)
        size = size + ent:byte_size()
        if size > max_size then break end
        insert(ents, ent)
        i = i + 1
    end
    return ErrOK, ents
end

function ldbstorage:Term(i)
    local ii = uint64(i)
    if ii < self.offset then
        return ErrCompacted, 0
    elseif ii - self.offset >= self.count then
        return ErrUnavailable, 0
    end
    local ent = self:getEntry(i)
    return ErrOK, ent.term
end

function ldbstorage:LastIndex() return ErrOK, self:lastIndex() end

function ldbstorage:FirstIndex() return ErrOK, self:firstIndex() end

function ldbstorage:Snapshot() return self.snap end

return ldbstorage
