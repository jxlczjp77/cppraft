local lu = require "luaunit"
local raft = require "lraft"
local ldbstorage = require "ldbstorage"
local raftpb = raft.pb
raft.default_logger().level = raft.LOG_OFF

local function newTestConfig(id, peers, election, heartbeat, storage)
    local cfg = raft.config()
    cfg.ID = id
    cfg.peers = peers
    cfg.ElectionTick = election
    cfg.HeartbeatTick = heartbeat
    if type(storage) == "table" then
        cfg.Storage = storage.base
    else
        cfg.Storage = storage
    end
    cfg.MaxSizePerMsg = raft.noLimit
    cfg.MaxInflightMsgs = 256
    return cfg
end

local function makeEntry(index, term, data, type)
    return raftpb.entry(index, term, data, type)
end

local function make_message(from, to, type, index, term, reject, ents, logterm,
                            commit, rejectHint)
    local msg = raftpb.message()
    msg.from = from
    msg.to = to
    msg.type = type
    msg.term = term
    msg.reject = reject
    msg.index = index
    msg.logterm = logterm
    msg.commit = commit
    msg.rejecthint = rejectHint
    msg.entries = ents
    return msg
end

local function equal_readstate(rd1, rd2)
    assert(#rd1.entries == #rd2.entries)
    for i = 1, #rd1.entries do
        local a, b = rd1.entries[i], rd2.entries[i]
        assert(a.index == b.index)
        assert(a.term == b.term)
        assert(a.data == b.data)
        assert(a.type == b.type)
    end
    assert(#rd1.committed_entries == #rd2.committed_entries)
    for i = 1, #rd1.committed_entries do
        local a, b = rd1.committed_entries[i], rd2.committed_entries[i]
        assert(a.index == b.index)
        assert(a.term == b.term)
        assert(a.data == b.data)
        assert(a.type == b.type)
    end
    if rd1.hardstate then
        assert(rd1.hardstate.term == rd2.hardstate.term)
        assert(rd1.hardstate.vote == rd2.hardstate.vote)
        assert(rd1.hardstate.commit == rd2.hardstate.commit)
    else
        assert(not rd2.hardstate)
    end
    assert(rd1.mustsync == rd2.mustsync)
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

local function assert_equal_storage(storage1, storage2, cb)
    local ok, err = pcall(function()
        local _, first1 = storage1:FirstIndex()
        local _, first2 = storage2:FirstIndex()
        assert(first1 == first2)

        local _, last1 = storage1:LastIndex()
        local _, last2 = storage2:LastIndex()
        assert(last1 == last2)

        local _, ents1 = storage1:Entries(first1, last1 + 1, raft.noLimit)
        local _, ents2 = storage2:Entries(first2, last2 + 1, raft.noLimit)
        equal_entrys(ents1, ents2)
    end)
    cb()
    assert(ok, err)
end

-- TestRawNodeStep ensures that RawNode.Step ignore local message.
function TestRawNodeStep()
    for msgt = raftpb.MessageType_MIN, raftpb.MessageType_MAX do
        local name = raftpb.MessageType_Name(msgt)
        local node = raft.rawnode()
        local s = raft.memorystorage()
        node:init(newTestConfig(1, {}, 10, 1, s), {1})
        local msg = raftpb.message()
        msg.type = msgt
        local err = node:step(msg)
        if raft.IsLocalMsg(err) then assert(err, raft.ErrStepLocalMsg) end
    end
end

-- TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
-- no goroutine in RawNode.

-- TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
-- send the given proposal and ConfChange to the underlying raft.
function TestRawNodeProposeAndConfChange()
    local test_func = function(s)
        local node = raft.rawnode()
        node:init(newTestConfig(1, {}, 10, 1, s), {1})
        local rd = node:ready()
        s:append(rd.entries)
        node:advance(rd)

        rd = node:ready()
        assert(not rd.mustsync)
        assert(raft.IsEmptyHardState(rd.hardstate))
        assert(rd.entries:empty())

        node:campaign()
        local proposed = false
        local ccdata, lastIndex
        while true do
            rd = node:ready()
            s:append(rd.entries)

            -- Once we are the leader, propose a command and a ConfChange.
            if not proposed and rd.softstate.Lead == node.id then
                node:propose("somedata")

                local cc = raftpb.confchange()
                cc.type = raftpb.ConfChangeAddNode
                cc.nodeid = 1
                ccdata = cc:serialize()
                node:propose_confchange(cc)

                proposed = true
            end

            node:advance(rd)

            -- Exit when we have four entries: one ConfChange, one no-op for the election,
            -- our proposed command and proposed ConfChange.
            local ok, li = s:LastIndex()
            assert(ok, raft.OK)
            if li >= 4 then
                lastIndex = li
                break
            end
        end

        local ok, entries =
            s:Entries(lastIndex - 1, lastIndex + 1, raft.noLimit)
        assert(ok == raft.OK)
        assert(#entries, 2)
        assert(entries[1].data == "somedata")
        assert(entries[2].type == raftpb.EntryConfChange)
        assert(entries[2].data == ccdata)
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

-- TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
-- not affect the later propose to add new node.
function TestRawNodeProposeAddDuplicateNode()
    local test_func = function(s)
        local node = raft.rawnode()
        node:init(newTestConfig(1, {}, 10, 1, s), {1})
        local rd = node:ready()
        s:append(rd.entries)
        node:advance(rd)

        node:campaign()
        while true do
            rd = node:ready()
            s:append(rd.entries)
            if rd.softstate.Lead == node.id then
                node:advance(rd)
                break
            end
            node:advance(rd)
        end

        local proposeConfChangeAndApply =
            function(cc)
                node:propose_confchange(cc)
                local rd = node:ready()
                s:append(rd.entries)
                for _, entry in rd.committed_entries:ipairs() do
                    if entry.type == raftpb.EntryConfChange then
                        local cc = raftpb.confchange()
                        cc:parser(entry.data)
                        node:apply_confchange(cc)
                    end
                end
                node:advance(rd)
            end

        local cc1 = raftpb.confchange()
        cc1.type = raftpb.ConfChangeAddNode
        cc1.nodeid = 1
        local ccdata1 = cc1:serialize()
        proposeConfChangeAndApply(cc1)

        -- try to add the same node again
        proposeConfChangeAndApply(cc1)

        -- the new node join should be ok
        local cc2 = raftpb.confchange()
        cc2.type = raftpb.ConfChangeAddNode
        cc1.nodeid = 2
        local ccdata2 = cc2:serialize()
        proposeConfChangeAndApply(cc2)

        local err, lastindex = s:LastIndex()
        assert(err == raft.OK)

        -- the last three entries should be: ConfChange cc1, cc1, cc2
        local err, entries = s:Entries(lastindex - 2, lastindex + 1,
                                       raft.noLimit)
        assert(err == raft.OK)
        assert(#entries, 3)
        assert(entries[1].data == ccdata1)
        assert(entries[3].data == ccdata2)
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

-- TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
-- to the underlying raft. It also ensures that ReadState can be read out.
function TestRawNodeReadIndex()
    local test_func = function(s)
        local msgs = {}
        local appendStep = function(m)
            table.insert(msgs, raftpb.message(m))
            return raft.OK
        end
        local wrs = {raft.readstate(1, "somedata")}

        local node = raft.rawnode()
        node:init(newTestConfig(1, {}, 10, 1, s), {1})
        node.read_states = wrs
        -- ensure the ReadStates can be read out
        assert(node:has_ready() == true)
        local rd = node:ready()
        for i, a in rd.readstates:ipairs() do
            local b = wrs[i]
            assert(a.index == b.index)
        end
        s:append(rd.entries)
        node:advance(rd)
        -- ensure raft.readStates is reset after advance
        assert(node.read_states:empty())

        local wrequestCtx = "somedata2"
        node:campaign()
        while true do
            rd = node:ready()
            s:append(rd.entries)

            if rd.softstate.Lead == node.id then
                node:advance(rd)

                -- Once we are the leader, issue a ReadIndex request
                node.step_func = appendStep
                node:readindex(wrequestCtx)
                break
            end
            node:advance(rd)
        end
        -- ensure that MsgReadIndex message is sent to the underlying raft
        assert(#msgs == 1)
        assert(msgs[1].type == raftpb.MsgReadIndex)
        assert(msgs[1][1].data == wrequestCtx)
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

-- TestBlockProposal from node_test.go has no equivalent in rawNode because there is
-- no leader check in RawNode.

-- TestNodeTick from node_test.go has no equivalent in rawNode because
-- it reaches into the raft object which is not exposed.

-- TestNodeStop from node_test.go has no equivalent in rawNode because there is
-- no goroutine in RawNode.

-- TestRawNodeStart ensures that a node can be started correctly. The node should
-- start with correct configuration change entries, and can accept and commit
-- proposals.
function TestRawNodeStart()
    local cc = raftpb.confchange()
    cc.type = raftpb.ConfChangeAddNode
    cc.nodeid = 1
    local ccdata = cc:serialize()
    local makeHardState = function(term, commit, vote)
        return raftpb.hardstate(term, commit, vote)
    end
    local ent1 = makeEntry(1, 1, ccdata, raftpb.EntryConfChange)
    local ent2 = makeEntry(3, 2, "foo")
    local wants = {
        {
            hardstate = makeHardState(1, 1, 0),
            entries = {ent1},
            committed_entries = {ent1},
            mustsync = true
        }, {
            hardstate = makeHardState(2, 3, 1),
            entries = {ent2},
            committed_entries = {ent2},
            mustsync = true
        }
    }

    local test_func = function(storage)
        local node = raft.rawnode()
        node:init(newTestConfig(1, {}, 10, 1, storage), {1})
        local rd = node:ready()

        equal_readstate(rd, wants[1])
        storage:append(rd.entries)
        node:advance(rd)

        node:campaign()
        rd = node:ready()
        storage:append(rd.entries)
        node:advance(rd)

        node:propose("foo")
        rd = node:ready()
        equal_readstate(rd, wants[2])
        storage:append(rd.entries)
        node:advance(rd)
        assert(not node:has_ready())
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

function TestRawNodeRestart()
    local entries = {makeEntry(1, 1), makeEntry(2, 1, "foo")}
    local st = raftpb.hardstate(1, 1)

    local want = {
        {
            hardstate = nil,
            entries = {},
            -- commit up to commit index in st
            committed_entries = {table.unpack(entries, 1, st.commit)},
            mustsync = false
        }
    }

    local test_func = function(s)
        s:set_hardstate(st)
        s:append(entries)
        local node = raft.rawnode()
        node:init(newTestConfig(1, {}, 10, 1, s), {})
        local rd = node:ready()

        equal_readstate(rd, want[1])
        node:advance(rd)
        assert(not node:has_ready())
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

function TestRawNodeRestartFromSnapshot()
    local snap = raftpb.snapshot()
    snap.metadata.index = 2
    snap.metadata.term = 1
    snap.metadata.conf_state.nodes = {1, 2}
    local entries = {makeEntry(3, 1, "foo")}
    local st = raftpb.hardstate(1, 3)

    local want = {
        {
            hardstate = nil,
            entries = {},
            -- commit up to commit index in st
            committed_entries = entries,
            mustsync = false
        }
    }

    local test_func = function(s)
        s:set_hardstate(st)
        s:apply_snapshot(snap)
        s:append(entries)
        local node = raft.rawnode()
        node:init(newTestConfig(1, {}, 10, 1, s), {})
        local rd = node:ready()
        equal_readstate(rd, want[1])
        node:advance(rd)
        assert(not node:has_ready())
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

-- TestRawNodeCommitPaginationAfterRestart is the RawNode version of
-- TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
-- Raft group would forget to apply entries:
--
-- - node learns that index 11 is committed
-- - nextEnts returns index 1..10 in CommittedEntries (but index 10 already
--   exceeds maxBytes), which isn't noticed internally by Raft
-- - Commit index gets bumped to 10
-- - the node persists the HardState, but crashes before applying the entries
-- - upon restart, the storage returns the same entries, but `slice` takes a
--   different code path and removes the last entry.
-- - Raft does not emit a HardState, but when the app calls Advance(), it bumps
--   its internal applied index cursor to 10 (when it should be 9)
-- - the next Ready asks the app to apply index 11 (omitting index 10), losing a
--    write.
function TestRawNodeCommitPaginationAfterRestart()
    local test_func = function(s)
        local persistedHardState = raftpb.hardstate(1, 10, 1)

        s:set_hardstate(persistedHardState)
        local size = 0
        local ents = {}
        for i = 1, 10 do
            local ent = makeEntry(i, 1, "a", raftpb.EntryNormal)
            table.insert(ents, ent)
            size = size + ent:byte_size()
        end
        s:append(ents)

        local cfg = newTestConfig(1, {1}, 10, 1, s)
        -- Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
        -- not be included in the initial rd.CommittedEntries. However, our storage will ignore
        -- this and *will* return it (which is how the Commit index ended up being 10 initially).
        cfg.MaxSizePerMsg = size - ents[#ents]:byte_size() - 1
        s:append({makeEntry(11, 1, "boom", raft.EntryNormal)})

        local node = raft.rawnode()
        node:init(cfg, {1})

        local highestApplied = 0
        while highestApplied ~= 11 do
            local rd = node:ready()
            local n = #rd.committed_entries
            assert(n ~= 0)
            local next = rd.committed_entries[1].index
            assert((highestApplied ~= 0 and highestApplied + 1 ~= next) == false)
            highestApplied = rd.committed_entries[n].index
            node:advance(rd)
            local msg = make_message(1, 1, raftpb.MsgHeartbeat, 0, 1, false, {},
                                     0, 11)
            node:step(msg)
        end
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

-- TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
-- partitioned from a quorum of nodes. It verifies that the leader's log is
-- protected from unbounded growth even as new entries continue to be proposed.
-- This protection is provided by the MaxUncommittedEntriesSize configuration.
function TestRawNodeBoundedLogGrowthWithPartition()
    local maxEntries = 16
    local data = "testdata"
    local testEntry = makeEntry(0, 0, data)
    local maxEntrySize = maxEntries * testEntry:payload_size()

    local test_func = function(s)
        local cfg = newTestConfig(1, {1}, 10, 1, s)
        cfg.MaxUncommittedEntriesSize = maxEntrySize
        local node = raft.rawnode()
        node:init(cfg, {1})
        local rd = node:ready()
        s:append(rd.entries)
        node:advance(rd)

        -- Become the leader.
        node:campaign()
        while true do
            rd = node:ready()
            s:append(rd.entries)
            if rd.softstate.Lead == node.id then
                node:advance(rd)
                break
            end
            node:advance(rd)
        end

        -- Simulate a network partition while we make our proposals by never
        -- committing anything. These proposals should not cause the leader's
        -- log to grow indefinitely.
        for i = 1, 1024 do node:propose(data) end

        -- Check the size of leader's uncommitted log tail. It should not exceed the
        -- MaxUncommittedEntriesSize limit.
        local checkUncommitted = function(exp)
            assert(node.uncommitted_size == exp)
        end
        checkUncommitted(maxEntrySize)

        -- Recover from the partition. The uncommitted tail of the Raft log should
        -- disappear as entries are committed.
        rd = node:ready()
        assert(#rd.committed_entries == maxEntries)
        s:append(rd.entries)
        node:advance(rd)
        checkUncommitted(0)
    end

    local ms = raft.memorystorage()
    test_func(ms)

    ldbstorage.delete_db("./testdb")
    local ls = ldbstorage.new("./testdb")
    local ok, err = pcall(test_func, ls)
    assert_equal_storage(ms, ls, function()
        ls:close()
        ls:delete_db()
        assert(ok, err)
    end)
end

local runner = lu.LuaUnit.new()
runner:setOutputType("TAP")
runner:runSuite()
