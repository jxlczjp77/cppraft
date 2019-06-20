package.path = [[..\..\..\..\test\?.lua]]
local lu = require "luaunit"
local raft = require "lraft"
local raftpb = raft.pb

function newTestConfig(id, peers, election, heartbeat, storage)
	local cfg = raft.config()
	cfg.ID = id
	cfg.peers = peers
	cfg.ElectionTick = election
	cfg.HeartbeatTick = heartbeat
	cfg.Storage = storage
	cfg.MaxSizePerMsg = raft.noLimit
	cfg.MaxInflightMsgs = 256
	return cfg
end

-- TestRawNodeStep ensures that RawNode.Step ignore local message.
function TestRawNodeStep()
	for msgt = raftpb.MessageType_MIN, raftpb.MessageType_MAX do
		local name = raftpb.MessageType_Name(msgt)
		local node = raft.rawnode()
		local s = raft.memorystorage()
		node:init(newTestConfig(1, {}, 10, 1, s), { 1 })
		local msg = raftpb.message()
		msg.type = msgt
		local err = node:step(msg)
		if raft.IsLocalMsg(err) then
			assert(err, raft.ErrStepLocalMsg)
		end
	end
end

-- TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
-- no goroutine in RawNode.

-- TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
-- send the given proposal and ConfChange to the underlying raft.
function TestRawNodeProposeAndConfChange()
	local node = raft.rawnode()
	local s = raft.memorystorage()
	node:init(newTestConfig(1, {}, 10, 1, s), { 1 })
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
		local ok, li = s:lastindex()
		assert(ok, raft.OK)
		if li >= 4 then
			lastIndex = li
			break
		end
	end

	local ok, entries = s:entries(lastIndex - 1, lastIndex + 1, raft.noLimit)
	assert(ok == raft.OK)
	assert(entries:size(), 2)
	assert(entries:at(0):data() == "somedata")
	assert(entries:at(1):type() == raftpb.EntryConfChange)
	assert(entries:at(1):data() == ccdata)
end

-- TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
-- not affect the later propose to add new node.
function TestRawNodeProposeAddDuplicateNode()
	local node = raft.rawnode()
	local s = raft.memorystorage()
	node:init(newTestConfig(1, {}, 10, 1, s), { 1 })
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

	local proposeConfChangeAndApply = function(cc)
		node:propose_confchange(cc)
		local rd = node:ready()
		s:append(rd.entries)
		for _, entry in rd.committed_entries:ipairs() do
			if entry:type() == raftpb.EntryConfChange then
				local cc = raftpb.confchange()
				cc:parser(entry:data())
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
	proposeConfChangeAndApply(cc1);

	-- the new node join should be ok
	local cc2 = raftpb.confchange()
	cc2.type = raftpb.ConfChangeAddNode
	cc1.nodeid = 2
	local ccdata2 = cc2:serialize()
	proposeConfChangeAndApply(cc2)

	local err, lastindex = s:lastindex()
	assert(err == raft.OK)

	-- the last three entries should be: ConfChange cc1, cc1, cc2
	local err, entries = s:entries(lastindex - 2, lastindex + 1, raft.noLimit)
	assert(err == raft.OK)
	assert(entries:size(), 3)
	assert(entries:at(0):data() == ccdata1)
	assert(entries:at(2):data() == ccdata2)
end

-- TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
-- to the underlying raft. It also ensures that ReadState can be read out.
function TestRawNodeReadIndex()
	local msgs = {}
	local appendStep = function(m)
		table.insert(msgs, raftpb.message(m))
		return raft.OK
	end
	local wrs = { raft.readstate(1, "somedata") }

	local s = raft.memorystorage()
	local node = raft.rawnode()
	node:init(newTestConfig(1, {}, 10, 1, s), { 1 })
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
	assert(msgs[1]:entry(0):data() == wrequestCtx)
end

local runner = lu.LuaUnit.new()
runner:setOutputType("TAP")
runner:runSuite()
