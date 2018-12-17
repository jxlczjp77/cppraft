#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
#include <raft/rawnode.hpp>
#include "utils.hpp"
using namespace raft;
using namespace raftpb;
Config newTestConfig(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage);

// TestRawNodeStep ensures that RawNode.Step ignore local message.
BOOST_AUTO_TEST_CASE(TestRawNodeStep) {
	for (int i = MessageType_MIN; i <= MessageType_MAX; i++) {
		auto msgn = MessageType_Name((MessageType)i);
		auto s = std::make_shared<MemoryStorage>();
		RawNode rawNode;
		rawNode.Init(newTestConfig(1, {}, 10, 1, s), { {1} });
		auto msgt = MessageType(i);
		Message msg;
		msg.set_type(msgt);
		auto err = rawNode.Step(msg);
		// LocalMsg should be ignored.
		if (IsLocalMsg(msgt)) {
			BOOST_REQUIRE_EQUAL(err, ErrStepLocalMsg);
		}
	}
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
BOOST_AUTO_TEST_CASE(TestRawNodeProposeAndConfChange) {
	auto s = std::make_shared<MemoryStorage>();
	RawNode rawNode;
	rawNode.Init(newTestConfig(1, {}, 10, 1, s), { {1} });
	auto rd = rawNode.Ready();
	s->Append(rd.Entries);
	rawNode.Advance(rd);

	auto d = rawNode.Ready();
	BOOST_REQUIRE_EQUAL(d.MustSync, false);
	BOOST_REQUIRE_EQUAL(IsEmptyHardState(d.HardState), true);
	BOOST_REQUIRE_EQUAL(d.Entries.empty(), true);

	rawNode.Campaign(); 
	bool proposed = false;
	uint64_t lastIndex;
	string ccdata;
	for (;;) {
		rd = rawNode.Ready();
		s->Append(rd.Entries);
		// Once we are the leader, propose a command and a ConfChange.
		if (!proposed && rd.SoftState.Lead == rawNode.raft->id) {
			rawNode.Propose("somedata");

			ConfChange cc;
			cc.set_type(ConfChangeAddNode);
			cc.set_nodeid(1);
			ccdata = cc.SerializeAsString();
			rawNode.ProposeConfChange(cc);

			proposed = true;
		}
		rawNode.Advance(rd);

		// Exit when we have four entries: one ConfChange, one no-op for the election,
		// our proposed command and proposed ConfChange.
		auto err = s->LastIndex(lastIndex);
		BOOST_REQUIRE_EQUAL(err, OK);
		if (lastIndex >= 4) {
			break;
		}
	}

	vector<Entry> entries;
	auto err = s->Entries(lastIndex - 1, lastIndex + 1, noLimit, entries);
	BOOST_REQUIRE_EQUAL(err, OK);
	BOOST_REQUIRE_EQUAL(entries.size(), 2);
	BOOST_REQUIRE_EQUAL(entries[0].data(), "somedata");
	BOOST_REQUIRE_EQUAL(entries[1].type(), EntryConfChange);
	BOOST_REQUIRE_EQUAL(entries[1].data(), ccdata);
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
BOOST_AUTO_TEST_CASE(TestRawNodeProposeAddDuplicateNode) {
	auto s = std::make_shared<MemoryStorage>();
	RawNode rawNode;
	rawNode.Init(newTestConfig(1, {}, 10, 1, s), { {1} });
	auto rd = rawNode.Ready();
	s->Append(rd.Entries);
	rawNode.Advance(rd);

	rawNode.Campaign();
	for (;;) {
		rd = rawNode.Ready();
		s->Append(rd.Entries);
		if (rd.SoftState.Lead == rawNode.raft->id) {
			rawNode.Advance(rd);
			break;
		}
		rawNode.Advance(rd);
	}

	auto proposeConfChangeAndApply = [&](const ConfChange &cc) {
		rawNode.ProposeConfChange(cc);
		auto rd = rawNode.Ready();
		s->Append(rd.Entries);
		for (auto &entry : rd.CommittedEntries) {
			if (entry.type() == EntryConfChange) {
				ConfChange cc;
				cc.ParseFromString(entry.data());
				rawNode.ApplyConfChange(cc);
			}
		}
		rawNode.Advance(rd);
	};

	ConfChange cc1;
	cc1.set_type(ConfChangeAddNode);
	cc1.set_nodeid(1);
	auto ccdata1 = cc1.SerializeAsString();
	proposeConfChangeAndApply(cc1);

	// try to add the same node again
	proposeConfChangeAndApply(cc1);

	// the new node join should be ok
	ConfChange cc2;
	cc2.set_type(ConfChangeAddNode);
	cc2.set_nodeid(2);
	auto ccdata2 = cc2.SerializeAsString();
	proposeConfChangeAndApply(cc2);

	uint64_t lastIndex;
	auto err = s->LastIndex(lastIndex);
	BOOST_REQUIRE_EQUAL(err, OK);

	// the last three entries should be: ConfChange cc1, cc1, cc2
	vector<Entry> entries;
	err = s->Entries(lastIndex - 2, lastIndex + 1, noLimit, entries);
	BOOST_REQUIRE_EQUAL(err, OK);
	BOOST_REQUIRE_EQUAL(entries.size(), 3);
	BOOST_REQUIRE_EQUAL(entries[0].data(), ccdata1);
	BOOST_REQUIRE_EQUAL(entries[2].data(), ccdata2);
}
