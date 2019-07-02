#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
using namespace raft;
using namespace raftpb;

// TestMsgAppFlowControlFull ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
BOOST_AUTO_TEST_CASE(TestMsgAppFlowControlFull) {
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();

	auto &pr2 = r->prs[2];
	// force the progress to be in replicate state
	pr2->becomeReplicate();
	// fill in the inflights window
	for (int i = 0; i < r->maxInflight; i++) {
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
		auto ms = r->readMessages();
		BOOST_REQUIRE_EQUAL(ms.size(), 1);
	}

	// ensure 1
	BOOST_REQUIRE_EQUAL(pr2->ins->full(), true);

	// ensure 2
	for (int i = 0; i < 10; i++) {
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
		auto ms = r->readMessages();
		BOOST_REQUIRE_EQUAL(ms.size(), 0);
	}
}

// TestMsgAppFlowControlMoveForward ensures msgAppResp can move
// forward the sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
BOOST_AUTO_TEST_CASE(TestMsgAppFlowControlMoveForward) {
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();

	auto &pr2 = r->prs[2];
	// force the progress to be in replicate state
	pr2->becomeReplicate();
	// fill in the inflights window
	for (int i = 0; i < r->maxInflight; i++) {
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
		r->readMessages();
	}

	// 1 is noop, 2 is the first proposal we just sent.
	// so we start with 2.
	for (int tt = 2; tt < r->maxInflight; tt++) {
		// move forward the window
		r->Step(*make_message(2, 1, MsgAppResp, (uint64_t)tt));
		r->readMessages();

		// fill in the inflights window again
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
		auto ms = r->readMessages();
		BOOST_REQUIRE_EQUAL(ms.size(), 1);

		// ensure 1
		BOOST_REQUIRE_EQUAL(pr2->ins->full(), true);

		// ensure 2
		for (int i = 0; i < tt; i++) {
			r->Step(*make_message(2, 1, MsgAppResp, uint64_t(i)));
			BOOST_REQUIRE_EQUAL(pr2->ins->full(), true);
		}
	}
}

// TestMsgAppFlowControlRecvHeartbeat ensures a heartbeat response
// frees one slot if the window is full.
BOOST_AUTO_TEST_CASE(TestMsgAppFlowControlRecvHeartbeat) {
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();

	auto &pr2 = r->prs[2];
	// force the progress to be in replicate state
	pr2->becomeReplicate();
	// fill in the inflights window
	for (int i = 0; i < r->maxInflight; i++) {
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
		r->readMessages();
	}

	for (int tt = 1; tt < 5; tt++) {
		BOOST_REQUIRE_EQUAL(pr2->ins->full(), true);

		// recv tt msgHeartbeatResp and expect one free slot
		for (int i = 0; i < tt; i++) {
			r->Step(*make_message(2, 1, MsgHeartbeatResp));
			r->readMessages();
			BOOST_REQUIRE_EQUAL(pr2->ins->full(), false);
		}

		// one slot
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
		auto ms = r->readMessages();
		BOOST_REQUIRE_EQUAL(ms.size(), 1);

		// and just one slot
		for (int i = 0; i < 10; i++) {
			r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
			auto ms1 = r->readMessages();
			BOOST_REQUIRE_EQUAL(ms1.size(), 0);
		}

		// clear all pending messages.
		r->Step(*make_message(2, 1, MsgHeartbeatResp));
		r->readMessages();
	}
}
