#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
#include <progress.hpp>
using namespace raft;
using namespace raftpb;

typedef std::unique_ptr<Progress> ProgressPtr;
typedef std::unique_ptr<inflights> InflightsPtr;
typedef std::unique_ptr<Raft> RaftPtr;

MessagePtr make_message(uint64_t from, uint64_t to, MessageType type, uint64_t term = 0, bool reject = false) {
	MessagePtr msg = make_unique<Message>();
	msg->set_from(from);
	msg->set_to(to);
	msg->set_type(type);
	msg->set_term(term);
	msg->set_reject(reject);
	return std::move(msg);
}

ProgressPtr make_progress(ProgressStateType state, uint64_t match = 0, uint64_t next = 0, InflightsPtr &&ins = InflightsPtr(), uint64_t PendingSnapshot = 0) {
	ProgressPtr tt = std::make_unique<Progress>();
	tt->State = state;
	tt->Match = match;
	tt->Next = next;
	tt->PendingSnapshot = PendingSnapshot;
	tt->ins = std::move(ins);
	return std::move(tt);
}

Config newTestConfig(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, Storage *storage) {
	Config cfg;
	cfg.ID = id;
	cfg.peers = std::move(peers);
	cfg.ElectionTick = election;
	cfg.HeartbeatTick = heartbeat;
	cfg.Storage = storage;
	cfg.MaxSizePerMsg = noLimit;
	cfg.MaxInflightMsgs = 256;
	return std::move(cfg);
}

RaftPtr newTestRaft(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, Storage *storage) {
	auto config = newTestConfig(id, std::move(peers), election, heartbeat, storage);
	return std::make_unique<Raft>(config);
}

BOOST_AUTO_TEST_CASE(TestProgressBecomeProbe) {
	uint64_t match = 1;
	struct {
		ProgressPtr p;
		uint64_t wnext;
	} tests[] = {
		{make_progress(ProgressStateReplicate, match, 5, std::make_unique<inflights>(256)), 2},
		// snapshot finish
		{make_progress(ProgressStateSnapshot, match, 5, std::make_unique<inflights>(256), 10), 11},
		// snapshot failure
		{make_progress(ProgressStateSnapshot, match, 5, std::make_unique<inflights>(256)), 2},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		tt.p->becomeProbe();
		BOOST_REQUIRE_EQUAL(tt.p->State, ProgressStateProbe);
		BOOST_REQUIRE_EQUAL(tt.p->Match, match);
		BOOST_REQUIRE_EQUAL(tt.p->Next, tt.wnext);
	}
}

BOOST_AUTO_TEST_CASE(TestProgressBecomeReplicate) {
	auto p = make_progress(ProgressStateProbe, 1, 5, std::make_unique<inflights>(256));
	p->becomeReplicate();
	BOOST_REQUIRE_EQUAL(p->State, ProgressStateReplicate);
	BOOST_REQUIRE_EQUAL(p->Match, 1);
	BOOST_REQUIRE_EQUAL(p->Match + 1, p->Next);
}

BOOST_AUTO_TEST_CASE(TestProgressBecomeSnapshot) {
	auto p = make_progress(ProgressStateProbe, 1, 5, std::make_unique<inflights>(256));
	p->becomeSnapshot(10);
	BOOST_REQUIRE_EQUAL(p->State, ProgressStateSnapshot);
	BOOST_REQUIRE_EQUAL(p->Match, 1);
	BOOST_REQUIRE_EQUAL(p->PendingSnapshot, 10);
}

BOOST_AUTO_TEST_CASE(TestProgressUpdate) {
	uint64_t prevM = 3, prevN = 5;
	struct {
		uint64_t update;

		uint64_t wm;
		uint64_t wn;
		bool wok;
	} tests[] = {
		{prevM - 1, prevM, prevN, false},        // do not decrease match, next
		{prevM, prevM, prevN, false},            // do not decrease next
		{prevM + 1, prevM + 1, prevN, true},     // increase match, do not decrease next
		{prevM + 2, prevM + 2, prevN + 1, true}, // increase match, next
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto p = make_progress(ProgressStateProbe, prevM, prevN);
		bool ok = p->maybeUpdate(tt.update);
		BOOST_REQUIRE_EQUAL(ok, tt.wok);
		BOOST_REQUIRE_EQUAL(p->Match, tt.wm);
		BOOST_REQUIRE_EQUAL(p->Next, tt.wn);
	}
}

BOOST_AUTO_TEST_CASE(TestProgressMaybeDecr) {
	struct {
		ProgressStateType state;
		uint64_t m;
		uint64_t n;
		uint64_t rejected;
		uint64_t last;

		bool w;
		uint64_t wn;
	} tests[] = {
		// state replicate and rejected is not greater than match
		{ProgressStateReplicate, 5, 10, 5, 5, false, 10,},
		// state replicate and rejected is not greater than match
		{ProgressStateReplicate, 5, 10, 4, 4, false, 10,},
		// state replicate and rejected is greater than match
		// directly decrease to match+1
		{ProgressStateReplicate, 5, 10, 9, 9, true, 6,},
		// next-1 != rejected is always false
		{ProgressStateProbe, 0, 0, 0, 0, false, 0,},
		// next-1 != rejected is always false
		{ProgressStateProbe, 0, 10, 5, 5, false, 10,},
		// next>1 = decremented by 1
		{ProgressStateProbe, 0, 10, 9, 9, true, 9,},
		// next>1 = decremented by 1
		{ProgressStateProbe, 0, 2, 1, 1, true, 1,},
		// next<=1 = reset to 1
		{ProgressStateProbe, 0, 1, 0, 0, true, 1,},
		// decrease to min(rejected, last+1)
		{ProgressStateProbe, 0, 10, 9, 2, true, 3,},
		// rejected < 1, reset to 1
		{ProgressStateProbe, 0, 10, 9, 0, true, 1,},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto p = make_progress(tt.state, tt.m, tt.n);
		bool g = p->maybeDecrTo(tt.rejected, tt.last);
		BOOST_REQUIRE_EQUAL(g, tt.w);
		BOOST_REQUIRE_EQUAL(p->Match, tt.m);
		BOOST_REQUIRE_EQUAL(p->Next, tt.wn);
	}
}

BOOST_AUTO_TEST_CASE(TestProgressIsPaused) {
	struct {
		ProgressStateType state;
		bool paused;

		bool w;
	} tests[] = {
		{ProgressStateProbe, false, false},
		{ProgressStateProbe, true, true},
		{ProgressStateReplicate, false, false},
		{ProgressStateReplicate, true, false},
		{ProgressStateSnapshot, false, true},
		{ProgressStateSnapshot, true, true},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto p = make_progress(tt.state, 0, 0, std::make_unique<inflights>(256));
		p->Paused = tt.paused;
		bool g = p->IsPaused();
		BOOST_REQUIRE_EQUAL(g, tt.w);
	}
}

BOOST_AUTO_TEST_CASE(TestProgressResume) {
	auto p = make_progress(ProgressStateProbe, 0, 2);
	p->Paused = true;
	p->maybeDecrTo(1, 1);
	BOOST_REQUIRE_EQUAL(p->Paused, false);
	p->Paused = true;
	p->maybeUpdate(2);
	BOOST_REQUIRE_EQUAL(p->Paused, false);
}

BOOST_AUTO_TEST_CASE(TestProgressLeader) {
	MemoryStorage storage;
	RaftPtr r = newTestRaft(1, { 1, 2 }, 5, 1, &storage);
	r->becomeCandidate();
	r->becomeLeader();
	r->m_prs[2]->becomeReplicate();
	// Send proposals to r1. The first 5 entries should be appended to the log.
	Entry ent;
	ent.set_data("foo");
	auto propMsg = make_message(1, 1, MsgProp);
	*propMsg->mutable_entries()->Add() = ent;
	for (size_t i = 0; i < 5; i++) {
		auto &pr = r->m_prs[r->m_id];
		BOOST_REQUIRE_EQUAL(pr->State, ProgressStateReplicate);
		BOOST_REQUIRE_EQUAL(pr->Match, uint64_t(i + 1));
		BOOST_REQUIRE_EQUAL(pr->Next, pr->Match + 1);
		auto err = r->Step(*propMsg);
		BOOST_REQUIRE_EQUAL(err, OK);
	}
}
