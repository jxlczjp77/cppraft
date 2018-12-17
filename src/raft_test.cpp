#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
#include "utils.hpp"
#include <boost/throw_exception.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/random/linear_congruential.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
#include <boost/format.hpp>
// #define DISABLE_RAFT_TEST

using namespace raft;
using namespace raftpb;
typedef boost::minstd_rand base_generator_type;

typedef std::unique_ptr<Progress> ProgressPtr;
typedef std::unique_ptr<inflights> InflightsPtr;
typedef std::unique_ptr<Raft> RaftPtr;

void preVoteConfig(Config &c);

vector<MessagePtr> msg_move(initializer_list<MessagePtr> msg) {
	vector<MessagePtr> tmp;
	for (auto it = msg.begin(); it != msg.end(); ++it) tmp.emplace_back(std::move(*const_cast<MessagePtr*>(it)));
	return std::move(tmp);
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

Config newTestConfig(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage) {
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

template<class TRaftPtr>
vector<MessagePtr> readMessages(TRaftPtr &r) {
	return std::move(r->msgs);
}

stateMachinePtr nopStepper = std::make_unique<blackHole>();

networkptr newNetworkWithConfig(void(*configFunc)(Config &), const vector<stateMachinePtr> &peers);
template<class TRaftPtr> void setRandomizedElectionTimeout(TRaftPtr &r, int v);

TestRaftPtr newTestRaft(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage, Logger *Logger) {
	auto config = newTestConfig(id, std::move(peers), election, heartbeat, storage);
	config.Logger = Logger;
	auto r = std::make_shared<testRaft>();
	r->Init(std::move(config));
	return r;
}

TestRaftPtr newTestLearnerRaft(uint64_t id, vector<uint64_t> &&peers, vector<uint64_t> &&learners, int election, int heartbeat, StoragePtr storage) {
	auto cfg = newTestConfig(id, std::move(peers), election, heartbeat, storage);
	cfg.learners = learners;
	auto r = std::make_shared<testRaft>();
	r->Init(std::move(cfg));
	return r;
}
#ifndef DISABLE_RAFT_TEST
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
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	r->prs[2]->becomeReplicate();
	// Send proposals to r1. The first 5 entries should be appended to the log.
	auto propMsg = make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0, "foo") });
	for (size_t i = 0; i < 5; i++) {
		auto &pr = r->prs[r->id];
		BOOST_REQUIRE_EQUAL(pr->State, ProgressStateReplicate);
		BOOST_REQUIRE_EQUAL(pr->Match, uint64_t(i + 1));
		BOOST_REQUIRE_EQUAL(pr->Next, pr->Match + 1);
		auto err = r->Step(*propMsg);
		BOOST_REQUIRE_EQUAL(err, OK);
	}
}

// TestProgressResumeByHeartbeatResp ensures raft.heartbeat reset progress.paused by heartbeat response.
BOOST_AUTO_TEST_CASE(TestProgressResumeByHeartbeatResp) {
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();

	r->prs[2]->Paused = true;

	r->Step(*make_message(1, 1, MsgBeat));
	BOOST_REQUIRE_EQUAL(r->prs[2]->Paused, true);

	r->prs[2]->becomeReplicate();
	r->Step(*make_message(2, 1, MsgHeartbeatResp));
	BOOST_REQUIRE_EQUAL(r->prs[2]->Paused, false);
}

BOOST_AUTO_TEST_CASE(TestProgressPaused) {
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();

	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));

	auto ms = readMessages(r);
	BOOST_REQUIRE_EQUAL(ms.size(), 1);
}

BOOST_AUTO_TEST_CASE(TestProgressFlowControl) {
	auto cfg = newTestConfig(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	cfg.MaxInflightMsgs = 3;
	cfg.MaxSizePerMsg = 2048;
	RaftPtr r = std::make_unique<Raft>();
	r->Init(std::move(cfg));
	r->becomeCandidate();
	r->becomeLeader();

	// Throw away all the messages relating to the initial election.
	readMessages(r);

	// While node 2 is in probe state, propose a bunch of entries.
	r->prs[2]->becomeProbe();
	string blob(1000, 'a');
	for (int i = 0; i < 10; i++) {
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, string(blob)) }));
	}

	auto ms = readMessages(r);
	// First append has two entries: the empty entry to confirm the
	// election, and the first proposal (only one proposal gets sent
	// because we're in probe state).
	BOOST_REQUIRE_EQUAL(ms.size(), 1);
	BOOST_REQUIRE_EQUAL(ms[0]->type(), MsgApp);
	BOOST_REQUIRE_EQUAL(ms[0]->entries_size(), 2);
	BOOST_REQUIRE_EQUAL(ms[0]->entries(0).data().size(), 0);
	BOOST_REQUIRE_EQUAL(ms[0]->entries(1).data().size(), 1000);

	// When this append is acked, we change to replicate state and can
	// send multiple messages at once.
	r->Step(*make_message(2, 1, MsgAppResp, ms[0]->entries(1).index()));
	ms = readMessages(r);
	BOOST_REQUIRE_EQUAL(ms.size(), 3);
	for (auto &m : ms) {
		BOOST_REQUIRE_EQUAL(m->type(), MsgApp);
		BOOST_REQUIRE_EQUAL(m->entries_size(), 2);
	}

	// Ack all three of those messages together and get the last two
	// messages (containing three entries).
	r->Step(*make_message(2, 1, MsgAppResp, ms[2]->entries(1).index()));
	ms = readMessages(r);
	BOOST_REQUIRE_EQUAL(ms.size(), 2);
	for (auto &m : ms) {
		BOOST_REQUIRE_EQUAL(m->type(), MsgApp);
	}
	BOOST_REQUIRE_EQUAL(ms[0]->entries_size(), 2);
	BOOST_REQUIRE_EQUAL(ms[1]->entries_size(), 1);
}

BOOST_AUTO_TEST_CASE(TestUncommittedEntryLimit) {
	// Use a relatively large number of entries here to prevent regression of a
	// bug which computed the size before it was fixed. This test would fail
	// with the bug, either because we'd get dropped proposals earlier than we
	// expect them, or because the final tally ends up nonzero. (At the time of
	// writing, the former).
	const int maxEntries = 1024;
	Entry testEntry;
	testEntry.set_data("testdata");
	const int maxEntrySize = maxEntries * (int)PayloadSize(testEntry);
	auto cfg = newTestConfig(1, { 1, 2, 3 }, 5, 1, std::make_shared<MemoryStorage>());
	cfg.MaxUncommittedEntriesSize = uint64_t(maxEntrySize);
	cfg.MaxInflightMsgs = 2 * 1024; // avoid interference
	RaftPtr r = std::make_unique<Raft>();
	r->Init(std::move(cfg));
	r->becomeCandidate();
	r->becomeLeader();
	BOOST_REQUIRE_EQUAL(r->uncommittedSize, 0);

	// Set the two followers to the replicate state. Commit to tail of log.
	const int numFollowers = 2;
	r->prs[2]->becomeReplicate();
	r->prs[3]->becomeReplicate();
	r->uncommittedSize = 0;

	// Send proposals to r1. The first 5 entries should be appended to the log.
	auto propMsg = make_message(1, 1, MsgProp, 0, 0, false, { testEntry });
	vector<Entry> propEnts(maxEntries);
	for (int i = 0; i < maxEntries; i++) {
		auto err = r->Step(*propMsg);
		BOOST_REQUIRE_EQUAL(err, OK);
		propEnts[i] = testEntry;
	}

	// Send one more proposal to r1. It should be rejected.
	auto err = r->Step(*propMsg);
	BOOST_REQUIRE_EQUAL(err, ErrProposalDropped);

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
	auto ms = readMessages(r);
	BOOST_REQUIRE_EQUAL(ms.size(), maxEntries * numFollowers);
	r->reduceUncommittedSize(propEnts);
	BOOST_REQUIRE_EQUAL(r->uncommittedSize, 0);

	// Send a single large proposal to r1. Should be accepted even though it
	// pushes us above the limit because we were beneath it before the proposal.
	propEnts.resize(2 * maxEntries);
	for (auto &ent : propEnts) {
		ent = testEntry;
	}
	auto propMsgLarge = make_message(1, 1, MsgProp, 0, 0, false, std::move(propEnts));
	err = r->Step(*propMsgLarge);
	BOOST_REQUIRE_EQUAL(err, OK);

	// Send one more proposal to r1. It should be rejected, again.
	err = r->Step(*propMsg);
	BOOST_REQUIRE_EQUAL(err, ErrProposalDropped);

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
	ms = readMessages(r);
	BOOST_REQUIRE_EQUAL(ms.size(), 1 * numFollowers);
	r->reduceUncommittedSize(propEnts);
	BOOST_REQUIRE_EQUAL(r->uncommittedSize, 0);
}
void testLeaderElection(bool preVote);
BOOST_AUTO_TEST_CASE(TestLeaderElection) {
	testLeaderElection(false);
}

BOOST_AUTO_TEST_CASE(TestLeaderElectionPreVote) {
	testLeaderElection(true);
}

// TestLearnerElectionTimeout verfies that the leader should not start election even
// when times out.
BOOST_AUTO_TEST_CASE(TestLearnerElectionTimeout) {
	auto n1 = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);

	// n2 is learner. Learner should not start election even when times out.
	setRandomizedElectionTimeout(n2, n2->electionTimeout);
	for (size_t i = 0; i < n2->electionTimeout; i++) {
		n2->tick();
	}

	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
BOOST_AUTO_TEST_CASE(TestLearnerPromotion) {
	auto n1 = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);

	auto nt = newNetwork({ n1, n2 });

	BOOST_REQUIRE_NE(n1->state, StateLeader);

	// n1 should become leader
	setRandomizedElectionTimeout(n1, n1->electionTimeout);
	for (size_t i = 0; i < n1->electionTimeout; i++) {
		n1->tick();
	}

	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);

	nt->send(make_message(1, 1, MsgBeat));

	n1->addNode(2);
	n2->addNode(2);
	BOOST_REQUIRE_EQUAL(n2->isLearner, false);

	// n2 start election, should become leader
	setRandomizedElectionTimeout(n2, n2->electionTimeout);
	for (size_t i = 0; i < n2->electionTimeout; i++) {
		n2->tick();
	}

	nt->send(make_message(2, 2, MsgBeat));

	BOOST_REQUIRE_EQUAL(n1->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n2->state, StateLeader);
}

// TestLearnerCannotVote checks that a learner can't vote even it receives a valid Vote request.
BOOST_AUTO_TEST_CASE(TestLearnerCannotVote) {
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n2->becomeFollower(1, None);

	auto msg = make_message(1, 2, MsgVote, 11, 2);
	msg->set_logterm(11);
	n2->Step(*msg);

	BOOST_REQUIRE_EQUAL(n2->msgs.size(), 0);
}
void testLeaderCycle(bool preVote);
BOOST_AUTO_TEST_CASE(TestLeaderCycle) {
	testLeaderCycle(false);
}

BOOST_AUTO_TEST_CASE(TestLeaderCyclePreVote) {
	testLeaderCycle(true);
}
void testLeaderElectionOverwriteNewerLogs(bool preVote);
// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
BOOST_AUTO_TEST_CASE(TestLeaderElectionOverwriteNewerLogs) {
	testLeaderElectionOverwriteNewerLogs(false);
}

BOOST_AUTO_TEST_CASE(TestLeaderElectionOverwriteNewerLogsPreVote) {
	testLeaderElectionOverwriteNewerLogs(true);
}

void testVoteFromAnyState(MessageType vt);
BOOST_AUTO_TEST_CASE(TestVoteFromAnyState) {
	testVoteFromAnyState(MsgVote);
}

BOOST_AUTO_TEST_CASE(TestPreVoteFromAnyState) {
	testVoteFromAnyState(MsgPreVote);
}

BOOST_AUTO_TEST_CASE(TestLogReplication) {
	struct {
		networkptr network;
		vector<MessagePtr> msgs;
		uint64_t wcommitted;
	} tests[] = {
		{
			newNetwork({nullptr, nullptr, nullptr}),
			msg_move({make_message(1, 1, MsgProp, 0, 0, false, {makeEntry(0, 0, "somedata")})}),
			2,
		},
		{
			newNetwork({nullptr, nullptr, nullptr}),
			msg_move({
				make_message(1, 1, MsgProp, 0, 0, false, {makeEntry(0, 0, "somedata")}),
				make_message(1, 2, MsgHup),
				make_message(1, 2, MsgProp, 0, 0, false, {makeEntry(0, 0, "somedata")}),
			}),
			4,
		},
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		tt.network->send(make_message(1, 1, MsgHup));

		for (auto &m : tt.msgs) {
			tt.network->send(std::make_unique<Message>(*m));
		}

		for (auto it = tt.network->peers.begin(); it != tt.network->peers.end(); ++it) {
			testRaft *sm = dynamic_cast<testRaft*>(it->second.get());
			BOOST_REQUIRE_EQUAL(sm->raftLog->committed, tt.wcommitted);

			vector<Entry> ents;
			for (auto &e : nextEnts(sm, tt.network->storage[it->first].get())) {
				if (!e.data().empty()) {
					ents.push_back(e);
				}
			}
			vector<MessagePtr> props;
			for (auto &m : tt.msgs) {
				if (m->type() == MsgProp) {
					props.push_back(std::make_unique<Message>(*m));
				}
			}
			for (size_t k = 0; k < props.size(); k++) {
				BOOST_REQUIRE_EQUAL(ents[k].data(), props[k]->entries(0).data());
			}
		}
	}
}

// TestLearnerLogReplication tests that a learner can receive entries from the leader.
BOOST_AUTO_TEST_CASE(TestLearnerLogReplication) {
	auto n1 = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_unique<MemoryStorage>());
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_unique<MemoryStorage>());

	auto nt = newNetwork({ n1, n2 });

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);

	setRandomizedElectionTimeout(n1, n1->electionTimeout);
	for (size_t i = 0; i < n1->electionTimeout; i++) {
		n1->tick();
	}
	
	nt->send(make_message(1, 1, MsgBeat));

		// n1 is leader and n2 is learner
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->isLearner, true);


	auto nextCommitted = n1->raftLog->committed + 1;
	
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	BOOST_REQUIRE_EQUAL(n1->raftLog->committed, nextCommitted);
	BOOST_REQUIRE_EQUAL(n1->raftLog->committed, n2->raftLog->committed);

	auto match = n1->getProgress(2)->Match;
	BOOST_REQUIRE_EQUAL(n2->raftLog->committed, match);
}

BOOST_AUTO_TEST_CASE(TestSingleNodeCommit) {
	auto tt = newNetwork({ nullptr });
	make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") });
	tt->send(make_message(1, 1, MsgHup));
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 3);
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
BOOST_AUTO_TEST_CASE(TestCannotCommitWithoutNewTermEntry) {
	auto tt = newNetwork({ nullptr, nullptr, nullptr, nullptr, nullptr });
	tt->send(make_message(1, 1, MsgHup));

	// 0 cannot reach 2,3,4
	tt->cut(1, 3);
	tt->cut(1, 4);
	tt->cut(1, 5);

	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 1);

	// network recovery
	tt->recover();
	// avoid committing ChangeTerm proposal
	tt->ignore(MsgApp);

	// elect 2 as the new leader with term 2
	tt->send(make_message(2, 2, MsgHup));

	// no log entries from previous term should be committed
	sm = dynamic_cast<testRaft*>(tt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 1);

	tt->recover();
	// send heartbeat; reset wait
	tt->send(make_message(2, 2, MsgBeat));
	// append an entry at current term
	tt->send(make_message(2, 2, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));
	// expect the committed to be advanced
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 5);
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
BOOST_AUTO_TEST_CASE(TestCommitWithoutNewTermEntry) {
	auto tt = newNetwork({ nullptr, nullptr, nullptr, nullptr, nullptr });
	tt->send(make_message(1, 1, MsgHup));

	// 0 cannot reach 2,3,4
	tt->cut(1, 3);
	tt->cut(1, 4);
	tt->cut(1, 5);

	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 1);

	// network recovery
	tt->recover();

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt->send(make_message(2, 2, MsgHup));
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 4);
}

BOOST_AUTO_TEST_CASE(TestDuelingCandidates) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());

	auto nt = newNetwork({ a, b, c });
	nt->cut(1, 3);

	nt->send(make_message(1, 1, MsgHup));
	nt->send(make_message(3, 3, MsgHup));

	// 1 becomes leader since it receives votes from 1 and 2
	auto sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateCandidate);

	nt->recover();

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	nt->send(make_message(3, 3, MsgHup));

	auto storage = std::make_shared<MemoryStorage>();
	storage->Append(EntryVec{ {}, makeEntry(1, 1) });
	raft_log wlog(storage, &DefaultLogger::instance());
	wlog.committed = 1;
	wlog.unstable.offset = 2;
	raft_log wlog1(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
	struct {
		testRaft *sm;
		StateType state;
		uint64_t term;
		raft_log *raftLog;
	} tests[] = {
		{ a.get(), StateFollower, 2, &wlog },
		{ b.get(), StateFollower, 2, &wlog },
		{ c.get(), StateFollower, 2, &wlog1 },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		BOOST_REQUIRE_EQUAL(tt.sm->state, tt.state);
		BOOST_REQUIRE_EQUAL(tt.sm->Term, tt.term);
		auto base = ltoa(tt.raftLog);
		auto sm = dynamic_cast<testRaft*>(nt->peers[1 + i].get());
		if (sm) {
			auto l = ltoa(sm->raftLog.get());
			auto g = diffu(base, l);
			BOOST_REQUIRE_EQUAL(g, "");
		} else {
			std::cout << "#%d: empty log" << i << std::endl;
		}
	}
}

BOOST_AUTO_TEST_CASE(TestDuelingPreCandidates) {
	auto cfgA = newTestConfig(1, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
	auto cfgB = newTestConfig(2, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
	auto cfgC = newTestConfig(3, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
	cfgA.PreVote = true;
	cfgB.PreVote = true;
	cfgC.PreVote = true;
	auto a = std::make_shared<testRaft>();
	auto b = std::make_shared<testRaft>();
	auto c = std::make_shared<testRaft>();
	a->Init(std::move(cfgA));
	b->Init(std::move(cfgB));
	c->Init(std::move(cfgC));

	auto nt = newNetwork({ a, b, c });
	nt->cut(1, 3);

	nt->send(make_message(1, 1, MsgHup));
	nt->send(make_message(3, 3, MsgHup));

	// 1 becomes leader since it receives votes from 1 and 2
	auto sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);

	// 3 campaigns then reverts to follower when its PreVote is rejected
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateFollower);

	nt->recover();

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
	nt->send(make_message(3, 3, MsgHup));


	auto storage = std::make_shared<MemoryStorage>();
	storage->Append(EntryVec{ {}, makeEntry(1, 1) });
	raft_log wlog(storage, &DefaultLogger::instance());
	wlog.committed = 1;
	wlog.unstable.offset = 2;
	raft_log wlog1(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
	struct {
		testRaft *sm;
		StateType state;
		uint64_t term;
		raft_log *raftLog;
	} tests[] = {
		{ a.get(), StateLeader, 1, &wlog },
		{ b.get(), StateFollower, 1, &wlog },
		{ c.get(), StateFollower, 1, &wlog1 },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		BOOST_REQUIRE_EQUAL(tt.sm->state, tt.state);
		BOOST_REQUIRE_EQUAL(tt.sm->Term, tt.term);

		auto base = ltoa(tt.raftLog);
		auto sm = dynamic_cast<testRaft*>(nt->peers[1 + i].get());
		if (sm) {
			auto l = ltoa(sm->raftLog.get());
			auto g = diffu(base, l);
			BOOST_REQUIRE_EQUAL(g, "");
		} else {
			std::cout << (boost::format("#%d: empty log") % i).str() << endl;
		}
	}
}

BOOST_AUTO_TEST_CASE(TestCandidateConcede) {
	auto tt = newNetwork({ nullptr, nullptr, nullptr });
	tt->isolate(1);

	tt->send(make_message(1, 1, MsgHup));
	tt->send(make_message(3, 3, MsgHup));

	// heal the partition
	tt->recover();
	// send heartbeat; reset wait
	tt->send(make_message(3, 3, MsgBeat));

	auto data = "force follower";
	// send a proposal to 3 to flush out a MsgApp to 1
	tt->send(make_message(3, 3, MsgProp, 0, 0, false, { makeEntry(0, 0, string(data)) }));
	// send heartbeat; flush out commit
	tt->send(make_message(3, 3, MsgBeat));

	auto a = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(a->state, StateFollower);
	BOOST_REQUIRE_EQUAL(a->Term, 1);
	auto storage = std::make_shared<MemoryStorage>();
	storage->Append(EntryVec{ {}, makeEntry(1, 1), makeEntry(2, 1, string(data)) });
	raft_log wlog(storage, &DefaultLogger::instance());
	wlog.committed = 2;
	wlog.unstable.offset = 3;
	auto wantLog = ltoa(&wlog);
	for (size_t i = 0; i < tt->peers.size(); i++) {
		auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
		if (sm) {
			auto l = ltoa(sm->raftLog.get());
			auto g = diffu(wantLog, l);
			BOOST_REQUIRE_EQUAL(g, "");
		} else {
			std::cout << (boost::format("#%d: empty log") % i).str() << endl;
		}
	}
}

BOOST_AUTO_TEST_CASE(TestSingleNodeCandidate) {
	auto tt = newNetwork({ nullptr });
	tt->send(make_message(1, 1, MsgHup));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestSingleNodePreCandidate) {
	auto tt = newNetworkWithConfig(preVoteConfig, { nullptr });
	tt->send(make_message(1, 1, MsgHup));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestOldMessages) {
	auto tt = newNetwork({ nullptr, nullptr, nullptr });
	// make 0 leader @ term 3
	tt->send(make_message(1, 1, MsgHup));
	tt->send(make_message(2, 2, MsgHup));
	tt->send(make_message(1, 1, MsgHup));
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt->send(make_message(2, 1, MsgApp, 0, 2, false, { makeEntry(3, 2) }));
	// commit a new entry
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));

	auto storage = std::make_shared<MemoryStorage>();
	storage->Append(EntryVec{ {}, makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 3, "somedata") });
	raft_log ilog(storage, &DefaultLogger::instance());
	ilog.committed = 4;
	ilog.unstable.offset = 5;
	auto base = ltoa(&ilog);
	for (size_t i = 0; i < tt->peers.size(); i++) {
		auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
		if (sm) {
			auto l = ltoa(sm->raftLog.get());
			auto g = diffu(base, l);
			BOOST_REQUIRE_EQUAL(g, "");
		} else {
			std::cout << (boost::format("#%d: empty log") % i).str() << endl;
		}
	}
}

BOOST_AUTO_TEST_CASE(TestProposal) {
	struct {
		networkptr network;
		bool success;
	} tests[] = {
		{newNetwork({nullptr, nullptr, nullptr}), true},
		{ newNetwork({nullptr, nullptr, nopStepper}), true },
		{ newNetwork({nullptr, nopStepper, nopStepper}), false },
		{ newNetwork({nullptr, nopStepper, nopStepper, nullptr}), false },
		{ newNetwork({nullptr, nopStepper, nopStepper, nullptr, nullptr}), true },
	};

	for (size_t j = 0; j < sizeof(tests) / sizeof(tests[0]); j++) {
		auto &tt = tests[j];
		auto send = [&](MessagePtr &&m) {
			try {
				tt.network->send(std::move(m));
			} catch (std::runtime_error &) {
				BOOST_REQUIRE_EQUAL(tt.success, false);
			}
		};

		auto data = "somedata";

		// promote 1 to become leader
		send(make_message(1, 1, MsgHup));
		send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0, data) }));

		auto wantLog = std::make_unique<raft_log>(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
		if (tt.success) {
			auto storage = std::make_shared<MemoryStorage>();
			storage->Append(EntryVec{ {}, makeEntry(1, 1), makeEntry(2, 1, data) });
			wantLog = std::make_unique<raft_log>(storage, &DefaultLogger::instance());
			wantLog->committed = 2;
			wantLog->unstable.offset = 3;
		}
		auto base = ltoa(wantLog.get());
		for (size_t i = 0; i < tt.network->peers.size(); i++) {
			auto sm = dynamic_cast<testRaft*>(tt.network->peers[i].get());
			if (sm) {
				auto l = ltoa(sm->raftLog.get());
				auto g = diffu(base, l);
				BOOST_REQUIRE_EQUAL(g, "");
			} else {
				std::cout << (boost::format("#%d: empty log") % i).str() << endl;
			}
		}
		auto sm = dynamic_cast<testRaft*>(tt.network->peers[1].get());
		BOOST_REQUIRE_EQUAL(sm->Term, 1);
	}
}

BOOST_AUTO_TEST_CASE(TestProposalByProxy) {
	auto data = "somedata";
	networkptr tests[] = {
		newNetwork({nullptr, nullptr, nullptr}),
		newNetwork({nullptr, nullptr, nopStepper}),
	};

	for (size_t j = 0; j < sizeof(tests) / sizeof(tests[0]); j++) {
		auto &tt = tests[j];
		// promote 0 the leader
		tt->send(make_message(1, 1, MsgHup));

		// propose via follower
		tt->send(make_message(2, 2, MsgProp, 0, 0, false, { makeEntry(0, 0, data) }));

		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ {}, makeEntry(1, 1), makeEntry(2, 1, data) });
		raft_log wantLog(storage, &DefaultLogger::instance());
		wantLog.committed = 2;
		wantLog.unstable.offset = 3;
		auto base = ltoa(&wantLog);
		for (size_t i = 0; i < tt->peers.size(); i++) {
			auto sm = dynamic_cast<testRaft*>(tt->peers[i].get());
			if (sm) {
				auto l = ltoa(sm->raftLog.get());
				auto g = diffu(base, l);
				BOOST_REQUIRE_EQUAL(g, "");
			} else {
				std::cout << (boost::format("#%d: empty log") % i).str() << endl;
			}
		}
		auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
		BOOST_REQUIRE_EQUAL(sm->Term, 1);
	}
}

BOOST_AUTO_TEST_CASE(TestCommit) {
	struct {
		vector<uint64_t> matches;
		vector<Entry> logs;
		uint64_t smTerm;
		uint64_t w;
	} tests[] = {
		// single
		{ { 1 },{makeEntry(1, 1) }, 1, 1 },
		{ {1},{makeEntry(1, 1)}, 2, 0 },
		{ {2},{makeEntry(1, 1), makeEntry(2, 2)}, 2, 2 },
		{ {1},{makeEntry(1, 2)}, 2, 1 },

		// odd
		{ {2, 1, 1},{makeEntry(1, 1), makeEntry(2, 2)}, 1, 1 },
		{ {2, 1, 1},{makeEntry(1, 1), makeEntry(2, 1)}, 2, 0 },
		{ {2, 1, 2},{makeEntry(1, 1), makeEntry(2, 2)}, 2, 2 },
		{ {2, 1, 2},{makeEntry(1, 1), makeEntry(2, 1)}, 2, 0 },

		// even
		{ {2, 1, 1, 1},{makeEntry(1, 1), makeEntry(2, 2)}, 1, 1 },
		{ {2, 1, 1, 1},{makeEntry(1, 1), makeEntry(2, 1)}, 2, 0 },
		{ {2, 1, 1, 2},{makeEntry(1, 1), makeEntry(2, 2)}, 1, 1 },
		{ {2, 1, 1, 2},{makeEntry(1, 1), makeEntry(2, 1)}, 2, 0 },
		{ {2, 1, 2, 2},{makeEntry(1, 1), makeEntry(2, 2)}, 2, 2 },
		{ {2, 1, 2, 2},{makeEntry(1, 1), makeEntry(2, 1)}, 2, 0 },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(tt.logs);
		HardState hs;
		hs.set_term(tt.smTerm);
		storage->SetHardState(hs);

		auto sm = newTestRaft(1, { 1 }, 10, 2, storage);
		for (size_t j = 0; j < tt.matches.size(); j++) {
			sm->setProgress(uint64_t(j) + 1, tt.matches[j], tt.matches[j] + 1, false);
		}
		sm->maybeCommit();
		BOOST_REQUIRE_EQUAL(sm->raftLog->committed, tt.w);
	}
}

BOOST_AUTO_TEST_CASE(TestPastElectionTimeout) {
	struct {
		int elapse;
		double wprobability;
		bool round;
	} tests[] = {
		{ 5, 0, false },
		{ 10, 0.1, true },
		{ 13, 0.4, true },
		{ 15, 0.6, true },
		{ 18, 0.9, true },
		{ 20, 1, false },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto sm = newTestRaft(1, { 1 }, 10, 1, std::make_shared<MemoryStorage>());
		sm->electionElapsed = tt.elapse;
		double c = 0;
		for (size_t j = 0; j < 10000; j++) {
			sm->resetRandomizedElectionTimeout();
			if (sm->pastElectionTimeout()) {
				c++;
			}
		}
		double got = c / 10000.0;
		if (tt.round) {
			got = floor(got * 10 + 0.5) / 10.0;
		}
		BOOST_REQUIRE_EQUAL(got, tt.wprobability);
	}
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
BOOST_AUTO_TEST_CASE(TestStepIgnoreOldTermMsg) {
	bool called = false;
	auto fakeStep = [&](Raft *r, Message &m) {
		called = true;
		return OK;
	};
	auto sm = newTestRaft(1, { 1 }, 10, 1, std::make_shared<MemoryStorage>());
	sm->step = fakeStep;
	sm->Term = 2;
	sm->Step(*make_message(0, 0, MsgApp, 0, sm->Term - 1));
	BOOST_REQUIRE_EQUAL(called, false);
}

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
BOOST_AUTO_TEST_CASE(TestHandleMsgApp) {
	auto initMsg = [](MessageType Type, uint64_t Term, uint64_t LogTerm, uint64_t Index, uint64_t Commit, vector<Entry> &&ents = {}) {
		auto msg = make_message(0, 0, Type, Index, Term, false, std::move(ents));
		msg->set_commit(Commit);
		msg->set_logterm(LogTerm);
		return std::move(msg);
	};
	struct {
		MessagePtr m;
		uint64_t wIndex;
		uint64_t wCommit;
		bool wReject;
	} tests[] = {
		// Ensure 1
		{ initMsg(MsgApp, 2, 3, 2, 3), 2, 0, true }, // previous log mismatch
		{ initMsg(MsgApp, 2, 3, 3, 3), 2, 0, true }, // previous log non-exist

		// Ensure 2
		{ initMsg(MsgApp, 2, 1, 1, 1), 2, 1, false },
		{ initMsg(MsgApp, 2, 0, 0, 1, {makeEntry(1, 2)}), 1, 1, false },
		{ initMsg(MsgApp, 2, 2, 2, 3, {makeEntry(3, 2), makeEntry(4, 2)}), 4, 3, false },
		{ initMsg(MsgApp, 2, 2, 2, 4, {makeEntry(3, 2)}), 3, 3, false },
		{ initMsg(MsgApp, 2, 1, 1, 4, {makeEntry(2, 2)}), 2, 2, false },

		// Ensure 3
		{ initMsg(MsgApp, 1, 1, 1, 3), 2, 1, false },                                           // match entry 1, commit up to last new entry 1
		{ initMsg(MsgApp, 1, 1, 1, 3, {makeEntry(2, 2)}), 2, 2, false }, // match entry 1, commit up to last new entry 2
		{ initMsg(MsgApp, 2, 2, 2, 3), 2, 2, false },                                           // match entry 2, commit up to last new entry 2
		{ initMsg(MsgApp, 2, 2, 2, 4), 2, 2, false },                                           // commit up to log.last()
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ makeEntry(1, 1), makeEntry(2, 2) });
		auto sm = newTestRaft(1, { 1 }, 10, 1, storage);
		sm->becomeFollower(2, None);

		sm->handleAppendEntries(*tt.m);
		BOOST_REQUIRE_EQUAL(sm->raftLog->lastIndex(), tt.wIndex);
		BOOST_REQUIRE_EQUAL(sm->raftLog->committed, tt.wCommit);
		auto m = sm->readMessages();
		BOOST_REQUIRE_EQUAL(m.size(), 1);
		BOOST_REQUIRE_EQUAL(m[0]->reject(), tt.wReject);
	}
}
// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
BOOST_AUTO_TEST_CASE(TestHandleHeartbeat) {
	auto initMsg = [](uint64_t From, uint64_t To, MessageType Type, uint64_t Term, uint64_t Commit) {
		auto msg = make_message(From, To, Type, 0, Term, false);
		msg->set_commit(Commit);
		return std::move(msg);
	};
	uint64_t commit = 2;
	struct {
		MessagePtr m;
		uint64_t wCommit;
	} tests[] = {
		{initMsg(2, 1, MsgHeartbeat, 2, commit + 1), commit + 1},
		{initMsg(2, 1, MsgHeartbeat, 2, commit - 1), commit }, // do not decrease commit
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) });
		auto sm = newTestRaft(1, { 1, 2 }, 5, 1, storage);
		sm->becomeFollower(2, 2);
		sm->raftLog->commitTo(commit);
		sm->handleHeartbeat(*tt.m);
		BOOST_REQUIRE_EQUAL(sm->raftLog->committed, tt.wCommit);

		auto m = sm->readMessages();
		BOOST_REQUIRE_EQUAL(m.size(), 1);
		BOOST_REQUIRE_EQUAL(m[0]->type(), MsgHeartbeatResp);
	}
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
BOOST_AUTO_TEST_CASE(TestHandleHeartbeatResp) {
	auto storage = std::make_shared<MemoryStorage>();
	storage->Append(EntryVec{ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) });
	auto sm = newTestRaft(1, { 1, 2 }, 5, 1, storage);
	sm->becomeCandidate();
	sm->becomeLeader();
	sm->raftLog->commitTo(sm->raftLog->lastIndex());

	// A heartbeat response from a node that is behind; re-send MsgApp
	sm->Step(*make_message(2, 0, MsgHeartbeatResp));
	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->type(), MsgApp);

	// A second heartbeat response generates another MsgApp re-send
	sm->Step(*make_message(2, 0, MsgHeartbeatResp));
	msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->type(), MsgApp);

	// Once we have an MsgAppResp, heartbeats no longer send MsgApp.
	sm->Step(*make_message(2, 0, MsgAppResp, msgs[0]->index() + msgs[0]->entries_size()));
	// Consume the message sent in response to MsgAppResp
	sm->readMessages();

	sm->Step(*make_message(2, 0, MsgHeartbeatResp));
	msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 0);
}

// TestRaftFreesReadOnlyMem ensures raft will free read request from
// readOnly readIndexQueue and pendingReadIndex map.
// related issue: https://github.com/etcd-io/etcd/issues/7571
BOOST_AUTO_TEST_CASE(TestRaftFreesReadOnlyMem) {
	auto sm = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	sm->becomeCandidate();
	sm->becomeLeader();
	sm->raftLog->commitTo(sm->raftLog->lastIndex());

	auto ctx = "ctx";

	// leader starts linearizable read request.
	// more info: raft dissertation 6.4, step 2.
	sm->Step(*make_message(2, 0, MsgReadIndex, 0, 0, false, { makeEntry(0,0,string(ctx)) }));
	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->type(), MsgHeartbeat);
	BOOST_REQUIRE_EQUAL(msgs[0]->context(), ctx);
	BOOST_REQUIRE_EQUAL(sm->readOnly->readIndexQueue.size(), 1);
	BOOST_REQUIRE_EQUAL(sm->readOnly->pendingReadIndex.size(), 1);
	BOOST_REQUIRE_EQUAL(sm->readOnly->pendingReadIndex.find(string(ctx)) !=
		sm->readOnly->pendingReadIndex.end(), true);

	// heartbeat responses from majority of followers (1 in this case)
	// acknowledge the authority of the leader.
	// more info: raft dissertation 6.4, step 3.
	auto msg = make_message(2, 0, MsgHeartbeatResp);
	msg->set_context(ctx);
	sm->Step(*msg);
	BOOST_REQUIRE_EQUAL(sm->readOnly->readIndexQueue.size(), 0);
	BOOST_REQUIRE_EQUAL(sm->readOnly->pendingReadIndex.size(), 0);
	BOOST_REQUIRE_EQUAL(sm->readOnly->pendingReadIndex.find(string(ctx)) ==
		sm->readOnly->pendingReadIndex.end(), true);
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// MsgAppResp.
BOOST_AUTO_TEST_CASE(TestMsgAppRespWaitReset) {
	auto sm = newTestRaft(1, { 1, 2, 3 }, 5, 1, std::make_unique<MemoryStorage>());
	sm->becomeCandidate();
	sm->becomeLeader();

	// The new leader has just emitted a new Term 4 entry; consume those messages
	// from the outgoing queue.
	sm->bcastAppend();
	sm->readMessages();

	// Node 2 acks the first entry, making it committed.
	sm->Step(*make_message(2, 0, MsgAppResp, 1));
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 1);
	// Also consume the MsgApp messages that update Commit on the followers.
	sm->readMessages();

		// A new command is now proposed on node 1.
	sm->Step(*make_message(1, 0, MsgProp, 0, 0, false, { {} }));

	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->type(), MsgApp);
	BOOST_REQUIRE_EQUAL(msgs[0]->to(), 2);
	BOOST_REQUIRE_EQUAL(msgs[0]->entries_size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->entries(0).index(), 2);

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
	sm->Step(*make_message(3, 0, MsgAppResp, 1));
	msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->type(), MsgApp);
	BOOST_REQUIRE_EQUAL(msgs[0]->to(), 3);
	BOOST_REQUIRE_EQUAL(msgs[0]->entries_size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->entries(0).index(), 2);
}
void testRecvMsgVote(MessageType);
BOOST_AUTO_TEST_CASE(TestRecvMsgVote) {
	testRecvMsgVote(MsgVote);
}

BOOST_AUTO_TEST_CASE(TestRecvMsgPreVote) {
	testRecvMsgVote(MsgPreVote);
}

BOOST_AUTO_TEST_CASE(TestStateTransition) {
	struct {
		StateType from;
		StateType to;
		bool wallow;
		uint64_t wterm;
		uint64_t wlead;
	}tests[] = {
		{StateFollower, StateFollower, true, 1, None},
		{ StateFollower, StatePreCandidate, true, 0, None },
		{ StateFollower, StateCandidate, true, 1, None },
		{ StateFollower, StateLeader, false, 0, None },

		{ StatePreCandidate, StateFollower, true, 0, None },
		{ StatePreCandidate, StatePreCandidate, true, 0, None },
		{ StatePreCandidate, StateCandidate, true, 1, None },
		{ StatePreCandidate, StateLeader, true, 0, 1 },

		{ StateCandidate, StateFollower, true, 0, None },
		{ StateCandidate, StatePreCandidate, true, 0, None },
		{ StateCandidate, StateCandidate, true, 1, None },
		{ StateCandidate, StateLeader, true, 0, 1 },

		{ StateLeader, StateFollower, true, 1, None },
		{ StateLeader, StatePreCandidate, false, 0, None },
		{ StateLeader, StateCandidate, false, 1, None },
		{ StateLeader, StateLeader, true, 0, 1 },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		try {
			auto sm = newTestRaft(1, { 1 }, 10, 1, std::make_shared<MemoryStorage>());
			sm->state = tt.from;

			switch (tt.to) {
			case StateFollower:
				sm->becomeFollower(tt.wterm, tt.wlead);
				break;
			case StatePreCandidate:
				sm->becomePreCandidate();
				break;
			case StateCandidate:
				sm->becomeCandidate();
				break;
			case StateLeader:
				sm->becomeLeader();
				break;
			}
			BOOST_REQUIRE_EQUAL(sm->Term, tt.wterm);
			BOOST_REQUIRE_EQUAL(sm->lead, tt.wlead);
		} catch (std::runtime_error &) {
			BOOST_REQUIRE_EQUAL(tt.wallow, false);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestAllServerStepdown) {
	struct {
		StateType state;

		StateType wstate;
		uint64_t wterm;
		uint64_t windex;
	} tests[] = {
		{StateFollower, StateFollower, 3, 0},
		{ StatePreCandidate, StateFollower, 3, 0 },
		{ StateCandidate, StateFollower, 3, 0 },
		{ StateLeader, StateFollower, 3, 1 },
	};

	vector<MessageType> tmsgTypes = { MsgVote, MsgApp };
	uint64_t tterm = 3;

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto sm = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
		switch (tt.state) {
		case StateFollower:
			sm->becomeFollower(1, None);
			break;
		case StatePreCandidate:
			sm->becomePreCandidate();
			break;
		case StateCandidate:
			sm->becomeCandidate();
			break;
		case StateLeader:
			sm->becomeCandidate();
			sm->becomeLeader();
			break;
		}

		for (auto msgType : tmsgTypes) {
			auto msg = make_message(2, 0, msgType, 0, tterm);
			msg->set_logterm(tterm);
			sm->Step(*msg);
			BOOST_REQUIRE_EQUAL(sm->state, tt.wstate);
			BOOST_REQUIRE_EQUAL(sm->Term, tt.wterm);
			BOOST_REQUIRE_EQUAL(sm->raftLog->lastIndex(), tt.windex);
			BOOST_REQUIRE_EQUAL(sm->raftLog->allEntries().size(), tt.windex);

			uint64_t wlead = 2;
			if (msgType == MsgVote) {
				wlead = None;
			}
			BOOST_REQUIRE_EQUAL(sm->lead, wlead);
		}
	}
}

void testCandidateResetTerm(MessageType);
BOOST_AUTO_TEST_CASE(TestCandidateResetTermMsgHeartbeat) {
	testCandidateResetTerm(MsgHeartbeat);
}

BOOST_AUTO_TEST_CASE(TestCandidateResetTermMsgApp) {
	testCandidateResetTerm(MsgApp);
}

BOOST_AUTO_TEST_CASE(TestLeaderStepdownWhenQuorumActive) {
	auto sm = newTestRaft(1, { 1, 2, 3 }, 5, 1, std::make_shared<MemoryStorage>());

	sm->checkQuorum = true;

	sm->becomeCandidate();
	sm->becomeLeader();

	for (size_t i = 0; i < sm->electionTimeout + 1; i++) {
		sm->Step(*make_message(2, 0, MsgHeartbeatResp, 0, sm->Term));
		sm->tick();
	}
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestLeaderStepdownWhenQuorumLost) {
	auto sm = newTestRaft(1, { 1, 2, 3 }, 5, 1, std::make_shared<MemoryStorage>());

	sm->checkQuorum = true;

	sm->becomeCandidate();
	sm->becomeLeader();

	for (size_t i = 0; i < sm->electionTimeout + 1; i++) {
		sm->tick();
	}

	BOOST_REQUIRE_EQUAL(sm->state, StateFollower);
}

BOOST_AUTO_TEST_CASE(TestLeaderSupersedingWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	a->checkQuorum = true;
	b->checkQuorum = true;
	c->checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->electionTimeout + 1);

	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}

	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateLeader);
	BOOST_REQUIRE_EQUAL(c->state, StateFollower);

	nt->send(make_message(3, 3, MsgHup));

	// Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
	BOOST_REQUIRE_EQUAL(c->state, StateCandidate);

	// Letting b's electionElapsed reach to electionTimeout
	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}
	nt->send(make_message(3, 3, MsgHup));
	BOOST_REQUIRE_EQUAL(c->state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestLeaderElectionWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	a->checkQuorum = true;
	b->checkQuorum = true;
	c->checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(a, a->electionTimeout + 1);
	setRandomizedElectionTimeout(b, b->electionTimeout + 2);

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateLeader);
	BOOST_REQUIRE_EQUAL(c->state, StateFollower);

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
	setRandomizedElectionTimeout(a, a->electionTimeout + 1);
	setRandomizedElectionTimeout(b, b->electionTimeout + 2);
	for (size_t i = 0; i < a->electionTimeout; i++) {
		a->tick();
	}
	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->state, StateLeader);
}


// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
BOOST_AUTO_TEST_CASE(TestFreeStuckCandidateWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	a->checkQuorum = true;
	b->checkQuorum = true;
	c->checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->electionTimeout + 1);

	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(1);
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(b->state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->state, StateCandidate);
	BOOST_REQUIRE_EQUAL(c->Term, b->Term + 1);

	// Vote again for safety
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(b->state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->state, StateCandidate);
	BOOST_REQUIRE_EQUAL(c->Term, b->Term + 2);

	nt->recover();
	nt->send(make_message(1, 3, MsgHeartbeat, 0, a->Term));

	// Disrupt the leader so that the stuck peer is freed
	BOOST_REQUIRE_EQUAL(a->state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->Term, a->Term);

	// Vote again, should become leader this time
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(c->state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestNonPromotableVoterWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1 }, 10, 1, std::make_shared<MemoryStorage>());

	a->checkQuorum = true;
	b->checkQuorum = true;

	auto nt = newNetwork({ a, b });
	setRandomizedElectionTimeout(b, b->electionTimeout + 1);
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
	b->delProgress(2);

	BOOST_REQUIRE_EQUAL(b->promotable(), false);

	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateLeader);
	BOOST_REQUIRE_EQUAL(b->state, StateFollower);
	BOOST_REQUIRE_EQUAL(b->lead, 1);
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
BOOST_AUTO_TEST_CASE(TestDisruptiveFollower) {
	auto n1 = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n3 = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->checkQuorum = true;
	n2->checkQuorum = true;
	n3->checkQuorum = true;

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	auto nt = newNetwork({ n1, n2, n3 });

	nt->send(make_message(1, 1, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateFollower
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StateFollower);

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	setRandomizedElectionTimeout(n3, n3->electionTimeout + 2);
	for (size_t i = 0; i < n3->randomizedElectionTimeout - 1; i++) {
		n3->tick();
	}

	// ideally, before last election tick elapses,
	// the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
	// from leader n1, and then resets its "electionElapsed"
	// however, last tick may elapse before receiving any
	// messages from leader, thus triggering campaign
	n3->tick();

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateCandidate
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StateCandidate);

	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 3
	BOOST_REQUIRE_EQUAL(n1->Term, 2);
	BOOST_REQUIRE_EQUAL(n2->Term, 2);
	BOOST_REQUIRE_EQUAL(n3->Term, 3);

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
	nt->send(make_message(1, 3, MsgHeartbeat, 0, n1->Term));

	// then candidate n3 responds with "pb.MsgAppResp" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election

	// check state
	// n1.state == StateFollower
	// n2.state == StateFollower
	// n3.state == StateCandidate
	BOOST_REQUIRE_EQUAL(n1->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StateCandidate);

	// check term
	// n1.Term == 3
	// n2.Term == 2
	// n3.Term == 3
	BOOST_REQUIRE_EQUAL(n1->Term, 3);
	BOOST_REQUIRE_EQUAL(n2->Term, 2);
	BOOST_REQUIRE_EQUAL(n3->Term, 3);
}

// TestDisruptiveFollowerPreVote tests isolated follower,
// with slow network incoming from leader, election times out
// to become a pre-candidate with less log than current leader.
// Then pre-vote phase prevents this isolated node from forcing
// current leader to step down, thus less disruptions.
BOOST_AUTO_TEST_CASE(TestDisruptiveFollowerPreVote) {
	auto n1 = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n3 = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->checkQuorum = true;
	n2->checkQuorum = true;
	n3->checkQuorum = true;

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	auto nt = newNetwork({ n1, n2, n3 });

	nt->send(make_message(1, 1, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateFollower
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StateFollower);

	nt->isolate(3);
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	n1->preVote = true;
	n2->preVote = true;
	n3->preVote = true;
	nt->recover();
	nt->send(make_message(3, 3, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StatePreCandidate
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StatePreCandidate);

	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 2
	BOOST_REQUIRE_EQUAL(n1->Term, 2);
	BOOST_REQUIRE_EQUAL(n2->Term, 2);
	BOOST_REQUIRE_EQUAL(n3->Term, 2);

	// delayed leader heartbeat does not force current leader to step down
	nt->send(make_message(1, 3, MsgHeartbeat, 0, n1->Term));
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestReadOnlyOptionSafe) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->electionTimeout + 1);

	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateLeader);

	struct {
		testRaft *sm;
		int proposals;
		uint64_t wri;
		const char* wctx;
	} tests[] = {
		{ a.get(), 10, 11, "ctx1" },
		{ b.get(), 10, 21, "ctx2" },
		{ c.get(), 10, 31, "ctx3" },
		{ a.get(), 10, 41, "ctx4" },
		{ b.get(), 10, 51, "ctx5" },
		{ c.get(), 10, 61, "ctx6" },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		for (size_t j = 0; j < tt.proposals; j++) {
			nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
		}

		nt->send(make_message(tt.sm->id, tt.sm->id, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, tt.wctx) }));

		auto r = tt.sm;
		BOOST_REQUIRE_GT(r->readStates.size(), 0);
		auto &rs = r->readStates[0];
		BOOST_REQUIRE_EQUAL(rs.Index, tt.wri);
		BOOST_REQUIRE_EQUAL(rs.RequestCtx, tt.wctx);
		r->readStates.clear();
	}
}

BOOST_AUTO_TEST_CASE(TestReadOnlyOptionLease) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	a->readOnly->option = ReadOnlyLeaseBased;
	b->readOnly->option = ReadOnlyLeaseBased;
	c->readOnly->option = ReadOnlyLeaseBased;
	a->checkQuorum = true;
	b->checkQuorum = true;
	c->checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->electionTimeout + 1);

	for (size_t i = 0; i < b->electionTimeout; i++) {
		b->tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateLeader);

	struct {
		testRaft *sm;
		int proposals;
		uint64_t wri;
		const char *wctx;
	} tests[] = {
		{ a.get(), 10, 11, "ctx1" },
		{ b.get(), 10, 21, "ctx2" },
		{ c.get(), 10, 31, "ctx3" },
		{ a.get(), 10, 41, "ctx4" },
		{ b.get(), 10, 51, "ctx5" },
		{ c.get(), 10, 61, "ctx6" },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		for (size_t j = 0; j < tt.proposals; j++) {
			nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
		}

		nt->send(make_message(tt.sm->id, tt.sm->id, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, tt.wctx) }));

		auto r = tt.sm;
		auto &rs = r->readStates[0];
		BOOST_REQUIRE_EQUAL(rs.Index, tt.wri);
		BOOST_REQUIRE_EQUAL(rs.RequestCtx, tt.wctx);
		r->readStates.clear();
	}
}

// TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex message
// when it commits at least one log entry at it term.
BOOST_AUTO_TEST_CASE(TestReadOnlyForNewLeader) {
	struct {
		uint64_t id;
		uint64_t committed;
		uint64_t applied;
		uint64_t compactIndex;
	} nodeConfigs[] = {
		{1, 1, 1, 0},
		{ 2, 2, 2, 2 },
		{ 3, 2, 2, 2 },
	};
	vector<stateMachinePtr> peers;
	for (auto &c : nodeConfigs) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ makeEntry(1, 1), makeEntry(2, 1) });
		HardState hs;
		hs.set_term(1);
		hs.set_commit(c.committed);
		storage->SetHardState(hs);
		if (c.compactIndex != 0) {
			storage->Compact(c.compactIndex);
		}
		auto cfg = newTestConfig(c.id, { 1, 2, 3 }, 10, 1, storage);
		cfg.Applied = c.applied;
		auto raft = std::make_shared<testRaft>();
		raft->Init(std::move(cfg));
		peers.push_back(raft);
	}
	auto nt = newNetwork(peers);

	// Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
	nt->ignore(MsgApp);
	// Force peer a to become leader.
	nt->send(make_message(1, 1, MsgHup));

	auto sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);

	// Ensure peer a drops read only request.
	uint64_t windex = 4;
	auto wctx = "ctx";
	nt->send(make_message(1, 1, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, wctx) }));
	BOOST_REQUIRE_EQUAL(sm->readStates.size(), 0);

	nt->recover();

	// Force peer a to commit a log entry at its term
	for (size_t i = 0; i < sm->heartbeatTimeout; i++) {
		sm->tick();
	}
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, 4);

	uint64_t ter;
	auto err = sm->raftLog->term(sm->raftLog->committed, ter);
	auto lastLogTerm = sm->raftLog->zeroTermOnErrCompacted(ter, err);
	BOOST_REQUIRE_EQUAL(lastLogTerm, sm->Term);

	// Ensure peer a accepts read only request after it commits a entry at its term.
	nt->send(make_message(1, 1, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, wctx) }));
	BOOST_REQUIRE_EQUAL(sm->readStates.size(), 1);
	auto &rs = sm->readStates[0];
	BOOST_REQUIRE_EQUAL(rs.Index, windex);
	BOOST_REQUIRE_EQUAL(rs.RequestCtx, wctx);
}

BOOST_AUTO_TEST_CASE(TestLeaderAppResp) {
	// initial progress: match = 0; next = 3
	struct {
		uint64_t index;
		bool reject;
		// progress
		uint64_t wmatch;
		uint64_t wnext;
		// message
		int wmsgNum;
		uint64_t windex;
		uint64_t wcommitted;
	} tests[] = {
		{3, true, 0, 3, 0, 0, 0},  // stale resp; no replies
		{ 2, true, 0, 2, 1, 1, 0 },  // denied resp; leader does not commit; decrease next and send probing msg
		{ 2, false, 2, 4, 2, 2, 2 }, // accept resp; leader commits; broadcast with commit index
		{ 0, false, 0, 3, 0, 0, 0 }, // ignore heartbeat replies
	};

	for (auto &tt : tests) {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ {}, makeEntry(1, 0), makeEntry(2, 1) });
		auto sm = newTestRaft(1, { 1, 2, 3 }, 10, 1, storage);
		sm->raftLog->unstable.offset = 3;
		sm->becomeCandidate();
		sm->becomeLeader();
		sm->readMessages();
		auto msg = make_message(2, 0, MsgAppResp, tt.index, sm->Term, tt.reject);
		msg->set_rejecthint(tt.index);
		sm->Step(*msg);

		auto &p = sm->prs[2];
		BOOST_REQUIRE_EQUAL(p->Match, tt.wmatch);
		BOOST_REQUIRE_EQUAL(p->Next, tt.wnext);

		auto msgs = sm->readMessages();
		for (auto &msg : msgs) {
			BOOST_REQUIRE_EQUAL(msg->index(), tt.windex);
			BOOST_REQUIRE_EQUAL(msg->commit(), tt.wcommitted);
		}
	}
}

// When the leader receives a heartbeat tick, it should
// send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries.
BOOST_AUTO_TEST_CASE(TestBcastBeat) {
	uint64_t offset = 1000;
	// make a state machine with log.offset = 1000
	auto s = makeSnapshot(offset, 1);
	for (uint64_t i : {1, 2, 3}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	auto storage = std::make_shared<MemoryStorage>();
	storage->ApplySnapshot(*s);
	auto sm = newTestRaft(1, {}, 10, 1, storage);
	sm->Term = 1;

	sm->becomeCandidate();
	sm->becomeLeader();
	for (size_t i = 0; i < 10; i++) {
		mustAppendEntry(sm.get(), { makeEntry(i + 1, 0) });
	}
	// slow follower
	sm->prs[2]->Match = 5;
	sm->prs[2]->Next = 6;
	// normal follower
	sm->prs[3]->Match = sm->raftLog->lastIndex();
	sm->prs[3]->Next = sm->raftLog->lastIndex() + 1;

	sm->Step(*make_message(0, 0, MsgBeat));
	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 2);
	map<uint64_t, uint64_t> wantCommitMap = {
		{2, min(sm->raftLog->committed, sm->prs[2]->Match)},
		{3, min(sm->raftLog->committed, sm->prs[3]->Match)},
	};
	for (auto &m : msgs) {
		BOOST_REQUIRE_EQUAL(m->type(), MsgHeartbeat);
		BOOST_REQUIRE_EQUAL(m->index(), 0);
		BOOST_REQUIRE_EQUAL(m->logterm(), 0);
		BOOST_REQUIRE_EQUAL(wantCommitMap[m->to()], m->commit());
		wantCommitMap.erase(m->to());
		BOOST_REQUIRE_EQUAL(m->entries_size(), 0);
	}
}

BOOST_AUTO_TEST_CASE(TestRecvMsgBeat) {
	struct {
		StateType state;
		int wMsg;
	} tests[] = {
		{StateLeader, 2},
		// candidate and follower should ignore MsgBeat
	{ StateCandidate, 0 },
	{ StateFollower, 0 },
	};

	for (auto &tt : tests) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ {}, makeEntry(1, 0), makeEntry(2, 1) });
		auto sm = newTestRaft(1, { 1, 2, 3 }, 10, 1, storage);
		sm->Term = 1;
		sm->state = tt.state;
		switch (tt.state) {
		case StateFollower:
			sm->step = stepFollower;
			break;
		case StateCandidate:
			sm->step = stepCandidate;
			break;
		case StateLeader:
			sm->step = stepLeader;
			break;
		}

		sm->Step(*make_message(1, 1, MsgBeat));

		auto msgs = sm->readMessages();
		BOOST_REQUIRE_EQUAL(msgs.size(), tt.wMsg);

		for (auto &m : msgs) {
			BOOST_REQUIRE_EQUAL(m->type(), MsgHeartbeat);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestLeaderIncreaseNext) {
	vector<Entry> previousEnts = { makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1) };
	struct {
		// progress
		ProgressStateType state;
		uint64_t next;

		uint64_t wnext;
	} tests[] = {
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
		{ProgressStateReplicate, 2, uint64_t(previousEnts.size() + 1 + 1 + 1)},
		// state probe, not optimistically increase next
		{ ProgressStateProbe, 2, 2 },
	};

	for (auto &tt : tests) {
		auto sm = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
		sm->raftLog->append(previousEnts);
		sm->becomeCandidate();
		sm->becomeLeader();
		sm->prs[2]->State = tt.state;
		sm->prs[2]->Next = tt.next;
		sm->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0, "somedata") }));

		auto &p = sm->prs[2];
		BOOST_REQUIRE_EQUAL(p->Next, tt.wnext);
	}
}

BOOST_AUTO_TEST_CASE(TestSendAppendForProgressProbe) {
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	r->readMessages();
	r->prs[2]->becomeProbe();

	// each round is a heartbeat
	for (size_t i = 0; i < 3; i++) {
		if (i == 0) {
			// we expect that raft will only send out one msgAPP on the first
			// loop. After that, the follower is paused until a heartbeat response is
			// received.
			mustAppendEntry(r.get(), { makeEntry(0, 0, "somedata") });
			r->sendAppend(2);
			auto msg = r->readMessages();
			BOOST_REQUIRE_EQUAL(msg.size(), 1);
			BOOST_REQUIRE_EQUAL(msg[0]->index(), 0);
		}

		BOOST_REQUIRE_EQUAL(r->prs[2]->IsPaused(), true);
		for (size_t j = 0; j < 10; j++) {
			mustAppendEntry(r.get(), { makeEntry(0, 0, "somedata") });
			r->sendAppend(2);
			auto msg = r->readMessages();
			BOOST_REQUIRE_EQUAL(msg.size(), 0);
		}

		// do a heartbeat
		for (size_t j = 0; j < r->heartbeatTimeout; j++) {
			r->Step(*make_message(1, 1, MsgBeat));
		}
		BOOST_REQUIRE_EQUAL(r->prs[2]->IsPaused(), true);

		// consume the heartbeat
		auto msg = r->readMessages();
		BOOST_REQUIRE_EQUAL(msg.size(), 1);
		BOOST_REQUIRE_EQUAL(msg[0]->type(), MsgHeartbeat);
	}

	// a heartbeat response will allow another message to be sent
	r->Step(*make_message(2, 1, MsgHeartbeatResp));
	auto msg = r->readMessages();
	BOOST_REQUIRE_EQUAL(msg.size(), 1);
	BOOST_REQUIRE_EQUAL(msg[0]->index(), 0);
	BOOST_REQUIRE_EQUAL(r->prs[2]->IsPaused(), true);
}

BOOST_AUTO_TEST_CASE(TestSendAppendForProgressReplicate) {
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	r->readMessages();
	r->prs[2]->becomeReplicate();

	for (size_t i = 0; i < 10; i++) {
		mustAppendEntry(r.get(), {makeEntry(0, 0, "somedata")});
		r->sendAppend(2);
		auto msgs = r->readMessages();
		BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	}
}

BOOST_AUTO_TEST_CASE(TestSendAppendForProgressSnapshot) {
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	r->readMessages();
	r->prs[2]->becomeSnapshot(10);

	for (size_t i = 0; i < 10; i++) {
		mustAppendEntry(r.get(), { makeEntry(0, 0, "somedata") });
		r->sendAppend(2);
		auto msgs = r->readMessages();
		BOOST_REQUIRE_EQUAL(msgs.size(), 0);
	}
}

BOOST_AUTO_TEST_CASE(TestRecvMsgUnreachable) {
	vector<Entry> previousEnts = { makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1) };
	auto s = std::make_shared<MemoryStorage>();
	s->Append(previousEnts);
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, s);
	r->becomeCandidate();
	r->becomeLeader();
	r->readMessages();
	// set node 2 to state replicate
	r->prs[2]->Match = 3;
	r->prs[2]->becomeReplicate();
	r->prs[2]->optimisticUpdate(5);

	r->Step(*make_message(2, 1, MsgUnreachable));

	BOOST_REQUIRE_EQUAL(r->prs[2]->State, ProgressStateProbe);
	BOOST_REQUIRE_EQUAL(r->prs[2]->Match + 1, r->prs[2]->Next);
}

BOOST_AUTO_TEST_CASE(TestRestore) {
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2, 3}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}

	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestRaft(1, { 1, 2 }, 10, 1, storage);
	BOOST_REQUIRE_EQUAL(sm->restore(*s), true);
	BOOST_REQUIRE_EQUAL(sm->raftLog->lastIndex(), s->metadata().index());

	uint64_t t;
	auto err = sm->raftLog->term(s->metadata().index(), t);
	BOOST_REQUIRE_EQUAL(mustTerm(t, err), s->metadata().term());

	auto sg = sm->nodes();
	auto &ss = s->metadata().conf_state();
	BOOST_REQUIRE_EQUAL(sg.size(), ss.nodes().size());
	for (size_t i = 0; i < sg.size(); ++i) {
		BOOST_REQUIRE_EQUAL(sg[i], ss.nodes()[(int)i]);
	}
	BOOST_REQUIRE_EQUAL(sm->restore(*s), false);
}

// TestRestoreWithLearner restores a snapshot which contains learners.
BOOST_AUTO_TEST_CASE(TestRestoreWithLearner) {
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	for (uint64_t i : {3}) {
		s->mutable_metadata()->mutable_conf_state()->add_learners(i);
	}
	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestLearnerRaft(3, { 1, 2 }, { 3 }, 8, 2, storage);
	BOOST_REQUIRE_EQUAL(sm->restore(*s), true);
	BOOST_REQUIRE_EQUAL(sm->raftLog->lastIndex(), s->metadata().index());
	uint64_t t;
	auto err = sm->raftLog->term(s->metadata().index(), t);
	BOOST_REQUIRE_EQUAL(mustTerm(t, err), s->metadata().term());
	auto sg = sm->nodes();
	BOOST_REQUIRE_EQUAL(sg.size(), s->metadata().conf_state().nodes().size());
	auto lns = sm->learnerNodes();
	BOOST_REQUIRE_EQUAL(lns.size(), s->metadata().conf_state().learners().size());
	for (auto n : s->metadata().conf_state().nodes()) {
		BOOST_REQUIRE_EQUAL(sm->prs[n]->IsLearner, false);
	}
	for (auto n : s->metadata().conf_state().learners()) {
		BOOST_REQUIRE_EQUAL(sm->learnerPrs[n]->IsLearner, true);
	}
	BOOST_REQUIRE_EQUAL(sm->restore(*s), false);
}

BOOST_AUTO_TEST_CASE(TestRestoreInvalidLearner) {
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	for (uint64_t i : {3}) {
		s->mutable_metadata()->mutable_conf_state()->add_learners(i);
	}

	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestRaft(3, { 1, 2, 3 }, 10, 1, storage);

	BOOST_REQUIRE_EQUAL(sm->isLearner, false);
	BOOST_REQUIRE_EQUAL(sm->restore(*s), false);
}

BOOST_AUTO_TEST_CASE(TestRestoreLearnerPromotion) {
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2, 3}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestLearnerRaft(3, { 1, 2 }, { 3 }, 10, 1, storage);

	BOOST_REQUIRE_EQUAL(sm->isLearner, true);
	BOOST_REQUIRE_EQUAL(sm->restore(*s), true);
	BOOST_REQUIRE_EQUAL(sm->isLearner, false);
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader
BOOST_AUTO_TEST_CASE(TestLearnerReceiveSnapshot) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	for (uint64_t i : {2}) {
		s->mutable_metadata()->mutable_conf_state()->add_learners(i);
	}

	auto n1 = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->restore(*s);

	// Force set n1 appplied index.
	n1->raftLog->appliedTo(n1->raftLog->committed);

	auto nt = newNetwork({ n1, n2 });

	setRandomizedElectionTimeout(n1, n1->electionTimeout);
	for (size_t i = 0; i < n1->electionTimeout; i++) {
		n1->tick();
	}
	
	nt->send(make_message(1, 1, MsgBeat));
	BOOST_REQUIRE_EQUAL(n2->raftLog->committed, n1->raftLog->committed);
}

BOOST_AUTO_TEST_CASE(TestRestoreIgnoreSnapshot) {
	vector<Entry> previousEnts = { makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1) };
	uint64_t commit = 1;
	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestRaft(1, { 1, 2 }, 10, 1, storage);
	sm->raftLog->append(previousEnts);
	sm->raftLog->commitTo(commit);

	auto s = makeSnapshot(commit, 1);
	for (uint64_t i : {1, 2}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}

	// ignore snapshot
	BOOST_REQUIRE_EQUAL(sm->restore(*s), false);
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, commit);

	// ignore snapshot and fast forward commit
	s->mutable_metadata()->set_index(commit + 1);
	BOOST_REQUIRE_EQUAL(sm->restore(*s), false);
	BOOST_REQUIRE_EQUAL(sm->raftLog->committed, commit + 1);
}

BOOST_AUTO_TEST_CASE(TestProvideSnap) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestRaft(1, { 1 }, 10, 1, storage);
	sm->restore(*s);

	sm->becomeCandidate();
	sm->becomeLeader();

	// force set the next of node 2, so that node 2 needs a snapshot
	sm->prs[2]->Next = sm->raftLog->firstIndex();
	sm->Step(*make_message(2, 1, MsgAppResp, sm->prs[2]->Next - 1, 0, true));

	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	auto &m = msgs[0];
	BOOST_REQUIRE_EQUAL(m->type(), MsgSnap);
}

BOOST_AUTO_TEST_CASE(TestIgnoreProvidingSnap) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	auto storage = std::make_shared<MemoryStorage>();
	auto sm = newTestRaft(1, { 1 }, 10, 1, storage);
	sm->restore(*s);

	sm->becomeCandidate();
	sm->becomeLeader();

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
	sm->prs[2]->Next = sm->raftLog->firstIndex() - 1;
	sm->prs[2]->RecentActive = false;

	sm->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));

	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 0);
}

BOOST_AUTO_TEST_CASE(TestRestoreFromSnapMsg) {
	auto s = makeSnapshot(11, 11);
	for (uint64_t i : {1, 2}) {
		s->mutable_metadata()->mutable_conf_state()->add_nodes(i);
	}
	auto m = make_message(1, 2, MsgSnap);
	*m->mutable_snapshot() = *s;

	auto sm = newTestRaft(2, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	sm->Step(*m);

	BOOST_REQUIRE_EQUAL(sm->lead, 1);

	// TODO(bdarnell): what should this test?
}

BOOST_AUTO_TEST_CASE(TestSlowNodeRestore) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);
	for (size_t j = 0; j <= 100; j++) {
		nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	}
	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());
	nextEnts(lead, nt->storage[1].get());
	ConfState cs;
	for (auto n : lead->nodes()) *cs.mutable_nodes()->Add() = n;
	Snapshot sh;
	dynamic_cast<MemoryStorage*>(nt->storage[1].get())->CreateSnapshot(lead->raftLog->applied, &cs, "", sh);
	dynamic_cast<MemoryStorage*>(nt->storage[1].get())->Compact(lead->raftLog->applied);

	nt->recover();
	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
	for (;;) {
		nt->send(make_message(1, 1, MsgBeat));
		if (lead->prs[3]->RecentActive) {
			break;
		}
	}

	// trigger a snapshot
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));

	auto follower = dynamic_cast<testRaft*>(nt->peers[3].get());

	// trigger a commit
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	BOOST_REQUIRE_EQUAL(follower->raftLog->committed, lead->raftLog->committed);
}
// TestStepConfig tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
BOOST_AUTO_TEST_CASE(TestStepConfig) {
	// a raft that cannot make progress
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	auto index = r->raftLog->lastIndex();
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0,"",EntryConfChange) }));
	BOOST_REQUIRE_EQUAL(r->raftLog->lastIndex(), index + 1);
	BOOST_REQUIRE_EQUAL(r->pendingConfIndex, index + 1);
}

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
BOOST_AUTO_TEST_CASE(TestStepIgnoreConfig) {
	// a raft that cannot make progress
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0,"",EntryConfChange) }));
	auto index = r->raftLog->lastIndex();
	auto pendingConfIndex = r->pendingConfIndex;
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0,"",EntryConfChange) }));
	vector<Entry> wents = { makeEntry(3, 1, "", EntryNormal) };
	vector<Entry> ents;
	auto err = r->raftLog->entries(ents, index + 1, noLimit);
	BOOST_REQUIRE_EQUAL(err, OK);
	equal_entrys(ents, wents);
	BOOST_REQUIRE_EQUAL(r->pendingConfIndex, pendingConfIndex);
}

// TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
// based on uncommitted entries.
BOOST_AUTO_TEST_CASE(TestNewLeaderPendingConfig) {
	struct {
		bool addEntry;
		uint64_t wpendingIndex;
	} tests[] = {
		{false, 0},
		{true, 1},
	};
	for (auto &tt : tests) {
		auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
		if (tt.addEntry) {
			mustAppendEntry(r.get(), { makeEntry(0,0,"",EntryNormal) });
		}
		r->becomeCandidate();
		r->becomeLeader();
		BOOST_REQUIRE_EQUAL(r->pendingConfIndex, tt.wpendingIndex);
	}
}


// TestAddNode tests that addNode could update nodes correctly.
BOOST_AUTO_TEST_CASE(TestAddNode) {
	auto r = newTestRaft(1, { 1 }, 10, 1, std::make_shared<MemoryStorage>());
	r->addNode(2);
	auto nodes = r->nodes();
	vector<uint64_t> wnodes = { 1, 2 };
	BOOST_REQUIRE_EQUAL(wnodes.size(), nodes.size());
	for (size_t i = 0; i < wnodes.size(); ++i) {
		BOOST_REQUIRE_EQUAL(wnodes[i], nodes[i]);
	}
}

// TestAddLearner tests that addLearner could update nodes correctly.
BOOST_AUTO_TEST_CASE(TestAddLearner) {
	auto r = newTestRaft(1, { 1 }, 10, 1, std::make_shared<MemoryStorage>());
	r->addLearner(2);
	auto nodes = r->learnerNodes();
	vector<uint64_t> wnodes = { 2 };
	BOOST_REQUIRE_EQUAL(wnodes.size(), nodes.size());
	for (size_t i = 0; i < wnodes.size(); ++i) {
		BOOST_REQUIRE_EQUAL(wnodes[i], nodes[i]);
	}
	BOOST_REQUIRE_EQUAL(r->learnerPrs[2]->IsLearner, true);
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
BOOST_AUTO_TEST_CASE(TestAddNodeCheckQuorum) {
	auto r = newTestRaft(1, { 1 }, 10, 1, std::make_shared<MemoryStorage>());
	r->checkQuorum = true;

	r->becomeCandidate();
	r->becomeLeader();

	for (auto i = 0; i < r->electionTimeout-1; i++) {
		r->tick();
	}

	r->addNode(2);

	// This tick will reach electionTimeout, which triggers a quorum check.
	r->tick();

	// Node 1 should still be the leader after a single tick.
	BOOST_REQUIRE_EQUAL(r->state, StateLeader);

	// After another electionTimeout ticks without hearing from node 2,
	// node 1 should step down.
	for (size_t i = 0; i < r->electionTimeout; i++) {
		r->tick();
	}

	BOOST_REQUIRE_EQUAL(r->state, StateFollower);
}

// TestRemoveNode tests that removeNode could update nodes and
// and removed list correctly.
BOOST_AUTO_TEST_CASE(TestRemoveNode) {
	auto r = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->removeNode(2);
	vector<uint64_t> w = { 1 };
	auto g = r->nodes();
	BOOST_REQUIRE_EQUAL(g.size(), w.size());
	for (size_t i = 0; i < g.size(); i++) {
		BOOST_REQUIRE_EQUAL(g[i], w[i]);
	}

	// remove all nodes from cluster
	r->removeNode(1);
	w = {};
	g = r->nodes();
	BOOST_REQUIRE_EQUAL(g.size(), w.size());
	for (size_t i = 0; i < g.size(); i++) {
		BOOST_REQUIRE_EQUAL(g[i], w[i]);
	}
}

// TestRemoveLearner tests that removeNode could update nodes and
// and removed list correctly.
BOOST_AUTO_TEST_CASE(TestRemoveLearner) {
	auto r = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());
	r->removeNode(2);
	vector<uint64_t> w = { 1 };
	auto g = r->nodes();
	BOOST_REQUIRE_EQUAL(g.size(), w.size());
	for (size_t i = 0; i < g.size(); i++) {
		BOOST_REQUIRE_EQUAL(g[i], w[i]);
	}

	w = {};
	g = r->learnerNodes();
	BOOST_REQUIRE_EQUAL(g.size(), w.size());
	for (size_t i = 0; i < g.size(); i++) {
		BOOST_REQUIRE_EQUAL(g[i], w[i]);
	}

	// remove all nodes from cluster
	r->removeNode(1);
	g = r->nodes();
	BOOST_REQUIRE_EQUAL(g.size(), w.size());
	for (size_t i = 0; i < g.size(); i++) {
		BOOST_REQUIRE_EQUAL(g[i], w[i]);
	}
}

BOOST_AUTO_TEST_CASE(TestPromotable) {
	uint64_t id = 1;
	struct {
		vector<uint64_t> peers;
		bool wp;
	}tests[] = {
		{{1}, true},
		{{1, 2, 3}, true},
		{{}, false},
		{{2, 3}, false},
	};
	for (auto &tt : tests) {
		auto r = newTestRaft(id, std::move(tt.peers), 5, 1, std::make_shared<MemoryStorage>());
		auto g = r->promotable();
		BOOST_REQUIRE_EQUAL(g, tt.wp);
	}
}

BOOST_AUTO_TEST_CASE(TestRaftNodes) {
	struct {
		vector<uint64_t> ids;
		vector<uint64_t> wids;
	}tests[] = {
		{{1, 2, 3},{1, 2, 3},},
		{{3, 2, 1},{1, 2, 3},},
	};
	for (auto &tt : tests) {
		auto r = newTestRaft(1, std::move(tt.ids), 10, 1, std::make_shared<MemoryStorage>());
		auto g = r->nodes();
		BOOST_REQUIRE_EQUAL(g.size(), tt.wids.size());
		for (size_t i = 0; i < g.size(); i++) {
			BOOST_REQUIRE_EQUAL(g[i], tt.wids[i]);
		}
	}
}

void testCampaignWhileLeader(bool preVote);
BOOST_AUTO_TEST_CASE(TestCampaignWhileLeader) {
	testCampaignWhileLeader(false);
}

BOOST_AUTO_TEST_CASE(TestPreCampaignWhileLeader) {
	testCampaignWhileLeader(true);
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
BOOST_AUTO_TEST_CASE(TestCommitAfterRemoveNode) {
	// Create a cluster with two nodes.
	auto s = std::make_shared<MemoryStorage>();
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, s);
	r->becomeCandidate();
	r->becomeLeader();

	// Begin to remove the second node.
	ConfChange cc;
	cc.set_type(ConfChangeRemoveNode);
	cc.set_nodeid(2);
	auto ccData = cc.SerializeAsString();
	r->Step(*make_message(0, 0, MsgProp, 0, 0, false, { makeEntry(0, 0, std::move(ccData), EntryConfChange) }));
	// Stabilize the log and make sure nothing is committed yet.
	auto ents = nextEnts(r.get(), s.get());
	BOOST_REQUIRE_EQUAL(ents.size(), 0);
	auto ccIndex = r->raftLog->lastIndex();

	// While the config change is pending, make another proposal.
	r->Step(*make_message(0, 0, MsgProp, 0, 0, false, { makeEntry(0, 0, "hello", EntryNormal) }));

	// Node 2 acknowledges the config change, committing it.
	r->Step(*make_message(2, 0, MsgAppResp, ccIndex));
	ents = nextEnts(r.get(), s.get());
	BOOST_REQUIRE_EQUAL(ents.size(), 2);
	BOOST_REQUIRE_EQUAL(ents[0].type(), EntryNormal);
	BOOST_REQUIRE_EQUAL(ents[0].data().empty(), true);
	BOOST_REQUIRE_EQUAL(ents[1].type(), EntryConfChange);

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
	r->removeNode(2);
	ents = nextEnts(r.get(), s.get());
	BOOST_REQUIRE_EQUAL(ents.size(), 1);
	BOOST_REQUIRE_EQUAL(ents[0].type(), EntryNormal);
	BOOST_REQUIRE_EQUAL(ents[0].data(), "hello");
}

void checkLeaderTransferState(testRaft *r, StateType state, uint64_t lead);
// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
BOOST_AUTO_TEST_CASE(TestLeaderTransferToUpToDateNode) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	BOOST_REQUIRE_EQUAL(lead->lead, 1);

	// Transfer leadership to 2.
	nt->send(make_message(2, 1, MsgTransferLeader));

	checkLeaderTransferState(lead, StateFollower, 2);

	// After some log replication, transfer leadership back to 1.
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));

	nt->send(make_message(1, 2, MsgTransferLeader));

	checkLeaderTransferState(lead, StateLeader, 1);
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
BOOST_AUTO_TEST_CASE(TestLeaderTransferToUpToDateNodeFromFollower) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	BOOST_REQUIRE_EQUAL(lead->lead, 1);

	// Transfer leadership to 2.
	nt->send(make_message(2, 2, MsgTransferLeader));

	checkLeaderTransferState(lead, StateFollower, 2);

	// After some log replication, transfer leadership back to 1.
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));

	nt->send(make_message(1, 1, MsgTransferLeader));

	checkLeaderTransferState(lead, StateLeader, 1);
}
// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
BOOST_AUTO_TEST_CASE(TestLeaderTransferWithCheckQuorum) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	for (int i = 1; i < 4; i++) {
		auto r = dynamic_cast<testRaft*>(nt->peers[i].get());
		r->checkQuorum = true;
		setRandomizedElectionTimeout(r, r->electionTimeout + i);
	}

	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	auto f = dynamic_cast<testRaft*>(nt->peers[2].get());
	for (int i = 0; i < f->electionTimeout; i++) {
		f->tick();
	}

	nt->send(make_message(1, 1, MsgHup));

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	BOOST_REQUIRE_EQUAL(lead->lead, 1);

	// Transfer leadership to 2.
	nt->send(make_message(2, 1, MsgTransferLeader));

	checkLeaderTransferState(lead, StateFollower, 2);

	// After some log replication, transfer leadership back to 1.
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));

	nt->send(make_message(1, 2, MsgTransferLeader));

	checkLeaderTransferState(lead, StateLeader, 1);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferToSlowFollower) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));

	nt->recover();
	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(lead->prs[3]->Match, 1);

	// Transfer leadership to 3 when node 3 is lack of log.
	nt->send(make_message(3, 1, MsgTransferLeader));

	checkLeaderTransferState(lead, StateFollower, 3);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferAfterSnapshot) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());
	nextEnts(lead, nt->storage[1].get());
	ConfState cs;
	for (auto n : lead->nodes()) *cs.mutable_nodes()->Add() = n;
	Snapshot sh;
	dynamic_cast<MemoryStorage*>(nt->storage[1].get())->CreateSnapshot(lead->raftLog->applied, &cs, "", sh);
	dynamic_cast<MemoryStorage*>(nt->storage[1].get())->Compact(lead->raftLog->applied);

	nt->recover();
	BOOST_REQUIRE_EQUAL(lead->prs[3]->Match, 1);

	// Transfer leadership to 3 when node 3 is lack of snapshot.
	nt->send(make_message(3, 1, MsgTransferLeader));
	// Send pb.MsgHeartbeatResp to leader to trigger a snapshot for node 3.
	nt->send(make_message(3, 1, MsgHeartbeatResp));

	checkLeaderTransferState(lead, StateFollower, 3);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferToSelf) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	// Transfer leadership to self, there will be noop.
	nt->send(make_message(1, 1, MsgTransferLeader));
	checkLeaderTransferState(lead, StateLeader, 1);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferToNonExistingNode) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());
	// Transfer leadership to non-existing node, there will be noop.
	nt->send(make_message(4, 1, MsgTransferLeader));
	checkLeaderTransferState(lead, StateLeader, 1);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferTimeout) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	// Transfer leadership to isolated node, wait for timeout.
	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);
	for (int i = 0; i < lead->heartbeatTimeout; i++) {
		lead->tick();
	}
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	for (int i = 0; i < lead->electionTimeout-lead->heartbeatTimeout; i++) {
		lead->tick();
	}

	checkLeaderTransferState(lead, StateLeader, 1);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferIgnoreProposal) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	// Transfer leadership to isolated node to let transfer pending, then send proposal.
	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	auto err = lead->Step(*make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	BOOST_REQUIRE_EQUAL(err, ErrProposalDropped);
	BOOST_REQUIRE_EQUAL(lead->prs[1]->Match, 1);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferReceiveHigherTermVote) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	// Transfer leadership to isolated node to let transfer pending.
	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	nt->send(make_message(2, 2, MsgHup, 1, 2));

	checkLeaderTransferState(lead, StateFollower, 2);
}

BOOST_AUTO_TEST_CASE(TestLeaderTransferRemoveNode) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->ignore(MsgTimeoutNow);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	// The leadTransferee is removed when leadship transferring.
	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	lead->removeNode(3);

	checkLeaderTransferState(lead, StateLeader, 1);
}

// TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
BOOST_AUTO_TEST_CASE(TestLeaderTransferBack) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	// Transfer leadership back to self.
	nt->send(make_message(1, 1, MsgTransferLeader));

	checkLeaderTransferState(lead, StateLeader, 1);
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
// when last transfer is pending.
BOOST_AUTO_TEST_CASE(TestLeaderTransferSecondTransferToAnotherNode) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	// Transfer leadership to another node.
	nt->send(make_message(2, 1, MsgTransferLeader));

	checkLeaderTransferState(lead, StateFollower, 2);
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
BOOST_AUTO_TEST_CASE(TestLeaderTransferSecondTransferToSameNode) {
	auto nt = newNetwork({ nullptr, nullptr, nullptr });
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(3);

	auto lead = dynamic_cast<testRaft*>(nt->peers[1].get());

	nt->send(make_message(3, 1, MsgTransferLeader));
	BOOST_REQUIRE_EQUAL(lead->leadTransferee, 3);

	for (int i = 0; i < lead->heartbeatTimeout; i++) {
		lead->tick();
	}
	// Second transfer leadership request to the same node.
	nt->send(make_message(3, 1, MsgTransferLeader));

	for (int i = 0; i < lead->electionTimeout-lead->heartbeatTimeout; i++) {
		lead->tick();
	}

	checkLeaderTransferState(lead, StateLeader, 1);
}

// TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateLeader)
BOOST_AUTO_TEST_CASE(TestTransferNonMember) {
	auto r = newTestRaft(1, { 2, 3, 4 }, 5, 1, std::make_shared<MemoryStorage>());
	r->Step(*make_message(2, 1, MsgTimeoutNow));

	r->Step(*make_message(2, 1, MsgVoteResp));
	r->Step(*make_message(3, 1, MsgVoteResp));
	BOOST_REQUIRE_EQUAL(r->state, StateFollower);
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
BOOST_AUTO_TEST_CASE(TestNodeWithSmallerTermCanCompleteElection) {
	auto n1 = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n3 = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	n1->preVote = true;
	n2->preVote = true;
	n3->preVote = true;

	// cause a network partition to isolate node 3
	auto nt = newNetwork({ n1, n2, n3 });
	nt->cut(1, 3);
	nt->cut(2, 3);

	nt->send(make_message(1, 1, MsgHup));

	auto sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);

	sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateFollower);

	nt->send(make_message(3, 3, MsgHup));
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->state, StatePreCandidate);

	nt->send(make_message(2, 2, MsgHup));

	// check whether the term values are expected
	// a.Term == 3
	// b.Term == 3
	// c.Term == 1
	sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 3);

	sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 3);

	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 1);

	// check state
	// a == follower
	// b == leader
	// c == pre-candidate
	sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateFollower);
	sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->state, StatePreCandidate);

	iLog(sm->logger, "going to bring back peer 3 and kill peer 2");
	// recover the network then immediately isolate b which is currently
	// the leader, this is to emulate the crash of b.
	nt->recover();
	nt->cut(2, 1);
	nt->cut(2, 3);

	// call for election
	nt->send(make_message(3, 3, MsgHup));
	nt->send(make_message(1, 1, MsgHup));

	// do we have a leader?
	auto sma = dynamic_cast<testRaft*>(nt->peers[1].get());
	auto smb = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sma->state == StateLeader || smb->state == StateLeader, true);
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
BOOST_AUTO_TEST_CASE(TestPreVoteWithSplitVote) {
	auto n1 = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n3 = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	n1->preVote = true;
	n2->preVote = true;
	n3->preVote = true;

	auto nt = newNetwork({ n1, n2, n3 });
	nt->send(make_message(1, 1, MsgHup));

	// simulate leader down. followers start split vote.
	nt->isolate(1);
	list<MessagePtr> msgs;
	msgs.emplace_back(make_message(2, 2, MsgHup));
	msgs.emplace_back(make_message(3, 3, MsgHup));
	nt->send(msgs);

	// check whether the term values are expected
	// n2.Term == 3
	// n3.Term == 3
	auto sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 3);
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 3);

	// check state
	// n2 == candidate
	// n3 == candidate
	sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateCandidate);
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateCandidate);

	// node 2 election timeout first
	nt->send(make_message(2, 2, MsgHup));

	// check whether the term values are expected
	// n2.Term == 4
	// n3.Term == 4
	sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 4);
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->Term, 4);

	// check state
	// n2 == leader
	// n3 == follower
	sm = dynamic_cast<testRaft*>(nt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateLeader);
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->state, StateFollower);
}

networkptr newPreVoteMigrationCluster();
BOOST_AUTO_TEST_CASE(TestPreVoteMigrationCanCompleteElection) {
	auto nt = newPreVoteMigrationCluster();

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	auto n2 = dynamic_cast<testRaft*>(nt->peers[2].get());
	auto n3 = dynamic_cast<testRaft*>(nt->peers[3].get());

	// simulate leader down
	nt->isolate(1);

	// Call for elections from both n2 and n3.
	nt->send(make_message(3, 3, MsgHup));
	nt->send(make_message(2, 2, MsgHup));

	// check state
	// n2.state == Follower
	// n3.state == PreCandidate
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StatePreCandidate);

	nt->send(make_message(3, 3, MsgHup));
	nt->send(make_message(2, 2, MsgHup));

	// Do we have a leader?
	BOOST_REQUIRE_EQUAL(n2->state != StateLeader && n3->state != StateFollower, false);
}

BOOST_AUTO_TEST_CASE(TestPreVoteMigrationWithFreeStuckPreCandidate) {
	auto nt = newPreVoteMigrationCluster();

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	auto n1 = dynamic_cast<testRaft*>(nt->peers[1].get());
	auto n2 = dynamic_cast<testRaft*>(nt->peers[2].get());
	auto n3 = dynamic_cast<testRaft*>(nt->peers[3].get());

	nt->send(make_message(3, 3, MsgHup));
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StatePreCandidate);

	// Pre-Vote again for safety
	nt->send(make_message(3, 3, MsgHup));
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StatePreCandidate);

	nt->send(make_message(1, 3, MsgHeartbeat, 0, n1->Term));

	// Disrupt the leader so that the stuck peer is freed
	BOOST_REQUIRE_EQUAL(n1->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->Term, n1->Term);
}
#endif
TestRaftPtr entsWithConfig(void (*configFunc)(Config&), const vector<uint64_t> &terms) {
	auto storage = std::make_shared<MemoryStorage>();
	for (size_t i = 0; i < terms.size(); i++) {
		storage->Append(EntryVec{ makeEntry(uint64_t(i + 1), terms[i]) });
	}
	auto cfg = newTestConfig(1, {}, 5, 1, storage);
	if (configFunc) {
		configFunc(cfg);
	}
	auto sm = std::make_shared<testRaft>();
	sm->Init(std::move(cfg));
	sm->reset(terms[terms.size() - 1]);
	return sm;
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
TestRaftPtr votedWithConfig(void(*configFunc)(Config&), uint64_t vote, uint64_t term) {
	auto storage = std::make_shared<MemoryStorage>();
	HardState hs;
	hs.set_vote(vote);
	hs.set_term(term);
	storage->SetHardState(hs);
	auto cfg = newTestConfig(1, {}, 5, 1, storage);
	if (configFunc) {
		configFunc(cfg);
	}
	auto sm = std::make_shared<testRaft>();
	sm->Init(std::move(cfg));
	sm->reset(term);
	return sm;
}

void testLeaderElection(bool preVote) {
	void(*cfg)(Config&) = nullptr;
	auto candState = StateCandidate;
	uint64_t candTerm = 1;
	if (preVote) {
		cfg = preVoteConfig;
		// In pre-vote mode, an election that fails to complete
		// leaves the node in pre-candidate state without advancing
		// the term.
		candState = StatePreCandidate;
		candTerm = 0;
	}
	struct {
		networkptr network;
		StateType state;
		uint64_t expTerm;
	} tests[] = {
		{newNetworkWithConfig(cfg, {nullptr, nullptr, nullptr}), StateLeader, 1},
		{newNetworkWithConfig(cfg, {nullptr, nullptr, nopStepper}), StateLeader, 1},
		{newNetworkWithConfig(cfg, {nullptr, nopStepper, nopStepper}), candState, candTerm},
		{newNetworkWithConfig(cfg, {nullptr, nopStepper, nopStepper, nullptr}), candState, candTerm},
		{newNetworkWithConfig(cfg, {nullptr, nopStepper, nopStepper, nullptr, nullptr}), StateLeader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{ newNetworkWithConfig(cfg,
			{nullptr, entsWithConfig(cfg, {1}), entsWithConfig(cfg, {1}), entsWithConfig(cfg, {1, 1}), nullptr}),
			StateFollower, 1 },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		list<MessagePtr> msgs;
		msgs.emplace_back(make_message(1, 1, MsgHup));
		tt.network->send(msgs);
		testRaft *sm = dynamic_cast<testRaft*>(tt.network->peers[1].get());
		BOOST_REQUIRE_EQUAL(sm->state, tt.state);
		BOOST_REQUIRE_EQUAL(sm->Term, tt.expTerm);
	}
}


void preVoteConfig(Config &c) {
	c.PreVote = true;
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
networkptr newNetworkWithConfig(void (*configFunc)(Config &), const vector<stateMachinePtr> &peers) {
	size_t size = peers.size();
	vector<uint64_t> peerAddrs = idsBySize(size);

	map<uint64_t, stateMachinePtr> npeers;
	map<uint64_t, StoragePtr> nstorage;

	for (size_t j = 0; j < peers.size(); ++j) {
		auto &p = peers[j];
		uint64_t id = peerAddrs[j];
		testRaft *pTestRaft = nullptr;
		blackHole *pBlackHole = nullptr;
		if (!p) {
			nstorage[id] = std::make_shared<MemoryStorage>();
			auto cfg = newTestConfig(id, vector<uint64_t>{ peerAddrs }, 10, 1, nstorage[id]);
			if (configFunc) {
				configFunc(cfg);
			}
			auto r(std::make_unique<testRaft>());
			r->Init(std::move(cfg));
			npeers[id] = std::move(r);
		} else if (pTestRaft = dynamic_cast<testRaft*>(p.get()), pTestRaft) {
			map<uint64_t, bool> learners;
			for (auto it = pTestRaft->learnerPrs.begin(); it != pTestRaft->learnerPrs.end(); ++it) {
				learners[it->first] = true;
			}
			pTestRaft->id = id;
			for (size_t i = 0; i < size; i++) {
				if (learners.find(peerAddrs[i]) != learners.end()) {
					auto pp = std::make_unique<Progress>();
					pp->IsLearner = true;
					pTestRaft->learnerPrs[peerAddrs[i]] = std::move(pp);
				} else {
					pTestRaft->prs[peerAddrs[i]] = std::make_unique<Progress>();
				}
			}
			pTestRaft->reset(pTestRaft->Term);
			npeers[id] = std::move(const_cast<stateMachinePtr&>(p));
		} else if (dynamic_cast<blackHole*>(p.get())) {
			npeers[id] = std::move(const_cast<stateMachinePtr&>(p));
		} else {
			BOOST_THROW_EXCEPTION(std::runtime_error("unexpected state machine type"));
		}
	}
	auto nt = std::make_unique<network>();
	nt->peers = std::move(npeers);
	nt->storage = std::move(nstorage);
	return std::move(nt);
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
networkptr newNetwork(const vector<stateMachinePtr> &peers) {
	return newNetworkWithConfig(nullptr, peers);
}

void network::send(MessagePtr &&msg) {
	list<MessagePtr> msgs;
	msgs.emplace_back(std::move(msg));
	send(msgs);
}

void network::send(list<MessagePtr> &msgs) {
	while (!msgs.empty()) {
		auto &m = msgs.front();
		auto &p = peers[m->to()];
		p->Step(*m);
		auto newMsgs = filter(p->readMessages());
		msgs.pop_front();
		for (auto &p : newMsgs) msgs.emplace_back(std::move(p));
	}
}

void network::drop(uint64_t from, uint64_t to, double perc) {
	dropm[connem{ from, to }] = perc;
}

void network::cut(uint64_t one, uint64_t other) {
	drop(one, other, 2.0); // always drop
	drop(other, one, 2.0); // always drop
}

void network::isolate(uint64_t id) {
	for (size_t i = 0; i < peers.size(); i++) {
		uint64_t nid = uint64_t(i) + 1;
		if (nid != id) {
			drop(id, nid, 1.0); // always drop
			drop(nid, id, 1.0); // always drop
		}
	}
}

void network::ignore(MessageType t) {
	ignorem[t] = true;
}

void network::recover() {
	dropm.clear();
	ignorem.clear();
}

double random_Float64() {
	base_generator_type generator(42);
	boost::uniform_real<> uni_dist(0, 1);
	boost::variate_generator<base_generator_type&, boost::uniform_real<> > uni(generator, uni_dist);
	return uni();
}

vector<MessagePtr> network::filter(vector<MessagePtr> &&msgs) {
	vector<MessagePtr> mm;
	for (auto &m : msgs) {
		if (ignorem[m->type()]) {
			continue;
		}
		switch (m->type()) {
		case MsgHup:
			// hups never go over the network, so don't drop them but panic
			BOOST_THROW_EXCEPTION(std::runtime_error("unexpected msgHup"));
		default:
			auto perc = dropm[connem{ m->from(), m->to() }];
			double n = random_Float64();
			if (n < perc) {
				continue;
			}
		}
		if (msgHook) {
			if (!msgHook(*m)) {
				continue;
			}
		}
		mm.push_back(std::move(m));
	}
	return std::move(mm);
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
template<class TRaftPtr>
void setRandomizedElectionTimeout(TRaftPtr &r, int v) {
	r->randomizedElectionTimeout = v;
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
void testLeaderCycle(bool preVote) {
	void(*cfg)(Config&) = nullptr;
	if (preVote) {
		cfg = preVoteConfig;
	}
	auto n = newNetworkWithConfig(cfg, { nullptr, nullptr, nullptr });
	for (uint64_t campaignerID = 1; campaignerID <= 3; campaignerID++) {
		n->send(make_message(campaignerID, campaignerID, MsgHup));
		for (auto it = n->peers.begin(); it != n->peers.end(); ++it) {
			testRaft *sm = dynamic_cast<testRaft*>(it->second.get());
			BOOST_REQUIRE_EQUAL(sm->id == campaignerID && sm->state != StateLeader, false);
			BOOST_REQUIRE_EQUAL(sm->id != campaignerID && sm->state != StateFollower, false);
		}
	}
}

void testLeaderElectionOverwriteNewerLogs(bool preVote) {
	void(*cfg)(Config&) = nullptr;
	if (preVote) {
		cfg = preVoteConfig;
	}
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
	auto n = newNetworkWithConfig(cfg, {
		entsWithConfig(cfg, {1}),     // Node 1: Won first election
		entsWithConfig(cfg, {1}),     // Node 2: Got logs from node 1
		entsWithConfig(cfg, {2}),     // Node 3: Won second election
		votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
		votedWithConfig(cfg, 3, 2)
		}); // Node 5: Voted but didn't get logs

		// Node 1 campaigns. The election fails because a quorum of nodes
		// know about the election that already happened at term 2. Node 1's
		// term is pushed ahead to 2.
	n->send(make_message(1, 1, MsgHup));
	testRaft* sm1 = dynamic_cast<testRaft*>(n->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm1->state, StateFollower);
	BOOST_REQUIRE_EQUAL(sm1->Term, 2);

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n->send(make_message(1, 1, MsgHup));
	BOOST_REQUIRE_EQUAL(sm1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(sm1->Term, 3);

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	for (auto it = n->peers.begin(); it != n->peers.end(); ++it) {
		testRaft* sm = dynamic_cast<testRaft*>(it->second.get());
		auto entries = sm->raftLog->allEntries();
		BOOST_REQUIRE_EQUAL(entries.size(), 2);
		BOOST_REQUIRE_EQUAL(entries[0].term(), 1);
		BOOST_REQUIRE_EQUAL(entries[1].term(), 3);
	}
}

void testVoteFromAnyState(MessageType vt) {
	for (int st = 0; st < numStates; st++) {
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
		r->Term = 1;

		switch (st) {
		case StateFollower:
			r->becomeFollower(r->Term, 3);
			break;
		case StatePreCandidate:
			r->becomePreCandidate();
			break;
		case StateCandidate:
			r->becomeCandidate();
			break;
		case StateLeader:
			r->becomeCandidate();
			r->becomeLeader();
			break;
		}

		// Note that setting our state above may have advanced r.Term
		// past its initial value.
		auto origTerm = r->Term;
		auto newTerm = r->Term + 1;
		auto msg = make_message(2, 1, vt, 42, newTerm);
		msg->set_logterm(newTerm);
		auto err = r->Step(*msg);
		BOOST_REQUIRE_EQUAL(err, OK);
		BOOST_REQUIRE_EQUAL(r->msgs.size(), 1);
		BOOST_REQUIRE_EQUAL(r->msgs[0]->type(), voteRespMsgType(vt));
		BOOST_REQUIRE_EQUAL(r->msgs[0]->reject(), false);

		// If this was a real vote, we reset our state and term.
		if (vt == MsgVote) {
			BOOST_REQUIRE_EQUAL(r->state, StateFollower);
			BOOST_REQUIRE_EQUAL(r->Term, newTerm);
			BOOST_REQUIRE_EQUAL(r->Vote, 2);
		} else {
			// In a prevote, nothing changes.
			BOOST_REQUIRE_EQUAL(r->state, st);
			BOOST_REQUIRE_EQUAL(r->Term, origTerm);
			// if st == StateFollower or StatePreCandidate, r hasn't voted yet.
			// In StateCandidate or StateLeader, it's voted for itself.
			BOOST_REQUIRE_EQUAL(r->Vote != None && r->Vote != 1, false);
		}
	}
}

// nextEnts returns the appliable entries and updates the applied index
vector<Entry> nextEnts(testRaft *r, Storage *s) {
	// Transfer all unstable entries to "stable" storage.
	vector<Entry> ents;
	dynamic_cast<MemoryStorage*>(s)->Append(r->raftLog->unstableEntries());
	r->raftLog->stableTo(r->raftLog->lastIndex(), r->raftLog->lastTerm());

	ents = r->raftLog->nextEnts();
	r->raftLog->appliedTo(r->raftLog->committed);
	return std::move(ents);
}

void testRecvMsgVote(MessageType msgType) {
	struct {
		StateType state;
		uint64_t index, logTerm;
		uint64_t voteFor;
		bool wreject;
	} tests[] = {
		{StateFollower, 0, 0, None, true},
		{ StateFollower, 0, 1, None, true },
		{ StateFollower, 0, 2, None, true },
		{ StateFollower, 0, 3, None, false },

		{ StateFollower, 1, 0, None, true },
		{ StateFollower, 1, 1, None, true },
		{ StateFollower, 1, 2, None, true },
		{ StateFollower, 1, 3, None, false },

		{ StateFollower, 2, 0, None, true },
		{ StateFollower, 2, 1, None, true },
		{ StateFollower, 2, 2, None, false },
		{ StateFollower, 2, 3, None, false },

		{ StateFollower, 3, 0, None, true },
		{ StateFollower, 3, 1, None, true },
		{ StateFollower, 3, 2, None, false },
		{ StateFollower, 3, 3, None, false },

		{ StateFollower, 3, 2, 2, false },
		{ StateFollower, 3, 2, 1, true },

		{ StateLeader, 3, 3, 1, true },
		{ StatePreCandidate, 3, 3, 1, true },
		{ StateCandidate, 3, 3, 1, true },
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		auto &tt = tests[i];
		auto sm = newTestRaft(1, { 1 }, 10, 1, std::make_unique<MemoryStorage>());
		sm->state = tt.state;
		switch (tt.state) {
		case StateFollower:
			sm->step = stepFollower;
			break;
		case StateCandidate:
		case StatePreCandidate:
			sm->step = stepCandidate;
			break;
		case StateLeader:
			sm->step = stepLeader;
			break;
		}
		sm->Vote = tt.voteFor;
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{ {}, makeEntry(1, 2), makeEntry(2, 2) });
		sm->raftLog = std::make_unique<raft_log>(storage, &DefaultLogger::instance());
		sm->raftLog->unstable.offset = 3;

		// raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
		// test we're only testing MsgVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
		auto term = std::max(sm->raftLog->lastTerm(), tt.logTerm);
		sm->Term = term;
		auto msg = make_message(2, 0, msgType, tt.index, term);
		msg->set_logterm(tt.logTerm);
		sm->Step(*msg);

		auto msgs = sm->readMessages();
		BOOST_REQUIRE_EQUAL(msgs.size(), 1);
		BOOST_REQUIRE_EQUAL(msgs[0]->type(), voteRespMsgType(msgType));
		BOOST_REQUIRE_EQUAL(msgs[0]->reject(), tt.wreject);
	}
}

// testCandidateResetTerm tests when a candidate receives a
// MsgHeartbeat or MsgApp from leader, "Step" resets the term
// with leader's and reverts back to follower.
void testCandidateResetTerm(MessageType mt) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	auto nt = newNetwork({ a, b, c });

	nt->send(make_message(1, 1, MsgHup));
	BOOST_REQUIRE_EQUAL(a->state, StateLeader);
	BOOST_REQUIRE_EQUAL(b->state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->state, StateFollower);

	// isolate 3 and increase term in rest
	nt->isolate(3);

	nt->send(make_message(2, 2, MsgHup));
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->state, StateLeader);
	BOOST_REQUIRE_EQUAL(b->state, StateFollower);

	// trigger campaign in isolated c
	c->resetRandomizedElectionTimeout();
	for (size_t i = 0; i < c->randomizedElectionTimeout; i++) {
		c->tick();
	}

	BOOST_REQUIRE_EQUAL(c->state, StateCandidate);

	nt->recover();

	// leader sends to isolated candidate
	// and expects candidate to revert to follower

	nt->send(make_message(1, 3, mt, 0, a->Term));

	BOOST_REQUIRE_EQUAL(c->state, StateFollower);

	// follower c term is reset with leader's
	BOOST_REQUIRE_EQUAL(a->Term, c->Term);
}

void mustAppendEntry(testRaft *r, vector<Entry> &&ents) {
	if (!r->appendEntry(ents)) {
		BOOST_THROW_EXCEPTION(std::runtime_error("entry unexpectedly dropped"));
	}
}

void testCampaignWhileLeader(bool preVote) {
	auto cfg = newTestConfig(1, { 1 }, 5, 1, std::make_shared<MemoryStorage>());
	cfg.PreVote = preVote;
	auto r(std::make_unique<testRaft>());
	r->Init(std::move(cfg));
	BOOST_REQUIRE_EQUAL(r->state, StateFollower);
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	r->Step(*make_message(1, 1, MsgHup));
	BOOST_REQUIRE_EQUAL(r->state, StateLeader);
	auto term = r->Term;
	r->Step(*make_message(1, 1, MsgHup));
	BOOST_REQUIRE_EQUAL(r->state, StateLeader);
	BOOST_REQUIRE_EQUAL(r->Term, term);
}

void checkLeaderTransferState(testRaft *r, StateType state, uint64_t lead) {
	BOOST_REQUIRE_EQUAL(r->state, state);
	BOOST_REQUIRE_EQUAL(r->lead, lead);
	BOOST_REQUIRE_EQUAL(r->leadTransferee, None);
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
networkptr newPreVoteMigrationCluster() {
	auto n1 = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n3 = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	n1->preVote = true;
	n2->preVote = true;
	// We intentionally do not enable PreVote for n3, this is done so in order
	// to simulate a rolling restart process where it's possible to have a mixed
	// version cluster with replicas with PreVote enabled, and replicas without.

	auto nt = newNetwork({ n1, n2, n3 });
	nt->send(make_message(1, 1, MsgHup));

	// Cause a network partition to isolate n3.
	nt->isolate(3);
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));
	nt->send(make_message(3, 3, MsgHup));
	nt->send(make_message(3, 3, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateCandidate
	BOOST_REQUIRE_EQUAL(n1->state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->state, StateCandidate);

	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 4
	BOOST_REQUIRE_EQUAL(n1->Term, 2);
	BOOST_REQUIRE_EQUAL(n2->Term, 2);
	BOOST_REQUIRE_EQUAL(n3->Term, 4);

	// Enable prevote on n3, then recover the network
	n3->preVote = true;
	nt->recover();

	return nt;
}
