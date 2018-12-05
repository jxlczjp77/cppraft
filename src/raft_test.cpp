#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
#include <progress.hpp>
#include <boost/throw_exception.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/random/linear_congruential.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
#include <boost/format.hpp>
using namespace raft;
using namespace raftpb;
typedef boost::minstd_rand base_generator_type;

typedef std::unique_ptr<Progress> ProgressPtr;
typedef std::unique_ptr<inflights> InflightsPtr;
typedef std::unique_ptr<Raft> RaftPtr;

void preVoteConfig(Config &c);

MessagePtr make_message(uint64_t from, uint64_t to, MessageType type, uint64_t index = 0, uint64_t term = 0, bool reject = false, vector<Entry> &&ents = vector<Entry>()) {
	MessagePtr msg = make_unique<Message>();
	msg->set_from(from);
	msg->set_to(to);
	msg->set_type(type);
	msg->set_term(term);
	msg->set_reject(reject);
	msg->set_index(index);
	auto dd = msg->mutable_entries();
	for (auto &ent : ents) *dd->Add() = ent;
	return std::move(msg);
}

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
	return std::move(r->m_msgs);
}

// PayloadSize is the size of the payload of this Entry. Notably, it does not
// depend on its Index or Term.
int PayloadSize(const Entry &e) {
	return int(e.data().size());
}

struct stateMachine {
	virtual ErrorCode Step(Message &m) = 0;
	virtual vector<MessagePtr> readMessages() = 0;
};
typedef std::shared_ptr<stateMachine> stateMachinePtr;
typedef std::unique_ptr<MemoryStorage> MemoryStoragePtr;

struct connem {
	uint64_t from, to;
	friend bool operator<(const connem &l, const connem &r) { 
		return (l.from < r.from) || (l.from == r.from && l.to < r.to);
	}
};

struct blackHole : stateMachine {
	ErrorCode Step(Message &m) { return OK; }
	vector<MessagePtr> readMessages() { return {}; }
};

stateMachinePtr nopStepper = std::make_unique<blackHole>();

struct testRaft : public stateMachine, public Raft {
	testRaft(Config &c): Raft(c) {}
	ErrorCode Step(Message &m) { return Raft::Step(m); }
	vector<MessagePtr> readMessages() { return std::move(Raft::m_msgs); }
};
typedef std::shared_ptr<testRaft> TestRaftPtr;
vector<Entry> nextEnts(testRaft *r, Storage *s);

struct network {
	map<uint64_t, stateMachinePtr> peers;
	map<uint64_t, StoragePtr> storage;
	map<connem, double> dropm;
	map<MessageType, bool> ignorem;

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	bool (*msgHook)(const Message &m);

	void send(MessagePtr &&msg);
	void send(list<MessagePtr> &msgs);
	void drop(uint64_t from, uint64_t to, double perc);
	void cut(uint64_t one, uint64_t other);
	void isolate(uint64_t id);
	void ignore(MessageType t);
	void recover();
	vector<MessagePtr> filter(vector<MessagePtr> &&msgs);
};
typedef std::unique_ptr<network> networkptr;
networkptr newNetworkWithConfig(void(*configFunc)(Config &), const vector<stateMachinePtr> &peers);
networkptr newNetwork(const vector<stateMachinePtr> &peers);

TestRaftPtr newTestRaft(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage) {
	auto config = newTestConfig(id, std::move(peers), election, heartbeat, storage);
	return std::make_shared<testRaft>(config);
}

TestRaftPtr newTestLearnerRaft(uint64_t id, vector<uint64_t> &&peers, vector<uint64_t> &&learners, int election, int heartbeat, StoragePtr storage) {
	auto cfg = newTestConfig(id, std::move(peers), election, heartbeat, storage);
	cfg.learners = learners;
	return std::make_shared<testRaft>(cfg);
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
	auto r = newTestRaft(1, { 1, 2 }, 5, 1, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	r->m_prs[2]->becomeReplicate();
	// Send proposals to r1. The first 5 entries should be appended to the log.
	auto propMsg = make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0, "foo") });
	for (size_t i = 0; i < 5; i++) {
		auto &pr = r->m_prs[r->m_id];
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

	r->m_prs[2]->Paused = true;

	r->Step(*make_message(1, 1, MsgBeat));
	BOOST_REQUIRE_EQUAL(r->m_prs[2]->Paused, true);

	r->m_prs[2]->becomeReplicate();
	r->Step(*make_message(2, 1, MsgHeartbeatResp));
	BOOST_REQUIRE_EQUAL(r->m_prs[2]->Paused, false);
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
	RaftPtr r = std::make_unique<Raft>(cfg);
	r->becomeCandidate();
	r->becomeLeader();

	// Throw away all the messages relating to the initial election.
	readMessages(r);

	// While node 2 is in probe state, propose a bunch of entries.
	r->m_prs[2]->becomeProbe();
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
	const int maxEntrySize = maxEntries * PayloadSize(testEntry);
	auto cfg = newTestConfig(1, { 1, 2, 3 }, 5, 1, std::make_shared<MemoryStorage>());
	cfg.MaxUncommittedEntriesSize = uint64_t(maxEntrySize);
	cfg.MaxInflightMsgs = 2 * 1024; // avoid interference
	RaftPtr r = std::make_unique<Raft>(cfg);
	r->becomeCandidate();
	r->becomeLeader();
	BOOST_REQUIRE_EQUAL(r->m_uncommittedSize, 0);

	// Set the two followers to the replicate state. Commit to tail of log.
	const int numFollowers = 2;
	r->m_prs[2]->becomeReplicate();
	r->m_prs[3]->becomeReplicate();
	r->m_uncommittedSize = 0;

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
	BOOST_REQUIRE_EQUAL(r->m_uncommittedSize, 0);

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
	BOOST_REQUIRE_EQUAL(r->m_uncommittedSize, 0);
}
void testLeaderElection(bool preVote);
BOOST_AUTO_TEST_CASE(TestLeaderElection) {
	testLeaderElection(false);
}

BOOST_AUTO_TEST_CASE(TestLeaderElectionPreVote) {
	testLeaderElection(true);
}

template<class TRaftPtr>
void setRandomizedElectionTimeout(TRaftPtr &r, int v);
// TestLearnerElectionTimeout verfies that the leader should not start election even
// when times out.
BOOST_AUTO_TEST_CASE(TestLearnerElectionTimeout) {
	auto n1 = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);

	// n2 is learner. Learner should not start election even when times out.
	setRandomizedElectionTimeout(n2, n2->m_electionTimeout);
	for (size_t i = 0; i < n2->m_electionTimeout; i++) {
		n2->m_tick();
	}

	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
BOOST_AUTO_TEST_CASE(TestLearnerPromotion) {
	auto n1 = newTestLearnerRaft(1, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);

	auto nt = newNetwork({ n1, n2 });

	BOOST_REQUIRE_NE(n1->m_state, StateLeader);

	// n1 should become leader
	setRandomizedElectionTimeout(n1, n1->m_electionTimeout);
	for (size_t i = 0; i < n1->m_electionTimeout; i++) {
		n1->m_tick();
	}

	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);

	nt->send(make_message(1, 1, MsgBeat));

	n1->addNode(2);
	n2->addNode(2);
	BOOST_REQUIRE_EQUAL(n2->m_isLearner, false);

	// n2 start election, should become leader
	setRandomizedElectionTimeout(n2, n2->m_electionTimeout);
	for (size_t i = 0; i < n2->m_electionTimeout; i++) {
		n2->m_tick();
	}

	nt->send(make_message(2, 2, MsgBeat));

	BOOST_REQUIRE_EQUAL(n1->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateLeader);
}

// TestLearnerCannotVote checks that a learner can't vote even it receives a valid Vote request.
BOOST_AUTO_TEST_CASE(TestLearnerCannotVote) {
	auto n2 = newTestLearnerRaft(2, { 1 }, { 2 }, 10, 1, std::make_shared<MemoryStorage>());

	n2->becomeFollower(1, None);

	auto msg = make_message(1, 2, MsgVote, 11, 2);
	msg->set_logterm(11);
	n2->Step(*msg);

	BOOST_REQUIRE_EQUAL(n2->m_msgs.size(), 0);
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
			BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, tt.wcommitted);

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

	setRandomizedElectionTimeout(n1, n1->m_electionTimeout);
	for (size_t i = 0; i < n1->m_electionTimeout; i++) {
		n1->m_tick();
	}
	
	nt->send(make_message(1, 1, MsgBeat));

		// n1 is leader and n2 is learner
	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->m_isLearner, true);


	auto nextCommitted = n1->m_raftLog->m_committed + 1;
	
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	BOOST_REQUIRE_EQUAL(n1->m_raftLog->m_committed, nextCommitted);
	BOOST_REQUIRE_EQUAL(n1->m_raftLog->m_committed, n2->m_raftLog->m_committed);

	auto match = n1->getProgress(2)->Match;
	BOOST_REQUIRE_EQUAL(n2->m_raftLog->m_committed, match);
}

BOOST_AUTO_TEST_CASE(TestSingleNodeCommit) {
	auto tt = newNetwork({ nullptr });
	make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") });
	tt->send(make_message(1, 1, MsgHup));
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	tt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 3);
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
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 1);

	// network recovery
	tt->recover();
	// avoid committing ChangeTerm proposal
	tt->ignore(MsgApp);

	// elect 2 as the new leader with term 2
	tt->send(make_message(2, 2, MsgHup));

	// no log entries from previous term should be committed
	sm = dynamic_cast<testRaft*>(tt->peers[2].get());
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 1);

	tt->recover();
	// send heartbeat; reset wait
	tt->send(make_message(2, 2, MsgBeat));
	// append an entry at current term
	tt->send(make_message(2, 2, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));
	// expect the committed to be advanced
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 5);
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
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 1);

	// network recovery
	tt->recover();

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt->send(make_message(2, 2, MsgHup));
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 4);
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
	BOOST_REQUIRE_EQUAL(sm->m_state, StateLeader);

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->m_state, StateCandidate);

	nt->recover();

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	nt->send(make_message(3, 3, MsgHup));

	auto storage = std::make_shared<MemoryStorage>();
	storage->append({ {}, makeEntry(1, 1) });
	raft_log wlog(storage, &DefaultLogger::instance());
	wlog.m_committed = 1;
	wlog.m_unstable.m_offset = 2;
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
		BOOST_REQUIRE_EQUAL(tt.sm->m_state, tt.state);
		BOOST_REQUIRE_EQUAL(tt.sm->m_Term, tt.term);
		auto base = ltoa(tt.raftLog);
		auto sm = dynamic_cast<testRaft*>(nt->peers[1 + i].get());
		if (sm) {
			auto l = ltoa(sm->m_raftLog.get());
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
	auto a = std::make_shared<testRaft>(cfgA);
	auto b = std::make_shared<testRaft>(cfgB);
	auto c = std::make_shared<testRaft>(cfgC);

	auto nt = newNetwork({ a, b, c });
	nt->cut(1, 3);

	nt->send(make_message(1, 1, MsgHup));
	nt->send(make_message(3, 3, MsgHup));

	// 1 becomes leader since it receives votes from 1 and 2
	auto sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->m_state, StateLeader);

	// 3 campaigns then reverts to follower when its PreVote is rejected
	sm = dynamic_cast<testRaft*>(nt->peers[3].get());
	BOOST_REQUIRE_EQUAL(sm->m_state, StateFollower);

	nt->recover();

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
	nt->send(make_message(3, 3, MsgHup));


	auto storage = std::make_shared<MemoryStorage>();
	storage->append({ {}, makeEntry(1, 1) });
	raft_log wlog(storage, &DefaultLogger::instance());
	wlog.m_committed = 1;
	wlog.m_unstable.m_offset = 2;
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
		BOOST_REQUIRE_EQUAL(tt.sm->m_state, tt.state);
		BOOST_REQUIRE_EQUAL(tt.sm->m_Term, tt.term);

		auto base = ltoa(tt.raftLog);
		auto sm = dynamic_cast<testRaft*>(nt->peers[1 + i].get());
		if (sm) {
			auto l = ltoa(sm->m_raftLog.get());
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
	BOOST_REQUIRE_EQUAL(a->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(a->m_Term, 1);
	auto storage = std::make_shared<MemoryStorage>();
	storage->append({ {}, makeEntry(1, 1), makeEntry(2, 1, string(data)) });
	raft_log wlog(storage, &DefaultLogger::instance());
	wlog.m_committed = 2;
	wlog.m_unstable.m_offset = 3;
	auto wantLog = ltoa(&wlog);
	for (size_t i = 0; i < tt->peers.size(); i++) {
		auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
		if (sm) {
			auto l = ltoa(sm->m_raftLog.get());
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
	BOOST_REQUIRE_EQUAL(sm->m_state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestSingleNodePreCandidate) {
	auto tt = newNetworkWithConfig(preVoteConfig, { nullptr });
	tt->send(make_message(1, 1, MsgHup));

	auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->m_state, StateLeader);
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
	storage->append({ {}, makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 3, "somedata") });
	raft_log ilog(storage, &DefaultLogger::instance());
	ilog.m_committed = 4;
	ilog.m_unstable.m_offset = 5;
	auto base = ltoa(&ilog);
	for (size_t i = 0; i < tt->peers.size(); i++) {
		auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
		if (sm) {
			auto l = ltoa(sm->m_raftLog.get());
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
			storage->append({ {}, makeEntry(1, 1), makeEntry(2, 1, data) });
			wantLog = std::make_unique<raft_log>(storage, &DefaultLogger::instance());
			wantLog->m_committed = 2;
			wantLog->m_unstable.m_offset = 3;
		}
		auto base = ltoa(wantLog.get());
		for (size_t i = 0; i < tt.network->peers.size(); i++) {
			auto sm = dynamic_cast<testRaft*>(tt.network->peers[i].get());
			if (sm) {
				auto l = ltoa(sm->m_raftLog.get());
				auto g = diffu(base, l);
				BOOST_REQUIRE_EQUAL(g, "");
			} else {
				std::cout << (boost::format("#%d: empty log") % i).str() << endl;
			}
		}
		auto sm = dynamic_cast<testRaft*>(tt.network->peers[1].get());
		BOOST_REQUIRE_EQUAL(sm->m_Term, 1);
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
		storage->append({ {}, makeEntry(1, 1), makeEntry(2, 1, data) });
		raft_log wantLog(storage, &DefaultLogger::instance());
		wantLog.m_committed = 2;
		wantLog.m_unstable.m_offset = 3;
		auto base = ltoa(&wantLog);
		for (size_t i = 0; i < tt->peers.size(); i++) {
			auto sm = dynamic_cast<testRaft*>(tt->peers[i].get());
			if (sm) {
				auto l = ltoa(sm->m_raftLog.get());
				auto g = diffu(base, l);
				BOOST_REQUIRE_EQUAL(g, "");
			} else {
				std::cout << (boost::format("#%d: empty log") % i).str() << endl;
			}
		}
		auto sm = dynamic_cast<testRaft*>(tt->peers[1].get());
		BOOST_REQUIRE_EQUAL(sm->m_Term, 1);
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
		storage->append(tt.logs);
		HardState hs;
		hs.set_term(tt.smTerm);
		storage->SetHardState(hs);

		auto sm = newTestRaft(1, { 1 }, 10, 2, storage);
		for (size_t j = 0; j < tt.matches.size(); j++) {
			sm->setProgress(uint64_t(j) + 1, tt.matches[j], tt.matches[j] + 1, false);
		}
		sm->maybeCommit();
		BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, tt.w);
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
		sm->m_electionElapsed = tt.elapse;
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
	sm->m_step = fakeStep;
	sm->m_Term = 2;
	sm->Step(*make_message(0, 0, MsgApp, 0, sm->m_Term - 1));
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
		storage->append({ makeEntry(1, 1), makeEntry(2, 2) });
		auto sm = newTestRaft(1, { 1 }, 10, 1, storage);
		sm->becomeFollower(2, None);

		sm->handleAppendEntries(*tt.m);
		BOOST_REQUIRE_EQUAL(sm->m_raftLog->lastIndex(), tt.wIndex);
		BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, tt.wCommit);
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
		storage->append({ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) });
		auto sm = newTestRaft(1, { 1, 2 }, 5, 1, storage);
		sm->becomeFollower(2, 2);
		sm->m_raftLog->commitTo(commit);
		sm->handleHeartbeat(*tt.m);
		BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, tt.wCommit);

		auto m = sm->readMessages();
		BOOST_REQUIRE_EQUAL(m.size(), 1);
		BOOST_REQUIRE_EQUAL(m[0]->type(), MsgHeartbeatResp);
	}
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
BOOST_AUTO_TEST_CASE(TestHandleHeartbeatResp) {
	auto storage = std::make_shared<MemoryStorage>();
	storage->append({ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) });
	auto sm = newTestRaft(1, { 1, 2 }, 5, 1, storage);
	sm->becomeCandidate();
	sm->becomeLeader();
	sm->m_raftLog->commitTo(sm->m_raftLog->lastIndex());

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
	sm->m_raftLog->commitTo(sm->m_raftLog->lastIndex());

	auto ctx = "ctx";

	// leader starts linearizable read request.
	// more info: raft dissertation 6.4, step 2.
	sm->Step(*make_message(2, 0, MsgReadIndex, 0, 0, false, { makeEntry(0,0,string(ctx)) }));
	auto msgs = sm->readMessages();
	BOOST_REQUIRE_EQUAL(msgs.size(), 1);
	BOOST_REQUIRE_EQUAL(msgs[0]->type(), MsgHeartbeat);
	BOOST_REQUIRE_EQUAL(msgs[0]->context(), ctx);
	BOOST_REQUIRE_EQUAL(sm->m_readOnly->readIndexQueue.size(), 1);
	BOOST_REQUIRE_EQUAL(sm->m_readOnly->pendingReadIndex.size(), 1);
	BOOST_REQUIRE_EQUAL(sm->m_readOnly->pendingReadIndex.find(string(ctx)) !=
		sm->m_readOnly->pendingReadIndex.end(), true);

	// heartbeat responses from majority of followers (1 in this case)
	// acknowledge the authority of the leader.
	// more info: raft dissertation 6.4, step 3.
	auto msg = make_message(2, 0, MsgHeartbeatResp);
	msg->set_context(ctx);
	sm->Step(*msg);
	BOOST_REQUIRE_EQUAL(sm->m_readOnly->readIndexQueue.size(), 0);
	BOOST_REQUIRE_EQUAL(sm->m_readOnly->pendingReadIndex.size(), 0);
	BOOST_REQUIRE_EQUAL(sm->m_readOnly->pendingReadIndex.find(string(ctx)) ==
		sm->m_readOnly->pendingReadIndex.end(), true);
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
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 1);
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
			sm->m_state = tt.from;

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
			BOOST_REQUIRE_EQUAL(sm->m_Term, tt.wterm);
			BOOST_REQUIRE_EQUAL(sm->m_lead, tt.wlead);
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
			BOOST_REQUIRE_EQUAL(sm->m_state, tt.wstate);
			BOOST_REQUIRE_EQUAL(sm->m_Term, tt.wterm);
			BOOST_REQUIRE_EQUAL(sm->m_raftLog->lastIndex(), tt.windex);
			BOOST_REQUIRE_EQUAL(sm->m_raftLog->allEntries().size(), tt.windex);

			uint64_t wlead = 2;
			if (msgType == MsgVote) {
				wlead = None;
			}
			BOOST_REQUIRE_EQUAL(sm->m_lead, wlead);
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

	sm->m_checkQuorum = true;

	sm->becomeCandidate();
	sm->becomeLeader();

	for (size_t i = 0; i < sm->m_electionTimeout + 1; i++) {
		sm->Step(*make_message(2, 0, MsgHeartbeatResp, 0, sm->m_Term));
		sm->m_tick();
	}
	BOOST_REQUIRE_EQUAL(sm->m_state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestLeaderStepdownWhenQuorumLost) {
	auto sm = newTestRaft(1, { 1, 2, 3 }, 5, 1, std::make_shared<MemoryStorage>());

	sm->m_checkQuorum = true;

	sm->becomeCandidate();
	sm->becomeLeader();

	for (size_t i = 0; i < sm->m_electionTimeout + 1; i++) {
		sm->m_tick();
	}

	BOOST_REQUIRE_EQUAL(sm->m_state, StateFollower);
}

BOOST_AUTO_TEST_CASE(TestLeaderSupersedingWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	a->m_checkQuorum = true;
	b->m_checkQuorum = true;
	c->m_checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 1);

	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}

	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(c->m_state, StateFollower);

	nt->send(make_message(3, 3, MsgHup));

	// Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
	BOOST_REQUIRE_EQUAL(c->m_state, StateCandidate);

	// Letting b's electionElapsed reach to electionTimeout
	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}
	nt->send(make_message(3, 3, MsgHup));
	BOOST_REQUIRE_EQUAL(c->m_state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestLeaderElectionWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	a->m_checkQuorum = true;
	b->m_checkQuorum = true;
	c->m_checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(a, a->m_electionTimeout + 1);
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 2);

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(c->m_state, StateFollower);

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
	setRandomizedElectionTimeout(a, a->m_electionTimeout + 1);
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 2);
	for (size_t i = 0; i < a->m_electionTimeout; i++) {
		a->m_tick();
	}
	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->m_state, StateLeader);
}


// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
BOOST_AUTO_TEST_CASE(TestFreeStuckCandidateWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	a->m_checkQuorum = true;
	b->m_checkQuorum = true;
	c->m_checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 1);

	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	nt->isolate(1);
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(b->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->m_state, StateCandidate);
	BOOST_REQUIRE_EQUAL(c->m_Term, b->m_Term + 1);

	// Vote again for safety
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(b->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->m_state, StateCandidate);
	BOOST_REQUIRE_EQUAL(c->m_Term, b->m_Term + 2);

	nt->recover();
	nt->send(make_message(1, 3, MsgHeartbeat, 0, a->m_Term));

	// Disrupt the leader so that the stuck peer is freed
	BOOST_REQUIRE_EQUAL(a->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->m_Term, a->m_Term);

	// Vote again, should become leader this time
	nt->send(make_message(3, 3, MsgHup));

	BOOST_REQUIRE_EQUAL(c->m_state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestNonPromotableVoterWithCheckQuorum) {
	auto a = newTestRaft(1, { 1, 2 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1 }, 10, 1, std::make_shared<MemoryStorage>());

	a->m_checkQuorum = true;
	b->m_checkQuorum = true;

	auto nt = newNetwork({ a, b });
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 1);
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
	b->delProgress(2);

	BOOST_REQUIRE_EQUAL(b->promotable(), false);

	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(b->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(b->m_lead, 1);
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

	n1->m_checkQuorum = true;
	n2->m_checkQuorum = true;
	n3->m_checkQuorum = true;

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	auto nt = newNetwork({ n1, n2, n3 });

	nt->send(make_message(1, 1, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateFollower
	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->m_state, StateFollower);

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	setRandomizedElectionTimeout(n3, n3->m_electionTimeout + 2);
	for (size_t i = 0; i < n3->m_randomizedElectionTimeout - 1; i++) {
		n3->m_tick();
	}

	// ideally, before last election tick elapses,
	// the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
	// from leader n1, and then resets its "electionElapsed"
	// however, last tick may elapse before receiving any
	// messages from leader, thus triggering campaign
	n3->m_tick();

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateCandidate
	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->m_state, StateCandidate);

	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 3
	BOOST_REQUIRE_EQUAL(n1->m_Term, 2);
	BOOST_REQUIRE_EQUAL(n2->m_Term, 2);
	BOOST_REQUIRE_EQUAL(n3->m_Term, 3);

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
	nt->send(make_message(1, 3, MsgHeartbeat, 0, n1->m_Term));

	// then candidate n3 responds with "pb.MsgAppResp" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election

	// check state
	// n1.state == StateFollower
	// n2.state == StateFollower
	// n3.state == StateCandidate
	BOOST_REQUIRE_EQUAL(n1->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->m_state, StateCandidate);

	// check term
	// n1.Term == 3
	// n2.Term == 2
	// n3.Term == 3
	BOOST_REQUIRE_EQUAL(n1->m_Term, 3);
	BOOST_REQUIRE_EQUAL(n2->m_Term, 2);
	BOOST_REQUIRE_EQUAL(n3->m_Term, 3);
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

	n1->m_checkQuorum = true;
	n2->m_checkQuorum = true;
	n3->m_checkQuorum = true;

	n1->becomeFollower(1, None);
	n2->becomeFollower(1, None);
	n3->becomeFollower(1, None);

	auto nt = newNetwork({ n1, n2, n3 });

	nt->send(make_message(1, 1, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StateFollower
	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->m_state, StateFollower);

	nt->isolate(3);
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "somedata") }));
	n1->m_preVote = true;
	n2->m_preVote = true;
	n3->m_preVote = true;
	nt->recover();
	nt->send(make_message(3, 3, MsgHup));

	// check state
	// n1.state == StateLeader
	// n2.state == StateFollower
	// n3.state == StatePreCandidate
	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(n2->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(n3->m_state, StatePreCandidate);

	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 2
	BOOST_REQUIRE_EQUAL(n1->m_Term, 2);
	BOOST_REQUIRE_EQUAL(n2->m_Term, 2);
	BOOST_REQUIRE_EQUAL(n3->m_Term, 2);

	// delayed leader heartbeat does not force current leader to step down
	nt->send(make_message(1, 3, MsgHeartbeat, 0, n1->m_Term));
	BOOST_REQUIRE_EQUAL(n1->m_state, StateLeader);
}

BOOST_AUTO_TEST_CASE(TestReadOnlyOptionSafe) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 1);

	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);

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

		nt->send(make_message(tt.sm->m_id, tt.sm->m_id, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, tt.wctx) }));

		auto r = tt.sm;
		BOOST_REQUIRE_GT(r->m_readStates.size(), 0);
		auto &rs = r->m_readStates[0];
		BOOST_REQUIRE_EQUAL(rs.Index, tt.wri);
		BOOST_REQUIRE_EQUAL(rs.RequestCtx, tt.wctx);
		r->m_readStates.clear();
	}
}

BOOST_AUTO_TEST_CASE(TestReadOnlyOptionLease) {
	auto a = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto b = newTestRaft(2, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	auto c = newTestRaft(3, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	a->m_readOnly->option = ReadOnlyLeaseBased;
	b->m_readOnly->option = ReadOnlyLeaseBased;
	c->m_readOnly->option = ReadOnlyLeaseBased;
	a->m_checkQuorum = true;
	b->m_checkQuorum = true;
	c->m_checkQuorum = true;

	auto nt = newNetwork({ a, b, c });
	setRandomizedElectionTimeout(b, b->m_electionTimeout + 1);

	for (size_t i = 0; i < b->m_electionTimeout; i++) {
		b->m_tick();
	}
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);

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

		nt->send(make_message(tt.sm->m_id, tt.sm->m_id, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, tt.wctx) }));

		auto r = tt.sm;
		auto &rs = r->m_readStates[0];
		BOOST_REQUIRE_EQUAL(rs.Index, tt.wri);
		BOOST_REQUIRE_EQUAL(rs.RequestCtx, tt.wctx);
		r->m_readStates.clear();
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
		storage->append({ makeEntry(1, 1), makeEntry(2, 1) });
		HardState hs;
		hs.set_term(1);
		hs.set_commit(c.committed);
		storage->SetHardState(hs);
		if (c.compactIndex != 0) {
			storage->compact(c.compactIndex);
		}
		auto cfg = newTestConfig(c.id, { 1, 2, 3 }, 10, 1, storage);
		cfg.Applied = c.applied;
		auto raft = std::make_shared<testRaft>(cfg);
		peers.push_back(raft);
	}
	auto nt = newNetwork(peers);

	// Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
	nt->ignore(MsgApp);
	// Force peer a to become leader.
	nt->send(make_message(1, 1, MsgHup));

	auto sm = dynamic_cast<testRaft*>(nt->peers[1].get());
	BOOST_REQUIRE_EQUAL(sm->m_state, StateLeader);

	// Ensure peer a drops read only request.
	uint64_t windex = 4;
	auto wctx = "ctx";
	nt->send(make_message(1, 1, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, wctx) }));
	BOOST_REQUIRE_EQUAL(sm->m_readStates.size(), 0);

	nt->recover();

	// Force peer a to commit a log entry at its term
	for (size_t i = 0; i < sm->m_heartbeatTimeout; i++) {
		sm->m_tick();
	}
	nt->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));
	BOOST_REQUIRE_EQUAL(sm->m_raftLog->m_committed, 4);

	uint64_t term_;
	auto err = sm->m_raftLog->term(sm->m_raftLog->m_committed, term_);
	auto lastLogTerm = sm->m_raftLog->zeroTermOnErrCompacted(term_, err);
	BOOST_REQUIRE_EQUAL(lastLogTerm, sm->m_Term);

	// Ensure peer a accepts read only request after it commits a entry at its term.
	nt->send(make_message(1, 1, MsgReadIndex, 0, 0, false, { makeEntry(0, 0, wctx) }));
	BOOST_REQUIRE_EQUAL(sm->m_readStates.size(), 1);
	auto &rs = sm->m_readStates[0];
	BOOST_REQUIRE_EQUAL(rs.Index, windex);
	BOOST_REQUIRE_EQUAL(rs.RequestCtx, wctx);
}

TestRaftPtr entsWithConfig(void (*configFunc)(Config&), const vector<uint64_t> &terms) {
	auto storage = std::make_shared<MemoryStorage>();
	for (size_t i = 0; i < terms.size(); i++) {
		storage->append({ makeEntry(uint64_t(i + 1), terms[i]) });
	}
	auto cfg = newTestConfig(1, {}, 5, 1, storage);
	if (configFunc) {
		configFunc(cfg);
	}
	auto sm = std::make_shared<testRaft>(cfg);
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
	auto sm = std::make_shared<testRaft>(cfg);
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
		BOOST_REQUIRE_EQUAL(sm->m_state, tt.state);
		BOOST_REQUIRE_EQUAL(sm->m_Term, tt.expTerm);
	}
}


void preVoteConfig(Config &c) {
	c.PreVote = true;
}

vector<uint64_t> idsBySize(size_t size) {
	vector<uint64_t> ids(size);
	for (size_t i = 0; i < size; i++) {
		ids[i] = 1 + uint64_t(i);
	}
	return ids;
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
			npeers[id] = std::move(std::make_unique<testRaft>(cfg));
		} else if (pTestRaft = dynamic_cast<testRaft*>(p.get()), pTestRaft) {
			map<uint64_t, bool> learners;
			for (auto it = pTestRaft->m_learnerPrs.begin(); it != pTestRaft->m_learnerPrs.end(); ++it) {
				learners[it->first] = true;
			}
			pTestRaft->m_id = id;
			for (size_t i = 0; i < size; i++) {
				if (learners.find(peerAddrs[i]) != learners.end()) {
					auto pp = std::make_unique<Progress>();
					pp->IsLearner = true;
					pTestRaft->m_learnerPrs[peerAddrs[i]] = std::move(pp);
				} else {
					pTestRaft->m_prs[peerAddrs[i]] = std::make_unique<Progress>();
				}
			}
			pTestRaft->reset(pTestRaft->m_Term);
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
	r->m_randomizedElectionTimeout = v;
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
			BOOST_REQUIRE_EQUAL(sm->m_id == campaignerID && sm->m_state != StateLeader, false);
			BOOST_REQUIRE_EQUAL(sm->m_id != campaignerID && sm->m_state != StateFollower, false);
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
	BOOST_REQUIRE_EQUAL(sm1->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(sm1->m_Term, 2);

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n->send(make_message(1, 1, MsgHup));
	BOOST_REQUIRE_EQUAL(sm1->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(sm1->m_Term, 3);

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	for (auto it = n->peers.begin(); it != n->peers.end(); ++it) {
		testRaft* sm = dynamic_cast<testRaft*>(it->second.get());
		auto entries = sm->m_raftLog->allEntries();
		BOOST_REQUIRE_EQUAL(entries.size(), 2);
		BOOST_REQUIRE_EQUAL(entries[0].term(), 1);
		BOOST_REQUIRE_EQUAL(entries[1].term(), 3);
	}
}

void testVoteFromAnyState(MessageType vt) {
	for (int st = 0; st < numStates; st++) {
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_unique<MemoryStorage>());
		r->m_Term = 1;

		switch (st) {
		case StateFollower:
			r->becomeFollower(r->m_Term, 3);
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
		auto origTerm = r->m_Term;
		auto newTerm = r->m_Term + 1;
		auto msg = make_message(2, 1, vt, 42, newTerm);
		msg->set_logterm(newTerm);
		auto err = r->Step(*msg);
		BOOST_REQUIRE_EQUAL(err, OK);
		BOOST_REQUIRE_EQUAL(r->m_msgs.size(), 1);
		BOOST_REQUIRE_EQUAL(r->m_msgs[0]->type(), voteRespMsgType(vt));
		BOOST_REQUIRE_EQUAL(r->m_msgs[0]->reject(), false);

		// If this was a real vote, we reset our state and term.
		if (vt == MsgVote) {
			BOOST_REQUIRE_EQUAL(r->m_state, StateFollower);
			BOOST_REQUIRE_EQUAL(r->m_Term, newTerm);
			BOOST_REQUIRE_EQUAL(r->m_Vote, 2);
		} else {
			// In a prevote, nothing changes.
			BOOST_REQUIRE_EQUAL(r->m_state, st);
			BOOST_REQUIRE_EQUAL(r->m_Term, origTerm);
			// if st == StateFollower or StatePreCandidate, r hasn't voted yet.
			// In StateCandidate or StateLeader, it's voted for itself.
			BOOST_REQUIRE_EQUAL(r->m_Vote != None && r->m_Vote != 1, false);
		}
	}
}

// nextEnts returns the appliable entries and updates the applied index
vector<Entry> nextEnts(testRaft *r, Storage *s) {
	// Transfer all unstable entries to "stable" storage.
	vector<Entry> ents;
	dynamic_cast<MemoryStorage*>(s)->append(r->m_raftLog->unstableEntries());
	r->m_raftLog->stableTo(r->m_raftLog->lastIndex(), r->m_raftLog->lastTerm());

	ents = r->m_raftLog->nextEnts();
	r->m_raftLog->appliedTo(r->m_raftLog->m_committed);
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
		sm->m_state = tt.state;
		switch (tt.state) {
		case StateFollower:
			sm->m_step = stepFollower;
			break;
		case StateCandidate:
		case StatePreCandidate:
			sm->m_step = stepCandidate;
			break;
		case StateLeader:
			sm->m_step = stepLeader;
			break;
		}
		sm->m_Vote = tt.voteFor;
		auto storage = std::make_shared<MemoryStorage>();
		storage->append({ {}, makeEntry(1, 2), makeEntry(2, 2) });
		sm->m_raftLog = std::make_unique<raft_log>(storage, &DefaultLogger::instance());
		sm->m_raftLog->m_unstable.m_offset = 3;

		// raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
		// test we're only testing MsgVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
		auto term = std::max(sm->m_raftLog->lastTerm(), tt.logTerm);
		sm->m_Term = term;
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
	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(b->m_state, StateFollower);
	BOOST_REQUIRE_EQUAL(c->m_state, StateFollower);

	// isolate 3 and increase term in rest
	nt->isolate(3);

	nt->send(make_message(2, 2, MsgHup));
	nt->send(make_message(1, 1, MsgHup));

	BOOST_REQUIRE_EQUAL(a->m_state, StateLeader);
	BOOST_REQUIRE_EQUAL(b->m_state, StateFollower);

	// trigger campaign in isolated c
	c->resetRandomizedElectionTimeout();
	for (size_t i = 0; i < c->m_randomizedElectionTimeout; i++) {
		c->m_tick();
	}

	BOOST_REQUIRE_EQUAL(c->m_state, StateCandidate);

	nt->recover();

	// leader sends to isolated candidate
	// and expects candidate to revert to follower

	nt->send(make_message(1, 3, mt, 0, a->m_Term));

	BOOST_REQUIRE_EQUAL(c->m_state, StateFollower);

	// follower c term is reset with leader's
	BOOST_REQUIRE_EQUAL(a->m_Term, c->m_Term);
}
