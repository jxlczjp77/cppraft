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
			auto cfg = newTestConfig(id, vector<uint64_t>{ peerAddrs }, 10, 1, std::move(nstorage[id]));
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
