#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
using namespace raft;
using namespace raftpb;
#define DISABLE_PAPER_TEST

class DiscardLogger : public Logger {
public:
	virtual void log(const LogContext &ctx, const string &msg) {
	}
};
DiscardLogger discardLogger;

void equal_messages(const vector<MessagePtr> &a, const vector<MessagePtr> &b);
MessagePtr acceptAndReply(Message &m);
void testUpdateTermFromMessage(StateType state);

#ifndef DISABLE_PAPER_TEST
BOOST_AUTO_TEST_CASE(TestFollowerUpdateTermFromMessage) {
	testUpdateTermFromMessage(StateFollower);
}

BOOST_AUTO_TEST_CASE(TestCandidateUpdateTermFromMessage) {
	testUpdateTermFromMessage(StateCandidate);
}

BOOST_AUTO_TEST_CASE(TestLeaderUpdateTermFromMessage) {
	testUpdateTermFromMessage(StateLeader);
}


// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
BOOST_AUTO_TEST_CASE(TestRejectStaleTermMessage) {
	bool called = false;
	auto fakeStep = [&](Raft *r, Message &m) {
		called = true;
		return OK;
	};
	auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	r->step = fakeStep;
	HardState hs;
	hs.set_term(2);
	r->loadState(hs);

	r->Step(*make_message(0, 0, MsgApp, 0, r->Term - 1));

	BOOST_REQUIRE_EQUAL(called, false);
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
BOOST_AUTO_TEST_CASE(TestStartAsFollower) {
	auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	BOOST_REQUIRE_EQUAL(r->state, StateFollower);
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
BOOST_AUTO_TEST_CASE(TestLeaderBcastBeat) {
	// heartbeat interval
	int hi = 1;
	auto r = newTestRaft(1, { 1, 2, 3 }, 10, hi, std::make_shared<MemoryStorage>());
	r->becomeCandidate();
	r->becomeLeader();
	for (size_t i = 0; i < 10; i++) {
		mustAppendEntry(r.get(), { makeEntry(i + 1, 0) });
	}

	for (size_t i = 0; i < hi; i++) {
		r->tick();
	}

	auto msgs = r->readMessages();
	std::sort(msgs.begin(), msgs.end(), [](const MessagePtr &l, const MessagePtr &r) {
		return l->to() < r->to();
	});
	vector<MessagePtr> wmsgs;
	wmsgs.emplace_back(make_message(1, 2, MsgHeartbeat, 0, 1));
	wmsgs.emplace_back(make_message(1, 3, MsgHeartbeat, 0, 1));
	equal_messages(msgs, wmsgs);
}

void testNonleaderStartElection(StateType state);
BOOST_AUTO_TEST_CASE(TestFollowerStartElection) {
	testNonleaderStartElection(StateFollower);
}

BOOST_AUTO_TEST_CASE(TestCandidateStartNewElection) {
	testNonleaderStartElection(StateCandidate);
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
BOOST_AUTO_TEST_CASE(TestLeaderElectionInOneRoundRPC) {
	struct {
		int size;
		map<uint64_t, bool> votes;
		StateType state;
	}tests[] = {
		// win the election when receiving votes from a majority of the servers
		{1, {}, StateLeader},
		{3, {{2, true}, {3, true}}, StateLeader},
		{3, {{2, true}}, StateLeader},
		{5, {{2, true}, {3, true}, {4, true}, {5, true}}, StateLeader},
		{5, {{2, true}, {3, true}, {4, true}}, StateLeader},
		{5, {{2, true}, {3, true}}, StateLeader},

		// return to follower state if it receives vote denial from a majority
		{3, {{2, false}, {3, false}}, StateFollower},
		{5, {{2, false}, {3, false}, {4, false}, {5, false}}, StateFollower},
		{5, {{2, true}, {3, false}, {4, false}, {5, false}}, StateFollower},

		// stay in candidate if it does not obtain the majority
		{3, {}, StateCandidate},
		{5, {{2, true}}, StateCandidate},
		{5, {{2, false}, {3, false}}, StateCandidate},
		{5, {}, StateCandidate},
	};
	for (auto &tt : tests) {
		auto r = newTestRaft(1, idsBySize(tt.size), 10, 1, std::make_shared<MemoryStorage>());
		
		r->Step(*make_message(1, 1, MsgHup));
		for (auto it = tt.votes.begin(); it != tt.votes.end(); ++it) {
			auto id = it->first;
			auto vote = it->second;
			r->Step(*make_message(id, 1, MsgVoteResp, 0, r->Term, !vote));
		}

		BOOST_REQUIRE_EQUAL(r->state, tt.state);
		BOOST_REQUIRE_EQUAL(r->Term, 1);
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
BOOST_AUTO_TEST_CASE(TestFollowerVote) {
	struct {
		uint64_t vote;
		uint64_t nvote;
		bool wreject;
	} tests[] = {
		{None, 1, false},
		{None, 2, false},
		{1, 1, false},
		{2, 2, false},
		{1, 2, true},
		{2, 1, true},
	};
	for (auto tt : tests) {
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
		HardState hs;
		hs.set_term(1);
		hs.set_vote(tt.vote);
		r->loadState(hs);

		r->Step(*make_message(tt.nvote, 1, MsgVote, 0, 1));

		auto msgs = r->readMessages();
		vector<MessagePtr> wmsgs;
		wmsgs.emplace_back(make_message(1, tt.nvote, MsgVoteResp, 0, 1, tt.wreject));
		equal_messages(msgs, wmsgs);
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
BOOST_AUTO_TEST_CASE(TestCandidateFallback) {
	vector<MessagePtr> tests;
	tests.emplace_back(make_message(2, 1, MsgApp, 0, 1));
	tests.emplace_back(make_message(2, 1, MsgApp, 0, 2));
	for (auto &tt : tests) {
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
		r->Step(*make_message(1, 1, MsgHup));
		BOOST_REQUIRE_EQUAL(r->state, StateCandidate);

		r->Step(*tt);

		BOOST_REQUIRE_EQUAL(r->state, StateFollower);
		BOOST_REQUIRE_EQUAL(r->Term, tt->term());
	}
}

void testNonleaderElectionTimeoutRandomized(StateType state);
BOOST_AUTO_TEST_CASE(TestFollowerElectionTimeoutRandomized) {
	testNonleaderElectionTimeoutRandomized(StateFollower);
}

BOOST_AUTO_TEST_CASE(TestCandidateElectionTimeoutRandomized) {
	testNonleaderElectionTimeoutRandomized(StateCandidate);
}

void testNonleadersElectionTimeoutNonconflict(StateType state);
BOOST_AUTO_TEST_CASE(TestFollowersElectionTimeoutNonconflict) {
	testNonleadersElectionTimeoutNonconflict(StateFollower);
}

BOOST_AUTO_TEST_CASE(TestCandidatesElectionTimeoutNonconflict) {
	testNonleadersElectionTimeoutNonconflict(StateCandidate);
}

void commitNoopEntry(testRaft *r, MemoryStorage *s);
// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestLeaderStartReplication) {
	auto s = std::make_shared<MemoryStorage>();
	auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, s);
	r->becomeCandidate();
	r->becomeLeader();
	commitNoopEntry(r.get(), s.get());
	auto li = r->raftLog->lastIndex();

	vector<Entry> ents = { makeEntry(0, 0, "some data") };
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, std::move(ents)));

	BOOST_REQUIRE_EQUAL(r->raftLog->lastIndex(), li + 1);
	BOOST_REQUIRE_EQUAL(r->raftLog->committed, li);

	auto msgs = r->readMessages();
	std::sort(msgs.begin(), msgs.end(), [](const MessagePtr &l, const MessagePtr &r) {
		return l->to() < r->to();
	});
	vector<Entry> wents = { makeEntry(li + 1, 1, "some data") };
	vector<MessagePtr> wmsgs;
	wmsgs.emplace_back(make_message(1, 2, MsgApp, li, 1, false, vector<Entry>(wents), 1, li));
	wmsgs.emplace_back(make_message(1, 3, MsgApp, li, 1, false, vector<Entry>(wents), 1, li));
	equal_messages(msgs, wmsgs);
	equal_entrys(r->raftLog->unstableEntries(), wents);
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestLeaderCommitEntry) {
	auto s = std::make_shared<MemoryStorage>();
	auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, s);
	r->becomeCandidate();
	r->becomeLeader();
	commitNoopEntry(r.get(), s.get());
	auto li = r->raftLog->lastIndex();
	r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));

	for (auto &m : r->readMessages()) {
		r->Step(*acceptAndReply(*m));
	}

	BOOST_REQUIRE_EQUAL(r->raftLog->committed, li + 1);
	vector<Entry> wents = { makeEntry(li + 1, 1, "some data") };
	equal_entrys(r->raftLog->nextEnts(), wents);
	auto msgs = r->readMessages();
	std::sort(msgs.begin(), msgs.end(), [](const MessagePtr &l, const MessagePtr &r) {
		return l->to() < r->to();
	});
	for (size_t i = 0; i < msgs.size(); i++) {
		auto &m = msgs[i];
		BOOST_REQUIRE_EQUAL(i + 2, m->to());
		BOOST_REQUIRE_EQUAL(MsgApp, m->type());
		BOOST_REQUIRE_EQUAL(li + 1, m->commit());
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestLeaderAcknowledgeCommit) {
	struct {
		int size;
		map<uint64_t, bool> acceptors;
		bool wack;
	} tests[] = {
		{1, {}, true},
		{3, {}, false},
		{3,{{2, true}}, true},
		{3, {{2, true}, {3, true}}, true},
		{5, {}, false},
		{5, {{2, true}}, false},
		{5, {{2, true}, {3, true}}, true},
		{5, {{2, true}, {3, true}, {4, true}}, true},
		{5, {{2, true}, {3, true}, {4, true}, {5, true}}, true},
	};
	for (auto &tt : tests) {
		auto s = std::make_shared<MemoryStorage>();
		auto r = newTestRaft(1, idsBySize(tt.size), 10, 1, s);
		r->becomeCandidate();
		r->becomeLeader();
		commitNoopEntry(r.get(), s.get());
		auto li = r->raftLog->lastIndex();
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));

		for (auto &m : r->readMessages()) {
			if (tt.acceptors[m->to()]) {
				r->Step(*acceptAndReply(*m));
			}
		}

		BOOST_REQUIRE_EQUAL(r->raftLog->committed > li, tt.wack);
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestLeaderCommitPrecedingEntries) {
	vector<vector<Entry>> tests = {
		{},
		{makeEntry(1, 2)},
		{makeEntry(1, 1), makeEntry(2, 2)},
		{makeEntry(1, 1)},
	};
	for (auto &tt : tests) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(tt);
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, storage);
		HardState hs;
		hs.set_term(2);
		r->loadState(hs);
		r->becomeCandidate();
		r->becomeLeader();
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0, 0, "some data") }));

		for (auto &m : r->readMessages()) {
			r->Step(*acceptAndReply(*m));
		}

		uint64_t li = tt.size();
		vector<Entry> wents(tt);
		wents.insert(wents.end(), { makeEntry(li + 1, 3), makeEntry(li + 2, 3, "some data") });
		equal_entrys(wents, r->raftLog->nextEnts());
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestFollowerCommitEntry) {
	struct {
		vector<Entry> ents;
		uint64_t commit;
	} tests[] = {
		{{makeEntry(1, 1, "some data")}, 1},
		{{makeEntry(1, 1, "some data"), makeEntry(2, 1, "some data2")}, 2},
		{{makeEntry(1, 1, "some data2"), makeEntry(2, 1, "some data")}, 2},
		{{makeEntry(1, 1, "some data"), makeEntry(2, 1, "some data2")}, 1},
	};
	for (auto &tt : tests) {
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
		r->becomeFollower(1, 2);
		
		r->Step(*make_message(2, 1, MsgApp, 0, 1, false, vector<Entry>(tt.ents), 0, tt.commit));

		BOOST_REQUIRE_EQUAL(r->raftLog->committed, tt.commit);
		vector<Entry> wents(tt.ents.begin(), tt.ents.begin() + tt.commit);
		equal_entrys(r->raftLog->nextEnts(), wents);
	}
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestFollowerCheckMsgApp) {
	vector<Entry> ents = { makeEntry(1, 1), makeEntry(2, 2)  };
	struct {
		uint64_t term;
		uint64_t index;
		uint64_t windex;
		bool wreject;
		uint64_t wrejectHint;
	}tests[] = {
		// match with committed entries
		{0, 0, 1, false, 0},
		{ents[0].term(), ents[0].index(), 1, false, 0},
		// match with uncommitted entries
		{ents[1].term(), ents[1].index(), 2, false, 0},

		// unmatch with existing entry
		{ents[0].term(), ents[1].index(), ents[1].index(), true, 2},
		// unexisting entry
		{ents[1].term() + 1, ents[1].index() + 1, ents[1].index() + 1, true, 2},
	};
	for (auto &tt : tests) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(ents);
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, storage);
		HardState hs;
		hs.set_commit(1);
		r->loadState(hs);
		r->becomeFollower(2, 2);
		
		r->Step(*make_message(2, 1, MsgApp, tt.index, 2, false, {}, tt.term, 0));

		auto msgs = r->readMessages();
		vector<MessagePtr> wmsgs;
		wmsgs.emplace_back(make_message(1, 2, MsgAppResp, tt.windex, 2, tt.wreject, {}, 0, 0, tt.wrejectHint));
		equal_messages(msgs, wmsgs);
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
BOOST_AUTO_TEST_CASE(TestFollowerAppendEntries) {
	struct {
		uint64_t index, term;
		vector<Entry> ents;
		vector<Entry> wents;
		vector<Entry> wunstable;
	} tests[] = {
		{
			2, 2,
			{makeEntry(3, 3)},
			{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)},
			{makeEntry(3, 3)},
		},
		{
			1, 1,
			{makeEntry(2, 3), makeEntry(3, 4)},
			{makeEntry(1, 1), makeEntry(2, 3), makeEntry(3, 4)},
			{makeEntry(2, 3), makeEntry(3, 4)},
		},
		{
			0, 0,
			{makeEntry(1, 1)},
			{makeEntry(1, 1), makeEntry(2, 2)},
			{},
		},
		{
			0, 0,
			{makeEntry(1, 3)},
			{makeEntry(1, 3)},
			{makeEntry(1, 3)},
		},
	};
	for (auto &tt : tests) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(EntryVec{makeEntry(1, 1), makeEntry(2, 2)});
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, storage);
		r->becomeFollower(2, 2);

		r->Step(*make_message(2, 1, MsgApp, tt.index, 2, false, vector<Entry>(tt.ents), tt.term));

		equal_entrys(r->raftLog->allEntries(), tt.wents);
		equal_entrys(r->raftLog->unstableEntries(), tt.wunstable);
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
BOOST_AUTO_TEST_CASE(TestLeaderSyncFollowerLog) {
	vector<Entry> ents = {
		{},
		makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
		makeEntry(4, 4), makeEntry(5, 4),
		makeEntry(6, 5), makeEntry(7, 5),
		makeEntry(8, 6), makeEntry(9, 6), makeEntry(10, 6),
	};
	uint64_t term = 8;
	vector<vector<Entry>> tests = {
		{
			{},
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
			makeEntry(4, 4), makeEntry(5, 4),
			makeEntry(6, 5), makeEntry(7, 5),
			makeEntry(8, 6), makeEntry(9, 6),
		},
		{
			{},
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
			makeEntry(4, 4),
		},
		{
			{},
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
			makeEntry(4, 4), makeEntry(5, 4),
			makeEntry(6, 5), makeEntry(7, 5),
			makeEntry(8, 6), makeEntry(9, 6), makeEntry(10, 6), makeEntry(11, 6),
		},
		{
			{},
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
			makeEntry(4, 4), makeEntry(5, 4),
			makeEntry(6, 5), makeEntry(7, 5),
			makeEntry(8, 6), makeEntry(9, 6), makeEntry(10, 6),
			makeEntry(11, 7), makeEntry(12, 7),
		},
		{
			{},
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
			makeEntry(4, 4), makeEntry(5, 4), makeEntry(6, 4), makeEntry(7, 4),
		},
		{
			{},
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1),
			makeEntry(4, 2), makeEntry(5, 2), makeEntry(6, 2),
			makeEntry(7, 3), makeEntry(8, 3), makeEntry(9, 3), makeEntry(10, 3), makeEntry(11, 3),
		},
	};
	for (auto &tt : tests) {
		auto leadStorage = std::make_shared<MemoryStorage>();
		leadStorage->Append(ents);
		auto lead = newTestRaft(1, { 1, 2, 3 }, 10, 1, leadStorage);
		HardState hs;
		hs.set_commit(lead->raftLog->lastIndex());
		hs.set_term(term);
		lead->loadState(hs);
		auto followerStorage = std::make_shared<MemoryStorage>();
		followerStorage->Append(tt);
		auto follower = newTestRaft(2, { 1, 2, 3 }, 10, 1, followerStorage);
		HardState hs1;
		hs1.set_term(term - 1);
		follower->loadState(hs1);
		// It is necessary to have a three-node cluster.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
		auto n = newNetwork({ lead, follower, nopStepper });
		n->send(make_message(1, 1, MsgHup));
		// The election occurs in the term after the one we loaded with
		// lead.loadState above.
		n->send(make_message(3, 1, MsgVoteResp, 0, term + 1));

		n->send(make_message(1, 1, MsgProp, 0, 0, false, { {} }));

		auto g = diffu(ltoa(lead->raftLog.get()), ltoa(follower->raftLog.get()));
		BOOST_REQUIRE_EQUAL(g, "");
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
BOOST_AUTO_TEST_CASE(TestVoteRequest) {
	struct {
		vector<Entry> ents;
		uint64_t wterm;
	} tests[] = {
		{{makeEntry(1, 1)}, 2},
		{{makeEntry(1, 1), makeEntry(2, 2)}, 3},
	};
	for (auto &tt : tests) {
		auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
		r->Step(*make_message(2, 1, MsgApp, 0, tt.wterm - 1, false, vector<Entry>(tt.ents), 0));
		r->readMessages();

		for (int i = 1; i < r->electionTimeout*2; i++) {
			r->tickElection();
		}

		auto msgs = r->readMessages();
		BOOST_REQUIRE_EQUAL(msgs.size(), 2);
		for (size_t i = 0; i < msgs.size(); i++) {
			auto &m = msgs[i];
			BOOST_REQUIRE_EQUAL(m->type(), MsgVote);
			BOOST_REQUIRE_EQUAL(m->to(), i + 2);
			BOOST_REQUIRE_EQUAL(m->term(), tt.wterm);
			uint64_t windex = tt.ents[tt.ents.size() - 1].index();
			uint64_t wlogterm = tt.ents[tt.ents.size() - 1].term();
			BOOST_REQUIRE_EQUAL(m->index(), windex);
			BOOST_REQUIRE_EQUAL(m->logterm(), wlogterm);
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
BOOST_AUTO_TEST_CASE(TestVoter) {
	struct {
		vector<Entry> ents;
		uint64_t logterm;
		uint64_t index;

		bool wreject;
	}tests[] = {
		// same logterm
		{{makeEntry(1, 1)}, 1, 1, false},
		{{makeEntry(1, 1)}, 1, 2, false},
		{{makeEntry(1, 1), makeEntry(2, 1)}, 1, 1, true},
		// candidate higher logterm
		{{makeEntry(1, 1)}, 2, 1, false},
		{{makeEntry(1, 1)}, 2, 2, false},
		{{makeEntry(1, 1), makeEntry(2, 1)}, 2, 1, false},
		// voter higher logterm
		{{makeEntry(1, 2)}, 1, 1, true},
		{{makeEntry(1, 2)}, 1, 2, true},
		{{makeEntry(1, 2), makeEntry(2, 1)}, 1, 1, true},
	};
	for (auto &tt : tests) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(tt.ents);
		auto r = newTestRaft(1, { 1, 2 }, 10, 1, storage);
		
		r->Step(*make_message(2, 1, MsgVote, tt.index, 3, false, {}, tt.logterm));

		auto msgs = r->readMessages();
		BOOST_REQUIRE_EQUAL(msgs.size(), 1);
		auto &m = msgs[0];
		BOOST_REQUIRE_EQUAL(m->type(), MsgVoteResp);
		BOOST_REQUIRE_EQUAL(m->reject(), tt.wreject);
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
BOOST_AUTO_TEST_CASE(TestLeaderOnlyCommitsLogFromCurrentTerm) {
	vector<Entry> ents = { makeEntry(1, 1), makeEntry(2, 2) };
	struct {
		uint64_t index;
		uint64_t wcommit;
	}tests[] = {
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	};
	for (auto &tt : tests) {
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(ents);
		auto r = newTestRaft(1, { 1, 2 }, 10, 1, storage);
		HardState hs;
		hs.set_term(2);
		r->loadState(hs);
		// become leader at term 3
		r->becomeCandidate();
		r->becomeLeader();
		r->readMessages();
		// propose a entry to current term
		r->Step(*make_message(1, 1, MsgProp, 0, 0, false, { {} }));

		r->Step(*make_message(2, 1, MsgAppResp, tt.index, r->Term));
		BOOST_REQUIRE_EQUAL(r->raftLog->committed, tt.wcommit);
	}
}
#endif

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
void testUpdateTermFromMessage(StateType state) {
	auto r = newTestRaft(1, { 1, 2, 3 }, 10, 1, std::make_shared<MemoryStorage>());
	switch (state) {
	case StateFollower:
		r->becomeFollower(1, 2);
		break;
	case StateCandidate:
		r->becomeCandidate();
			break;
	case StateLeader:
		r->becomeCandidate();
		r->becomeLeader();
		break;
	}

	r->Step(*make_message(0, 0, MsgApp, 0, 2));

	BOOST_REQUIRE_EQUAL(r->Term, 2);
	BOOST_REQUIRE_EQUAL(r->state, StateFollower);
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
void testNonleaderStartElection(StateType state) {
	// election timeout
	int et = 10;
	auto r = newTestRaft(1, { 1, 2, 3 }, et, 1, std::make_shared<MemoryStorage>());
	switch (state) {
	case StateFollower:
		r->becomeFollower(1, 2);
		break;
	case StateCandidate:
		r->becomeCandidate();
		break;
	}

	for (size_t i = 1; i < 2*et; i++) {
		r->tick();
	}

	BOOST_REQUIRE_EQUAL(r->Term, 2);
	BOOST_REQUIRE_EQUAL(r->state, StateCandidate);
	BOOST_REQUIRE_EQUAL(r->votes[r->id], true);
	auto msgs = r->readMessages();
	std::sort(msgs.begin(), msgs.end(), [](const MessagePtr &l, const MessagePtr &r) {
		return l->to() < r->to();
	});
	vector<MessagePtr> wmsgs;
	wmsgs.emplace_back(make_message(1, 2, MsgVote, 0, 2));
	wmsgs.emplace_back(make_message(1, 3, MsgVote, 0, 2));
	equal_messages(msgs, wmsgs);
}

void equal_messages(const vector<MessagePtr> &a, const vector<MessagePtr> &b) {
	BOOST_REQUIRE_EQUAL(a.size(), b.size());
	for (size_t i = 0; i < a.size(); i++) {
		const Message &e1 = *a[i];
		const Message &e2 = *b[i];
		BOOST_REQUIRE_EQUAL(e1.index(), e2.index());
		BOOST_REQUIRE_EQUAL(e1.term(), e2.term());
		BOOST_REQUIRE_EQUAL(e1.type(), e2.type());
	}
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
void testNonleaderElectionTimeoutRandomized(StateType state) {
	int et = 10;
	auto r = newTestRaft(1, { 1, 2, 3 }, et, 1, std::make_shared<MemoryStorage>(), &discardLogger);
	map<int, bool> timeouts;
	for (size_t round = 0; round < 50*et; round++) {
		switch (state) {
		case StateFollower:
			r->becomeFollower(r->Term + 1, 2);
			break;
		case StateCandidate:
			r->becomeCandidate();
			break;
		}

		int time = 0;
		for (auto msgs = r->readMessages(); msgs.empty(); msgs = r->readMessages()) {
			r->tick();
			time++;
		}
		timeouts[time] = true;
	}

	for (int d = et + 1; d < 2*et; d++) {
		BOOST_REQUIRE_EQUAL(timeouts[d], true);
	}
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
void testNonleadersElectionTimeoutNonconflict(StateType state) {
	int et = 10;
	int size = 5;
	vector<TestRaftPtr> rs(5);
	auto ids = idsBySize(size);
	for (int k = 0; k < size; k++) {
		rs[k] = newTestRaft(ids[k], vector<uint64_t>(ids), et, 1, std::make_shared<MemoryStorage>(), &discardLogger);
	}
	int conflicts = 0;
	for (size_t round = 0; round < 1000; round++) {
		for (auto &r : rs) {
			switch (state) {
			case StateFollower:
				r->becomeFollower(r->Term + 1, None);
				break;
			case StateCandidate:
				r->becomeCandidate();
				break;
			}
		}

		int timeoutNum = 0;
		for (; timeoutNum == 0;) {
			for (auto &r : rs) {
				r->tick();
				if (r->readMessages().size() > 0) {
					timeoutNum++;
				}
			}
		}
		// several rafts time out at the same tick
		if (timeoutNum > 1) {
			conflicts++;
		}
	}

	BOOST_REQUIRE_LE(double(conflicts) / 1000, 0.3);
}

void commitNoopEntry(testRaft *r, MemoryStorage *s) {
	BOOST_REQUIRE_EQUAL(r->state, StateLeader);
	r->bcastAppend();
	// simulate the response of MsgApp
	auto msgs = r->readMessages();
	for (auto &m : msgs) {
		BOOST_REQUIRE_EQUAL(m->type(), MsgApp);
		BOOST_REQUIRE_EQUAL(m->entries_size(), 1);
		BOOST_REQUIRE_EQUAL(m->entries(0).data().size(), 0);
		r->Step(*acceptAndReply(*m));
	}
	// ignore further messages to refresh followers' commit index
	r->readMessages();
	s->Append(r->raftLog->unstableEntries());
	r->raftLog->appliedTo(r->raftLog->committed);
	r->raftLog->stableTo(r->raftLog->lastIndex(), r->raftLog->lastTerm());
}

MessagePtr acceptAndReply(Message &m) {
	BOOST_REQUIRE_EQUAL(m.type(), MsgApp);
	return make_message(m.to(), m.from(), MsgAppResp, m.index() + m.entries_size());
}
