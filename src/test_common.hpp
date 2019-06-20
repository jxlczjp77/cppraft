#pragma once
#include <raft/raft.hpp>

using namespace raft;
using namespace std;

typedef std::unique_ptr<Snapshot> SnapshotPtr;

Entry makeEntry(uint64_t index, uint64_t term, string &&data = string(), EntryType type = EntryNormal);
SnapshotPtr makeSnapshot(uint64_t index, uint64_t term);
template<class EntryVec1, class EntryVec2>
void equal_entrys(const EntryVec1 &left, const EntryVec2 &right) {
	BOOST_REQUIRE_EQUAL(left.size(), right.size());
	for (size_t i = 0; i < left.size(); i++) {
		auto &e1 = left[i];
		auto &e2 = right[i];
		BOOST_REQUIRE_EQUAL(e1.index(), e2.index());
		BOOST_REQUIRE_EQUAL(e1.term(), e2.term());
	}
}
template<class Vec1, class Vec2>
void euqal_vec(const Vec1 &left, const Vec2 &right, const std::function<void(const typename Vec1::value_type &a, const typename Vec2::value_type &b)> &c) {
	BOOST_REQUIRE_EQUAL(left.size(), right.size());
	for (size_t i = 0; i < left.size(); i++) {
		auto &e1 = left[i];
		auto &e2 = right[i];
		c(e1, e2);
	}
}
unstable make_unstable(unique_ptr<Snapshot> &&snapshot, vector<Entry> &&entries, uint64_t offset, Logger &logger);
string ltoa(raft_log *l);
string diffu(const string &a, const string &b);
uint64_t mustTerm(const Result<uint64_t> &term);
vector<uint64_t> idsBySize(size_t size);

struct stateMachine {
	virtual ErrorCode Step(Message &m) = 0;
	virtual vector<MessagePtr> readMessages() = 0;
};

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

struct testRaft : public stateMachine, public Raft {
	ErrorCode Step(Message &m) { return Raft::Step(m); }
	vector<MessagePtr> readMessages() { return std::move(Raft::msgs); }
};

typedef std::shared_ptr<stateMachine> stateMachinePtr;
typedef std::unique_ptr<MemoryStorage> MemoryStoragePtr;
typedef std::shared_ptr<testRaft> TestRaftPtr;

EntryRange nextEnts(testRaft *r, Storage *s);
void mustAppendEntry(testRaft *r, vector<Entry> &&ents);

struct network {
	map<uint64_t, stateMachinePtr> peers;
	map<uint64_t, StoragePtr> storage;
	map<connem, double> dropm;
	map<MessageType, bool> ignorem;

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	bool(*msgHook)(const Message &m);

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
extern stateMachinePtr nopStepper;
TestRaftPtr newTestRaft(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage, Logger *Logger = nullptr);
networkptr newNetwork(const vector<stateMachinePtr> &peers);
MessagePtr make_message(
	uint64_t from,
	uint64_t to,
	MessageType type,
	uint64_t index = 0,
	uint64_t term = 0,
	bool reject = false,
	vector<Entry> &&ents = vector<Entry>(),
	uint64_t logterm = 0,
	uint64_t commit = 0,
	uint64_t wrejectHint = 0
);

class StackLogLevel {
	Logger &m_l;
	LogLevel m_old_level;
public:
	StackLogLevel(Logger &l, LogLevel v) : m_l(l) {
		m_old_level = m_l.setLogLevel(v);
	}
	~StackLogLevel() { m_l.setLogLevel(m_old_level); }
};
