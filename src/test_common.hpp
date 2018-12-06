#pragma once

#include <log_unstable.hpp>
#include <raft_log.hpp>
#include <raft/Raft.hpp>
#include <progress.hpp>

using namespace raft;
using namespace std;

typedef std::unique_ptr<Snapshot> SnapshotPtr;

Entry makeEntry(uint64_t index, uint64_t term, string &&data = string(), EntryType type = EntryNormal);
SnapshotPtr makeSnapshot(uint64_t index, uint64_t term);
void equal_entrys(const vector<Entry> &left, const vector<Entry> &right);
unstable make_unstable(unique_ptr<Snapshot> &&snapshot, vector<Entry> &&entries, uint64_t offset, Logger &logger);
string ltoa(raft_log *l);
string diffu(const string &a, const string &b);
uint64_t mustTerm(uint64_t term, ErrorCode err);

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
	testRaft(Config &c) : Raft(c) {}
	ErrorCode Step(Message &m) { return Raft::Step(m); }
	vector<MessagePtr> readMessages() { return std::move(Raft::m_msgs); }
};

typedef std::shared_ptr<stateMachine> stateMachinePtr;
typedef std::unique_ptr<MemoryStorage> MemoryStoragePtr;
typedef std::shared_ptr<testRaft> TestRaftPtr;

vector<Entry> nextEnts(testRaft *r, Storage *s);
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

TestRaftPtr newTestRaft(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage);
MessagePtr make_message(uint64_t from, uint64_t to, MessageType type, uint64_t index = 0, uint64_t term = 0, bool reject = false, vector<Entry> &&ents = vector<Entry>());
