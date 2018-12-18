#include "test_common.hpp"
#include <boost/test/unit_test.hpp>
#include <boost/format.hpp>

struct doexit {
	~doexit() {
		google::protobuf::ShutdownProtobufLibrary();
	}
} t;

Entry makeEntry(uint64_t index, uint64_t term, string &&data, EntryType type) {
	Entry tmp;
	tmp.set_index(index);
	tmp.set_term(term);
	tmp.set_data(std::move(data));
	tmp.set_type(type);
	return tmp;
}

SnapshotPtr makeSnapshot(uint64_t index, uint64_t term) {
	SnapshotPtr sh = make_unique<Snapshot>();
	sh->mutable_metadata()->set_index(index);
	sh->mutable_metadata()->set_term(term);
	return sh;
}

unstable make_unstable(unique_ptr<Snapshot> &&snapshot, vector<Entry> &&entries, uint64_t offset, Logger &logger) {
	unstable u;
	u.snapshot = std::move(snapshot);
	u.entries.insert(u.entries.begin(), entries.begin(), entries.end());
	u.offset = offset;
	u.logger = &logger;
	return std::move(u);
}

MessagePtr make_message(
	uint64_t from,
	uint64_t to,
	MessageType type,
	uint64_t index,
	uint64_t term,
	bool reject,
	vector<Entry> &&ents,
	uint64_t logterm,
	uint64_t commit,
	uint64_t wrejectHint
) {
	MessagePtr msg = make_unique<Message>();
	msg->set_from(from);
	msg->set_to(to);
	msg->set_type(type);
	msg->set_term(term);
	msg->set_reject(reject);
	msg->set_index(index);
	msg->set_logterm(logterm);
	msg->set_commit(commit);
	msg->set_rejecthint(wrejectHint);
	auto dd = msg->mutable_entries();
	for (auto &ent : ents) *dd->Add() = ent;
	return std::move(msg);
}

string ltoa(raft_log *l) {
	auto s = (boost::format("committed: %d\n") % l->committed).str();
	s += (boost::format("applied:  %d\n") % l->applied).str();
	auto ents = l->allEntries();
	for (size_t i = 0; i < ents.size(); i++) {
		auto &e = ents[i];
		auto r = (boost::format("data:%1%, index:%2%, term:%3%, type:%4%") % e.data() % e.index() % e.term() % e.type()).str();
		s += (boost::format("#%d: %s\n") % i % r).str();
	}
	return s;
}

string diffu(const string &a, const string &b) {
	if (a == b) {
		return "";
	}
	return "diff: \n" + a + "\n" + b + "\n";
}

uint64_t mustTerm(const Result<uint64_t> &term) {
	if (!term.Ok()) {
		abort();
	}
	return term.value;
}

vector<uint64_t> idsBySize(size_t size) {
	vector<uint64_t> ids(size);
	for (size_t i = 0; i < size; i++) {
		ids[i] = 1 + uint64_t(i);
	}
	return ids;
}
