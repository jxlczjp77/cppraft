#include "test_common.hpp"
#include <boost/test/unit_test.hpp>

struct doexit {
	~doexit() {
		google::protobuf::ShutdownProtobufLibrary();
	}
} t;

Entry makeEntry(uint64_t index, uint64_t term, string &&data) {
	Entry tmp;
	tmp.set_index(index);
	tmp.set_term(term);
	tmp.set_data(std::move(data));
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
	u.m_snapshot = std::move(snapshot);
	u.m_entries = std::move(entries);
	u.m_offset = offset;
	u.m_logger = &logger;
	return std::move(u);
}

void equal_entrys(const vector<Entry> &left, const vector<Entry> &right) {
	BOOST_REQUIRE_EQUAL(left.size(), right.size());
	for (size_t i = 0; i < left.size(); i++) {
		auto &e1 = left[i];
		auto &e2 = right[i];
		BOOST_REQUIRE_EQUAL(e1.index(), e2.index());
		BOOST_REQUIRE_EQUAL(e1.term(), e2.term());
	}
	auto l = &DefaultLogger::instance();
}
