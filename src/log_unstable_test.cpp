#define BOOST_TEST_MODULE raft test
#include <boost/test/unit_test.hpp>
#include "test_common.hpp"

using namespace raft;
using namespace std;

BOOST_AUTO_TEST_CASE(TestUnstableMaybeFirstIndex) {
	struct {
		vector<Entry> entries;
		uint64_t offset;
		std::unique_ptr<Snapshot> snap;
		bool wok;
		uint64_t windex;
	} tests[] = {
		// no snapshot
		{{ makeEntry(5, 1) }, 5, nullptr, false, 0,},
		{{}, 0, nullptr,false, 0,},
		// has snapshot
		{{makeEntry(5, 1)}, 5, makeSnapshot(4, 1),true, 5,},
		{{}, 5, makeSnapshot(4, 1),true, 5,},
	};

	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		unstable u = make_unstable(std::move(tt.snap), std::move(tt.entries), tt.offset, DefaultLogger::instance());
		auto index = u.maybeFirstIndex();
		BOOST_REQUIRE_EQUAL(index.err == OK, tt.wok);
		BOOST_REQUIRE_EQUAL(index.value, tt.windex);
	}
}

BOOST_AUTO_TEST_CASE(TestMaybeLastIndex) {
	struct {
		vector<Entry> entries;
		uint64_t offset;
		std::unique_ptr<Snapshot> snap;
		bool wok;
		uint64_t windex;
	} tests[] = {
		// last in entries
		{{ makeEntry(5, 1)}, 5, nullptr,true, 5,},
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),true, 5,},
		// last in snapshot
		{{}, 5, makeSnapshot(4, 1),true, 4,},
		// empty unstable
		{{}, 0, nullptr,false, 0,},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		unstable u = make_unstable(std::move(tt.snap), std::move(tt.entries), tt.offset, DefaultLogger::instance());
		auto index = u.maybeLastIndex();
		BOOST_REQUIRE_EQUAL(index.err == OK, tt.wok);
		BOOST_REQUIRE_EQUAL(index.value, tt.windex);
	}
}

BOOST_AUTO_TEST_CASE(TestUnstableMaybeTerm) {
	struct {
		vector<Entry> entries;
		uint64_t offset;
		std::unique_ptr<Snapshot> snap;
		uint64_t index;

		bool wok;
		uint64_t wterm;
	} tests[] = {
		// term from entries
		{{ makeEntry(5, 1)}, 5, nullptr,5,true, 1},
		{{ makeEntry(5, 1)}, 5, nullptr,6,false, 0},
		{{ makeEntry(5, 1)}, 5, nullptr,4,false, 0},
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),5,true, 1},
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),6,false, 0},
		// term from snapshot
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),4,true, 1},
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),3,false, 0},
		{{}, 5, makeSnapshot(4, 1),5,false, 0},
		{{}, 5, makeSnapshot(4, 1),4,true, 1},
		{{}, 0, nullptr,5,false, 0}
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		unstable u = make_unstable(std::move(tt.snap), std::move(tt.entries), tt.offset, DefaultLogger::instance());
		auto term = u.maybeTerm(tt.index);
		BOOST_REQUIRE_EQUAL(term.err == OK, tt.wok);
		BOOST_REQUIRE_EQUAL(term.value, tt.wterm);
	}
}

BOOST_AUTO_TEST_CASE(TestUnstableRestore) {
	unstable u = make_unstable(makeSnapshot(4, 1), { makeEntry(5, 1) }, 5, DefaultLogger::instance());
	auto s = makeSnapshot(6, 2);
	u.restore(*s);

	BOOST_REQUIRE_EQUAL(u.offset, s->metadata().index() + 1);
	BOOST_REQUIRE_EQUAL(u.entries.empty(), true);
	BOOST_REQUIRE_EQUAL(u.snapshot->metadata().index(), s->metadata().index());
	BOOST_REQUIRE_EQUAL(u.snapshot->metadata().term(), s->metadata().term());
}

BOOST_AUTO_TEST_CASE(TestUnstableStableTo) {
	struct {
		vector<Entry> entries;
		uint64_t offset;
		std::unique_ptr<Snapshot> snap;
		uint64_t index;
		uint64_t term;

		uint64_t woffset;
		size_t wlen;
	} tests[] = {
		{{}, 0, nullptr,5, 1,0, 0,},
		{{ makeEntry(5, 1)}, 5, nullptr,5, 1,6, 0}, // stable to the first entry
		{{ makeEntry(5, 1), makeEntry(6, 1)}, 5, nullptr,5, 1,6, 1}, // stable to the first entry
		{{ makeEntry(6, 2)}, 6, nullptr,6, 1,6, 1}, // stable to the first entry and term mismatch
		{{ makeEntry(5, 1)}, 5, nullptr,4, 1,5, 1}, // stable to old entry
		{{ makeEntry(5, 1)}, 5, nullptr,4, 2,5, 1}, // stable to old entry
		// with snapshot
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),5, 1,6, 0}, // stable to the first entry
		{{ makeEntry(5, 1), makeEntry(6, 1)}, 5, makeSnapshot(4, 1),5, 1,6, 1}, // stable to the first entry
		{{ makeEntry(6, 2)}, 6, makeSnapshot(5, 1),6, 1, 6, 1}, // stable to the first entry and term mismatch
		{{ makeEntry(5, 1)}, 5, makeSnapshot(4, 1),4, 1,5, 1}, // stable to snapshot
		{{ makeEntry(5, 2)}, 5, makeSnapshot(4, 2),4, 1, 5, 1 } // stable to old entry
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		unstable u = make_unstable(std::move(tt.snap), std::move(tt.entries), tt.offset, DefaultLogger::instance());
		u.stableTo(tt.index, tt.term);
		BOOST_REQUIRE_EQUAL(u.offset, tt.woffset);
		BOOST_REQUIRE_EQUAL(u.entries.size(), tt.wlen);
	}
}

BOOST_AUTO_TEST_CASE(TestUnstableTruncateAndAppend) {
	struct {
		vector<Entry> entries;
		uint64_t offset;
		std::unique_ptr<Snapshot> snap;
		vector<Entry> toappend;

		uint64_t woffset;
		vector<Entry> wentries;
	} tests[] = {
		// append to the end
		{
			{ makeEntry(5, 1)}, 5, nullptr, { makeEntry(6, 1), makeEntry(7, 1)},
			5, {makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 1)},
		},
		// replace the unstable entries
		{
			{ makeEntry(5, 1)}, 5, nullptr, { makeEntry(5, 2), makeEntry(6, 2)},
			5, {makeEntry(5, 2), makeEntry(6, 2)},
		},
		{
			{ makeEntry(5, 1)}, 5, nullptr, {makeEntry(4, 2), makeEntry(5, 2), makeEntry(6, 2)},
			4,{makeEntry(4, 2), makeEntry(5, 2), makeEntry(6, 2)},
		},
		// truncate the existing entries and append
		{
			{makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 1)}, 5, nullptr, {makeEntry(6, 2)},
			5,{makeEntry(5, 1), makeEntry(6, 2)},
		},
		{
			{makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 1)}, 5, nullptr, {makeEntry(7, 2), makeEntry(8, 2)},
			5, {makeEntry(5, 1), makeEntry(6, 1), makeEntry(7, 2), makeEntry(8, 2)},
		},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		unstable u = make_unstable(std::move(tt.snap), std::move(tt.entries), tt.offset, DefaultLogger::instance());
		u.truncateAndAppend(make_slice(tt.toappend));
		BOOST_REQUIRE_EQUAL(u.offset, tt.woffset);
		equal_entrys(u.entries, tt.wentries);
	}
}
// BOOST_AUTO_TEST_SUITE_END()
