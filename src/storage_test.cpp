#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
using namespace raft;
using namespace raftpb;

Snapshot makeSnapshot(const string &data, uint64_t index, uint64_t term, const ConfState &cs) {
	Snapshot sh;
	sh.set_data(data);
	sh.mutable_metadata()->set_index(index);
	sh.mutable_metadata()->set_term(term);
	*sh.mutable_metadata()->mutable_conf_state() = cs;
	return std::move(sh);
};

BOOST_AUTO_TEST_CASE(TestStorageTerm) {
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5) };
	struct {
		uint64_t i;

		ErrorCode werr;
		uint64_t wterm;
		bool wpanic;
	} tests[] = {
		{2, ErrCompacted, 0, false},
		{3, OK, 3, false},
		{4, OK, 4, false},
		{5, OK, 5, false},
		{6, ErrUnavailable, 0, false},
	};

	for (auto &tt : tests) {
		auto s = std::make_shared<MemoryStorage>(ents);
		try {
			uint64_t term;
			auto err = s->term(tt.i, term);
			BOOST_REQUIRE_EQUAL(err, tt.werr);
			BOOST_REQUIRE_EQUAL(term, tt.wterm);
		} catch (const std::runtime_error &) {
			BOOST_REQUIRE_EQUAL(true, tt.wpanic);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestStorageEntries) {
	auto maxLimit = std::numeric_limits<uint64_t>().max();
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6) };
	struct {
		uint64_t lo, hi, maxsize;

		ErrorCode werr;
		vector<Entry> wentries;
	} tests[] = {
		{2, 6, maxLimit, ErrCompacted, {}},
		{3, 4, maxLimit, ErrCompacted, {}},
		{4, 5, maxLimit, OK, {makeEntry(4, 4)}},
		{4, 6, maxLimit, OK, {makeEntry(4, 4), makeEntry(5, 5)}},
		{4, 7, maxLimit, OK, {makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, OK, {makeEntry(4, 4)}},
		// limit to 2
		{4, 7, uint64_t(ents[1].ByteSize() + ents[2].ByteSize()), OK, {makeEntry(4, 4), makeEntry(5, 5)}},
		// limit to 2
		{4, 7, uint64_t(ents[1].ByteSize() + ents[2].ByteSize() + ents[3].ByteSize() / 2), OK, {makeEntry(4, 4), makeEntry(5, 5)}},
		{4, 7, uint64_t(ents[1].ByteSize() + ents[2].ByteSize() + ents[3].ByteSize() - 1), OK, {makeEntry(4, 4), makeEntry(5, 5)}},
		// all
		{4, 7, uint64_t(ents[1].ByteSize() + ents[2].ByteSize() + ents[3].ByteSize()), OK, {makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}},
	};

	for (auto &tt : tests) {
		auto s = std::make_shared<MemoryStorage>(ents);
		vector<Entry> entries;
		auto err = s->entries(tt.lo, tt.hi, tt.maxsize, entries);
		BOOST_REQUIRE_EQUAL(err, tt.werr);
		equal_entrys(entries, tt.wentries);
	}
}

BOOST_AUTO_TEST_CASE(TestStorageLastIndex) {
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5) };
	auto s = std::make_shared<MemoryStorage>(ents);

	uint64_t last;
	auto err = s->last_index(last);
	BOOST_REQUIRE_EQUAL(err, OK);
	BOOST_REQUIRE_EQUAL(last, 5);

	s->append({ makeEntry(6, 5) });
	err = s->last_index(last);
	BOOST_REQUIRE_EQUAL(err, OK);
	BOOST_REQUIRE_EQUAL(last, 6);
}

BOOST_AUTO_TEST_CASE(TestStorageFirstIndex) {
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5) };
	auto s = std::make_shared<MemoryStorage>(ents);

	uint64_t first;
	auto err = s->first_index(first);
	BOOST_REQUIRE_EQUAL(err, OK);
	BOOST_REQUIRE_EQUAL(first, 4);

	s->Compact(4);
	err = s->first_index(first);
	BOOST_REQUIRE_EQUAL(err, OK);
	BOOST_REQUIRE_EQUAL(first, 5);
}

BOOST_AUTO_TEST_CASE(TestStorageCompact) {
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5) };
	struct {
		uint64_t i;

		ErrorCode werr;
		uint64_t windex;
		uint64_t wterm;
		int wlen;
	}tests[] = {
		{2, ErrCompacted, 3, 3, 3},
		{3, ErrCompacted, 3, 3, 3},
		{4, OK, 4, 4, 2},
		{5, OK, 5, 5, 1},
	};

	for (auto &tt : tests) {
		auto s = std::make_shared<MemoryStorage>(ents);
		auto err = s->Compact(tt.i);
		BOOST_REQUIRE_EQUAL(err, tt.werr);
		BOOST_REQUIRE_EQUAL(s->m_entries[0].index(), tt.windex);
		BOOST_REQUIRE_EQUAL(s->m_entries[0].term(), tt.wterm);
		BOOST_REQUIRE_EQUAL(s->m_entries.size(), tt.wlen);
	}
}

BOOST_AUTO_TEST_CASE(TestStorageCreateSnapshot) {
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5) };
	ConfState cs;
	for (uint64_t i : {1, 2, 3}) {
		*cs.mutable_nodes()->Add() = i;
	}
	auto data = "data";
	struct {
		uint64_t i;

		ErrorCode werr;
		Snapshot wsnap;
	}tests[] = {
		{ 4, OK, makeSnapshot(data, 4, 4, cs) },
		{ 5, OK, makeSnapshot(data, 5, 5, cs) },
	};

	for (auto &tt : tests) {
		auto s = std::make_shared<MemoryStorage>(ents);
		Snapshot snap;
		auto err = s->CreateSnapshot(tt.i, &cs, data, snap);
		BOOST_REQUIRE_EQUAL(err, tt.werr);
		BOOST_REQUIRE_EQUAL(snap.metadata().index(), tt.wsnap.metadata().index());
		BOOST_REQUIRE_EQUAL(snap.metadata().term(), tt.wsnap.metadata().term());
		BOOST_REQUIRE_EQUAL(snap.data(), tt.wsnap.data());
		auto &conf1 = snap.metadata().conf_state();
		auto &conf2 = tt.wsnap.metadata().conf_state();
		BOOST_REQUIRE_EQUAL(conf1.nodes_size(), conf2.nodes_size());
		for (int i = 0; i < conf1.nodes_size(); i++) {
			BOOST_REQUIRE_EQUAL(conf1.nodes(i), conf2.nodes(i));
		}
	}
}

BOOST_AUTO_TEST_CASE(TestStorageAppend) {
	vector<Entry> ents = { makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5) };
	struct {
		vector<Entry> entries;

		ErrorCode werr;
		vector<Entry> wentries;
	} tests[] = {
		{
			{makeEntry(1, 1), makeEntry(2, 2)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)},
		},
		{
			{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)},
		},
		{
			{makeEntry(3, 3), makeEntry(4, 6), makeEntry(5, 6)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 6), makeEntry(5, 6)},
		},
		{
			{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			{makeEntry(2, 3), makeEntry(3, 3), makeEntry(4, 5)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 5)},
		},
		// truncate the existing entries and append
		{
			{makeEntry(4, 5)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 5)},
		},
		// direct append
		{
			{makeEntry(6, 5)},
			OK,
			{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 5)},
		},
	};

	for (auto &tt : tests) {
		auto s = std::make_shared<MemoryStorage>(ents);
		auto err = s->append(tt.entries);
		BOOST_REQUIRE_EQUAL(err, tt.werr);
		equal_entrys(s->m_entries, tt.wentries);
	}
}

BOOST_AUTO_TEST_CASE(TestStorageApplySnapshot) {
	ConfState cs;
	for (uint64_t i : {1, 2, 3}) {
		*cs.mutable_nodes()->Add() = i;
	}
	auto data = "data";

	Snapshot tests[] = {
		makeSnapshot(data, 4, 4, cs),
		makeSnapshot(data, 3, 3, cs),
	};

	auto s = std::make_shared<MemoryStorage>();

	//Apply Snapshot successful
	int i = 0;
	auto &tt = tests[i];
	auto err = s->apply_snapshot(tt);
	BOOST_REQUIRE_EQUAL(err, OK);

	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1;
	tt = tests[i];
	err = s->apply_snapshot(tt);
	BOOST_REQUIRE_EQUAL(err, ErrSnapOutOfDate);
}
