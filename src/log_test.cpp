#include <boost/test/unit_test.hpp>
#include "test_common.hpp"

BOOST_AUTO_TEST_CASE(TestFindConflict) {
	vector<Entry> previousEnts{ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) };
	struct {
		vector<Entry> ents;
		uint64_t wconflict;
	} tests[] = {
		// no conflict, empty ent
		{{}, 0},
		// no conflict
		{ {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}, 0 },
		{ {makeEntry(2, 2), makeEntry(3, 3)}, 0 },
		{ {makeEntry(3, 3)}, 0 },
		// no conflict, but has new entries
		{ {makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4 },
		{ {makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4 },
		{ {makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4 },
		{ {makeEntry(4, 4), makeEntry(5, 4)}, 4 },
		// conflicts with existing entries
		{ {makeEntry(1, 4), makeEntry(2, 4)}, 1 },
		{ {makeEntry(2, 1), makeEntry(3, 4), makeEntry(4, 4)}, 2 },
		{ {makeEntry(3, 1), makeEntry(4, 2), makeEntry(5, 4), makeEntry(6, 4)}, 3 },
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		raft_log raftLog(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
		raftLog.append(previousEnts);
		uint64_t conflict = raftLog.findConflict(tt.ents);
		BOOST_REQUIRE_EQUAL(conflict, tt.wconflict);
	}
}

BOOST_AUTO_TEST_CASE(TestIsUpToDate) {
	vector<Entry> previousEnts{ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) };
	raft_log raftLog(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
	raftLog.append(previousEnts);
	struct {
		uint64_t lastIndex;
		uint64_t term;
		bool wUpToDate;
	} tests[] = {
		// greater term, ignore lastIndex
		{raftLog.lastIndex() - 1, 4, true},
		{raftLog.lastIndex(), 4, true},
		{raftLog.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.lastIndex() - 1, 2, false},
		{raftLog.lastIndex(), 2, false},
		{raftLog.lastIndex() + 1, 2, false},
		// equal term, equal or lager lastIndex wins
		{raftLog.lastIndex() - 1, 3, false},
		{raftLog.lastIndex(), 3, true},
		{raftLog.lastIndex() + 1, 3, true},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		bool gUpToDate = raftLog.isUpToDate(tt.lastIndex, tt.term);
		BOOST_REQUIRE_EQUAL(gUpToDate, tt.wUpToDate);
	}
}

BOOST_AUTO_TEST_CASE(TestAppend) {
	vector<Entry> previousEnts{ makeEntry(1, 1), makeEntry(2, 2) };
	struct {
		vector<Entry> ents;
		uint64_t windex;
		vector<Entry> wents;
		uint64_t wunstable;
	} tests[] = {
		{{},2,{makeEntry(1,1), makeEntry(2,2)},3},
		{{makeEntry(3,2)},3,{makeEntry(1,1), makeEntry(2,2), makeEntry(3,2)},3,},
		// conflicts with index 1
		{{makeEntry(1,2)},1,{makeEntry(1,2)},1,},
		// conflicts with index 2
		{{makeEntry(2,3), makeEntry(3,3)},3,{makeEntry(1,1), makeEntry(2,3), makeEntry(3,3)},2,},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->Append(previousEnts);
		raft_log raftLog(storage, &DefaultLogger::instance());
		uint64_t index = raftLog.append(tt.ents);
		BOOST_REQUIRE_EQUAL(index, tt.windex);
		auto ents = raftLog.entries(1);
		BOOST_REQUIRE_EQUAL(ents.err, OK);
		equal_entrys(ents.value, tt.wents);
		BOOST_REQUIRE_EQUAL(raftLog.unstable.offset, tt.wunstable);
	}
}

BOOST_AUTO_TEST_CASE(TestLogMaybeAppend) {
	vector<Entry> previousEnts{ makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3) };
	uint64_t lastindex = 3, lastterm = 3, commit = 1;
	struct {
		uint64_t logTerm;
		uint64_t index;
		uint64_t committed;
		vector<Entry> ents;

		uint64_t wlasti;
		bool wappend;
		uint64_t wcommit;
		bool wpanic;
	} tests[] = {
		// not match: term is different
		{
			lastterm - 1, lastindex, lastindex, {makeEntry(lastindex + 1, 4)},
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			lastterm, lastindex + 1, lastindex, {makeEntry(lastindex + 2, 4)},
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			lastterm, lastindex, lastindex, {},
			lastindex, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, {},
			lastindex, true, lastindex, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex - 1, {},
			lastindex, true, lastindex - 1, false, // commit up to the commit in the message
		},
		{
			lastterm, lastindex, 0, {},
			lastindex, true, commit, false, // commit do not decrease
		},
		{
			0, 0, lastindex, {},
			0, true, commit, false, // commit do not decrease
		},
		{
			lastterm, lastindex, lastindex, {makeEntry(lastindex + 1, 4)},
			lastindex + 1, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, {makeEntry(lastindex + 1, 4)},
			lastindex + 1, true, lastindex + 1, false,
		},
		{
			lastterm, lastindex, lastindex + 2, {makeEntry(lastindex + 1, 4)},
			lastindex + 1, true, lastindex + 1, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex + 2, {makeEntry(lastindex + 1, 4), makeEntry(lastindex + 2, 4)},
			lastindex + 2, true, lastindex + 2, false,
		},
		// match with the the entry in the middle
		{
			lastterm - 1, lastindex - 1, lastindex, {makeEntry(lastindex, 4)},
			lastindex, true, lastindex, false,
		},
		{
			lastterm - 2, lastindex - 2, lastindex, {makeEntry(lastindex - 1, 4)},
			lastindex - 1, true, lastindex - 1, false,
		},
		{
			lastterm - 3, lastindex - 3, lastindex, {makeEntry(lastindex - 2, 4)},
			lastindex - 2, true, lastindex - 2, true, // conflict with existing committed entry
		},
		{
			lastterm - 2, lastindex - 2, lastindex, {makeEntry(lastindex - 1, 4), makeEntry(lastindex, 4)},
			lastindex, true, lastindex, false,
		},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		raft_log raftLog(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
		raftLog.append(previousEnts);
		raftLog.committed = commit;
		try {
			uint64_t glasti;
			bool gappend = raftLog.maybeAppend(tt.index, tt.logTerm, tt.committed, tt.ents, glasti);
			uint64_t gcommit = raftLog.committed;
			BOOST_REQUIRE_EQUAL(glasti, tt.wlasti);
			BOOST_REQUIRE_EQUAL(gappend, tt.wappend);
			BOOST_REQUIRE_EQUAL(gcommit, tt.wcommit);
			if (gappend && !tt.ents.empty()) {
				auto gents = raftLog.slice(raftLog.lastIndex() - tt.ents.size() + 1, raftLog.lastIndex() + 1);
				BOOST_REQUIRE_EQUAL(gents.err, OK);
				equal_entrys(gents.value, tt.ents);
			}
		} catch (const std::runtime_error &) {
			BOOST_REQUIRE_EQUAL(tt.wpanic, true);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestCompactionSideEffects) {
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	uint64_t lastIndex = 1000;
	uint64_t unstableIndex = 750;
	uint64_t lastTerm = lastIndex;
	auto storage = std::make_shared<MemoryStorage>();
	for (uint64_t i = 1; i <= unstableIndex; i++) {
		storage->Append(makeEntry(i, i));
	}
	raft_log raftLog(storage, &DefaultLogger::instance());
	for (uint64_t i = unstableIndex; i < lastIndex; i++) {
		raftLog.append(makeEntry(i + 1, i + 1));
	}

	bool ok = raftLog.maybeCommit(lastIndex, lastTerm);
	BOOST_REQUIRE_EQUAL(ok, true);
	raftLog.appliedTo(raftLog.committed);
	uint64_t offset = 500;
	storage->Compact(offset);
	BOOST_REQUIRE_EQUAL(raftLog.lastIndex(), lastIndex);
	for (uint64_t j = offset; j <= raftLog.lastIndex(); j++) {
		auto t = raftLog.term(j);
		BOOST_REQUIRE_EQUAL(mustTerm(t), j);
	}

	for (uint64_t j = offset; j <= raftLog.lastIndex(); j++) {
		BOOST_REQUIRE_EQUAL(raftLog.matchTerm(j, j), true);
	}

	auto &unstableEnts = raftLog.unstableEntries();
	BOOST_REQUIRE_EQUAL(unstableEnts.size(), 250);
	BOOST_REQUIRE_EQUAL(unstableEnts[0].index(), 751);

	uint64_t prev = raftLog.lastIndex();
	raftLog.append(makeEntry(raftLog.lastIndex() + 1, raftLog.lastIndex() + 1));
	BOOST_REQUIRE_EQUAL(raftLog.lastIndex(), prev + 1);

	auto ents = raftLog.entries(raftLog.lastIndex());
	BOOST_REQUIRE_EQUAL(ents.err, OK);
	BOOST_REQUIRE_EQUAL(ents.value.size(), 1);
}

BOOST_AUTO_TEST_CASE(TestHasNextEnts) {
	auto snap = makeSnapshot(3, 1);
	vector<Entry> ents{ makeEntry(4, 1),makeEntry(5, 1),makeEntry(6, 1) };
	struct {
		uint64_t applied;
		bool hasNext;
	} tests[] = {
		{0, true},
		{3, true},
		{4, true},
		{5, false},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->ApplySnapshot(*snap);
		raft_log raftLog(storage, &DefaultLogger::instance());
		raftLog.append(ents);
		raftLog.maybeCommit(5, 1);
		raftLog.appliedTo(tt.applied);
		BOOST_REQUIRE_EQUAL(raftLog.hasNextEnts(), tt.hasNext);
	}
}

BOOST_AUTO_TEST_CASE(TestNextEnts) {
	auto snap = makeSnapshot(3, 1);
	vector<Entry> ents{ makeEntry(4, 1),makeEntry(5, 1),makeEntry(6, 1) };
	struct {
		uint64_t applied;
		vector<Entry> wents;
	} tests[] = {
		{0, {ents.begin(), ents.begin() + 2}},
		{3, {ents.begin(), ents.begin() + 2}},
		{4, {ents.begin() + 1, ents.begin() + 2}},
		{5, {}},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->ApplySnapshot(*snap);
		raft_log raftLog(storage, &DefaultLogger::instance());
		raftLog.append(ents);
		raftLog.maybeCommit(5, 1);
		raftLog.appliedTo(tt.applied);
		auto nents = raftLog.nextEnts();
		equal_entrys(nents, tt.wents);
	}
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
BOOST_AUTO_TEST_CASE(TestUnstableEnts) {
	vector<Entry> previousEnts{ makeEntry(1, 1),makeEntry(2, 2) };
	struct {
		uint64_t unstable;
		vector<Entry> wents;
	} tests[] = {
		{3, {}},
		{1, previousEnts},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		vector<Entry> ee{ previousEnts.begin(), (previousEnts.begin() + (tt.unstable - 1)) };
		storage->Append(ee);
		raft_log raftLog(storage, &DefaultLogger::instance());
		raftLog.append(EntryVec{ previousEnts.begin() + (tt.unstable - 1), previousEnts.end() });
		auto ents = raftLog.unstableEntries();
		if (!ents.empty()) {
			raftLog.stableTo(ents[ents.size() - 1].index(), ents[ents.size() - i].term());
		}
		equal_entrys(ents, tt.wents);
		uint64_t w = previousEnts[previousEnts.size() - 1].index() + 1;
		uint64_t g = raftLog.unstable.offset;
		BOOST_REQUIRE_EQUAL(w, g);
	}
}

BOOST_AUTO_TEST_CASE(TestCommitTo) {
	vector<Entry> previousEnts{ makeEntry(1, 1),makeEntry(2, 2),makeEntry(3, 3) };
	uint64_t commit = 2;
	struct {
		uint64_t commit;
		uint64_t wcommit;
		bool wpanic;
	} tests[] = {
		{3, 3, false},
		{1, 2, false}, // never decrease
		{4, 0, true},  // commit out of range -> panic
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		try {
			raft_log raftLog(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
			raftLog.append(previousEnts);
			raftLog.committed = commit;
			raftLog.commitTo(tt.commit);
			BOOST_REQUIRE_EQUAL(raftLog.committed, tt.wcommit);
		} catch (const std::runtime_error &) {
			BOOST_REQUIRE_EQUAL(tt.wpanic, true);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestStableTo) {
	struct {
		uint64_t stablei;
		uint64_t stablet;
		uint64_t wunstable;
	} tests[] = {
		{1, 1, 2},
		{2, 2, 3},
		{2, 1, 1}, // bad term
		{3, 1, 1}, // bad index
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		raft_log raftLog(std::make_shared<MemoryStorage>(), &DefaultLogger::instance());
		raftLog.append(EntryVec{ makeEntry(1, 1),makeEntry(2, 2) });
		raftLog.stableTo(tt.stablei, tt.stablet);
		BOOST_REQUIRE_EQUAL(raftLog.unstable.offset, tt.wunstable);
	}
}

BOOST_AUTO_TEST_CASE(TestStableToWithSnap) {
	uint64_t snapi = 5, snapt = 2;
	struct {
		uint64_t stablei;
		uint64_t stablet;
		vector<Entry> newEnts;
		uint64_t wunstable;
	} tests[] = {
		{snapi + 1, snapt, {}, snapi + 1},
		{snapi, snapt, {}, snapi + 1},
		{snapi - 1, snapt, {}, snapi + 1},

		{snapi + 1, snapt + 1, {}, snapi + 1},
		{snapi, snapt + 1, {}, snapi + 1},
		{snapi - 1, snapt + 1, {}, snapi + 1},

		{snapi + 1, snapt,{makeEntry(snapi + 1, snapt)}, snapi + 2},
		{snapi, snapt,{makeEntry(snapi + 1, snapt)}, snapi + 1},
		{snapi - 1, snapt,{makeEntry(snapi + 1, snapt)}, snapi + 1},

		{snapi + 1, snapt + 1,{makeEntry(snapi + 1, snapt)}, snapi + 1},
		{snapi, snapt + 1,{makeEntry(snapi + 1, snapt)}, snapi + 1},
		{snapi - 1, snapt + 1,{makeEntry(snapi + 1, snapt)}, snapi + 1},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto storage = std::make_shared<MemoryStorage>();
		storage->ApplySnapshot(*makeSnapshot(snapi, snapt));
		raft_log raftLog(storage, &DefaultLogger::instance());
		raftLog.append(tt.newEnts);
		raftLog.stableTo(tt.stablei, tt.stablet);
		BOOST_REQUIRE_EQUAL(raftLog.unstable.offset, tt.wunstable);
	}
}

//TestCompaction ensures that the number of log entries is correct after compactions.
BOOST_AUTO_TEST_CASE(TestCompaction) {
	struct {
		uint64_t lastIndex;
		vector<uint64_t> compact;
		vector<int> wleft;
		bool wallow;
	} tests[] = {
		// out of upper bound
		{1000,{1001},{-1}, false},
		{1000,{300, 500, 800, 900},{700, 500, 200, 100}, true},
		// out of lower bound
		{1000,{300, 299},{700, -1}, false},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		try {
			auto storage = std::make_shared<MemoryStorage>();
			for (uint64_t i = 1; i <= tt.lastIndex; i++) {
				storage->Append(makeEntry(i, 0));
			}
			raft_log raftLog(storage, &DefaultLogger::instance());
			raftLog.maybeCommit(tt.lastIndex, 0);
			raftLog.appliedTo(raftLog.committed);
			for (size_t j = 0; j < tt.compact.size(); j++) {
				auto err = storage->Compact(tt.compact[j]);
				if (err != OK) {
					BOOST_REQUIRE_EQUAL(tt.wallow, false);
					continue;
				}
				BOOST_REQUIRE_EQUAL(raftLog.allEntries().size(), tt.wleft[j]);
			}
		} catch (const std::runtime_error &) {
			BOOST_REQUIRE_EQUAL(tt.wallow, false);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestLogRestore) {
	uint64_t index = 1000, term = 1000;
	auto storage = std::make_shared<MemoryStorage>();
	storage->ApplySnapshot(*makeSnapshot(index, term));
	raft_log raftLog(storage, &DefaultLogger::instance());
	BOOST_REQUIRE_EQUAL(raftLog.allEntries().empty(), true);
	BOOST_REQUIRE_EQUAL(raftLog.firstIndex(), index + 1);
	BOOST_REQUIRE_EQUAL(raftLog.committed, index);
	BOOST_REQUIRE_EQUAL(raftLog.unstable.offset, index + 1);
	auto t = raftLog.term(index);
	BOOST_REQUIRE_EQUAL(t.value, term);
}

BOOST_AUTO_TEST_CASE(TestIsOutOfBounds) {
	uint64_t offset = 100, num = 100;
	auto storage = std::make_shared<MemoryStorage>();
	storage->ApplySnapshot(*makeSnapshot(offset, 0));
	raft_log l(storage, &DefaultLogger::instance());
	for (uint64_t i = 1; i <= num; i++) {
		l.append(makeEntry(i + offset, 0));
	}

	uint64_t first = offset + 1;
	struct {
		uint64_t lo, hi;
		bool wpanic;
		bool wErrCompacted;
	} tests[] = {
		{first - 2, first + 1,false,true,},
		{first - 1, first + 1,false,true,},
		{first, first,false,false,},
		{first + num / 2, first + num / 2,false,false,},
		{first + num - 1, first + num - 1,false,false,},
		{first + num, first + num,false,false,},
		{first + num, first + num + 1,true,false,},
		{first + num + 1, first + num + 1,true,false,},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		try {
			auto err = l.mustCheckOutOfBounds(tt.lo, tt.hi);
			BOOST_REQUIRE_EQUAL(tt.wpanic, false);
			BOOST_REQUIRE_EQUAL(err == ErrCompacted, tt.wErrCompacted);
			BOOST_REQUIRE_EQUAL(err != OK, tt.wErrCompacted);
		} catch (const std::runtime_error &) {
			BOOST_REQUIRE_EQUAL(tt.wpanic, true);
		}
	}
}

BOOST_AUTO_TEST_CASE(TestTerm) {
	uint64_t offset = 100, num = 100;
	auto storage = std::make_shared<MemoryStorage>();
	storage->ApplySnapshot(*makeSnapshot(offset, 1));
	raft_log l(storage, &DefaultLogger::instance());
	for (uint64_t i = 1; i < num; i++) {
		l.append(makeEntry(i + offset, i));
	}

	struct {
		uint64_t index;
		uint64_t w;
	} tests[] = {
		{offset - 1, 0},
		{offset, 1},
		{offset + num / 2, num / 2},
		{offset + num - 1, num - 1},
		{offset + num, 0},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto t = l.term(tt.index);
		auto w = mustTerm(t);
		BOOST_REQUIRE_EQUAL(tt.w, w);
	}
}

BOOST_AUTO_TEST_CASE(TestTermWithUnstableSnapshot) {
	uint64_t storagesnapi = 100;
	uint64_t unstablesnapi = storagesnapi + 5;
	auto storage = std::make_shared<MemoryStorage>();
	storage->ApplySnapshot(*makeSnapshot(storagesnapi, 1));
	raft_log l(storage, &DefaultLogger::instance());
	l.restore(*makeSnapshot(unstablesnapi, 1));

	struct {
		uint64_t index;
		uint64_t w;
	} tests[] = {
		// cannot get term from storage
		{storagesnapi, 0},
		// cannot get term from the gap between storage ents and unstable snapshot
		{storagesnapi + 1, 0},
		{unstablesnapi - 1, 0},
		// get term from unstable snapshot index
		{unstablesnapi, 1},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto t = l.term(tt.index);
		auto w = mustTerm(t);
		BOOST_REQUIRE_EQUAL(tt.w, w);
	}
}

BOOST_AUTO_TEST_CASE(TestSlice) {
	uint64_t offset = 100, num = 100;
	uint64_t last = offset + num;
	uint64_t half = offset + num / 2;
	auto halfe = makeEntry(half, half);
	auto storage = std::make_shared<MemoryStorage>();
	storage->ApplySnapshot(*makeSnapshot(offset, 0));
	for (uint64_t i = 1; i < num / 2; i++) {
		storage->Append(makeEntry(offset + i, offset + i));
	}
	raft_log l(storage, &DefaultLogger::instance());
	for (uint64_t i = num / 2; i < num; i++) {
		l.append(makeEntry(offset + i, offset + i));
	}
	const uint64_t noLimit = std::numeric_limits<uint64_t>().max();
	struct {
		uint64_t from;
		uint64_t to;
		uint64_t limit;

		vector<Entry> w;
		bool wpanic;
	} tests[] = {
		// test no limit
		{offset - 1, offset + 1, noLimit, {}, false},
		{offset, offset + 1, noLimit, {}, false},
		{half - 1, half + 1, noLimit,{ makeEntry(half - 1, half - 1), makeEntry(half, half) }, false},
		{half, half + 1, noLimit,{ makeEntry(half, half) }, false},
		{last - 1, last, noLimit,{ makeEntry(last - 1, last - 1) }, false},
		{last, last + 1, noLimit, {}, true},

		// test limit
		{half - 1, half + 1, 0,{ makeEntry(half - 1, half - 1) }, false},
		{half - 1, half + 1, uint64_t(halfe.ByteSize() + 1),{ makeEntry(half - 1, half - 1) }, false},
		{half - 2, half + 1, uint64_t(halfe.ByteSize() + 1),{ makeEntry(half - 2, half - 2) }, false},
		{half - 1, half + 1, uint64_t(halfe.ByteSize() * 2),{ makeEntry(half - 1, half - 1), makeEntry(half, half) }, false},
		{half - 1, half + 2, uint64_t(halfe.ByteSize() * 3),{ makeEntry(half - 1, half - 1), makeEntry(half, half), makeEntry(half + 1, half + 1) }, false},
		{half, half + 2, uint64_t(halfe.ByteSize()),{ makeEntry(half, half) }, false},
		{half, half + 2, uint64_t(halfe.ByteSize() * 2),{ makeEntry(half, half), makeEntry(half + 1, half + 1) }, false},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		try {
			auto g = l.slice(tt.from, tt.to, tt.limit);
			BOOST_REQUIRE_EQUAL(tt.from <= offset && g.err != ErrCompacted, false);
			BOOST_REQUIRE_EQUAL(tt.from > offset && g.err != OK, false);
			equal_entrys(g.value, tt.w);
		} catch (const runtime_error &) {
			BOOST_REQUIRE_EQUAL(tt.wpanic, true);
		}
	}
}
