#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
#include <utils.hpp>

BOOST_AUTO_TEST_CASE(TestLimitSize) {
	vector<Entry> ents{ makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6) };
	struct {
		uint64_t maxsize;
		vector<Entry> wentries;
	} tests[] = {
		{std::numeric_limits<uint64_t>().max(),{ makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}},
		// even if maxsize is zero, the first entry should be returned
		{0,{ makeEntry(4, 4)}},
		// limit to 2
		{uint64_t(ents[0].ByteSize() + ents[1].ByteSize()),{ makeEntry(4, 4), makeEntry(5, 5)}},
		// limit to 2
		{uint64_t(ents[0].ByteSize() + ents[1].ByteSize() + ents[2].ByteSize() / 2),{ makeEntry(4, 4), makeEntry(5, 5)}},
		{uint64_t(ents[0].ByteSize() + ents[1].ByteSize() + ents[2].ByteSize() - 1),{ makeEntry(4, 4), makeEntry(5, 5)}},
		// all
		{uint64_t(ents[0].ByteSize() + ents[1].ByteSize() + ents[2].ByteSize()),{ makeEntry(4, 4), makeEntry(5, 5), makeEntry(6, 6)}},
	};
	for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
		auto &tt = tests[i];
		auto tmp = make_slice(ents);
		limitSize(tmp, tt.maxsize);
		equal_entrys(tmp, tt.wentries);
	}
}
