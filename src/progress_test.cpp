#include <boost/test/unit_test.hpp>
#include "test_common.hpp"

inflights make_inflights(int start, int count, int size, vector<uint64_t> &&buf) {
    inflights wantIn(size);
    wantIn.start = start;
    wantIn.count = count;
    wantIn.buffer = std::move(buf);
    return wantIn;
}

void equal_inflights(const inflights &left, const inflights &right) {
    BOOST_REQUIRE_EQUAL(left.count, right.count);
    BOOST_REQUIRE_EQUAL(left.size, right.size);
    BOOST_REQUIRE_EQUAL(left.start, right.start);
    BOOST_REQUIRE_EQUAL(left.buffer.size(), right.buffer.size());
    for (size_t i = 0; i < left.buffer.size(); i++) {
        BOOST_REQUIRE_EQUAL(left.buffer[i], right.buffer[i]);
    }
}

BOOST_AUTO_TEST_CASE(TestInflightsAdd) {
    // no rotating case
    inflights in = make_inflights(0, 0, 10, vector<uint64_t>(10));
    for (uint64_t i = 0; i < 5; i++) {
        in.add(i);
    }
    inflights wantIn = make_inflights(0, 5, 10, { 0, 1, 2, 3, 4, 0, 0, 0, 0, 0 });
    equal_inflights(in, wantIn);

    for (uint64_t i = 5; i < 10; i++) {
        in.add(i);
    }
    inflights wantIn2 = make_inflights(0, 10, 10, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    equal_inflights(in, wantIn2);

    // rotating case
    inflights in2 = make_inflights(5, 0, 10, vector<uint64_t>(10));
    for (uint64_t i = 0; i < 5; i++) {
        in2.add(i);
    }

    inflights wantIn21 = make_inflights(5, 5, 10, { 0, 0, 0, 0, 0, 0, 1, 2, 3, 4 });
    equal_inflights(in2, wantIn21);

    for (uint64_t i = 5; i < 10; i++) {
        in2.add(i);
    }
    inflights wantIn22 = make_inflights(5, 10, 10, { 5, 6, 7, 8, 9, 0, 1, 2, 3, 4 });
    equal_inflights(in2, wantIn22);
}

BOOST_AUTO_TEST_CASE(TestInflightFreeTo) {
    // no rotating case
    inflights in{ 10 };
    for (uint64_t i = 0; i < 10; i++) {
        in.add(i);
    }
    in.freeTo(4);

    inflights wantIn = make_inflights(5, 5, 10, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    equal_inflights(in, wantIn);

    in.freeTo(8);
    inflights wantIn2 = make_inflights(9, 1, 10, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    equal_inflights(in, wantIn2);

    // rotating case
    for (uint64_t i = 10; i < 15; i++) {
        in.add(i);
    }
    in.freeTo(12);

    inflights wantIn3 = make_inflights(3, 2, 10, { 10, 11, 12, 13, 14, 5, 6, 7, 8, 9 });
    equal_inflights(in, wantIn3);

    in.freeTo(14);
    inflights wantIn4 = make_inflights(0, 0, 10, { 10, 11, 12, 13, 14, 5, 6, 7, 8, 9 });
    equal_inflights(in, wantIn4);
}

BOOST_AUTO_TEST_CASE(TestInflightFreeFirstOne) {
    inflights in{ 10 };
    for (uint64_t i = 0; i < 10; i++) {
        in.add(i);
    }
    in.freeFirstOne();
    inflights wantIn = make_inflights(1, 9, 10, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    equal_inflights(in, wantIn);
}
