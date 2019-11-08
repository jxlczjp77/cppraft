#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
using namespace raft;
using namespace raftpb;

auto testingSnap = []() {
    auto sn = makeSnapshot(11, 11);
    for (uint64_t i : {1, 2}) {
        auto nodes = sn->mutable_metadata()->mutable_conf_state()->mutable_nodes();
        *nodes->Add() = i;
    }
    return sn;
}();

BOOST_AUTO_TEST_CASE(TestSendingSnapshotSetPendingSnapshot) {
    auto storage = std::make_shared<MemoryStorage>();
    auto sm = newTestRaft(1, { 1 }, 10, 1, storage);
    sm->restore(*testingSnap);

    sm->becomeCandidate();
    sm->becomeLeader();

    // force set the next of node 2, so that
    // node 2 needs a snapshot
    sm->prs[2]->Next = sm->raftLog->firstIndex();

    sm->Step(*make_message(2, 1, MsgAppResp, sm->prs[2]->Next - 1, 0, true));
    BOOST_REQUIRE_EQUAL(sm->prs[2]->PendingSnapshot, 11);
}

BOOST_AUTO_TEST_CASE(TestPendingSnapshotPauseReplication) {
    auto storage = std::make_shared<MemoryStorage>();
    auto sm = newTestRaft(1, { 1, 2 }, 10, 1, storage);
    sm->restore(*testingSnap);

    sm->becomeCandidate();
    sm->becomeLeader();

    sm->prs[2]->becomeSnapshot(11);

    sm->Step(*make_message(1, 1, MsgProp, 0, 0, false, { makeEntry(0,0,"somedata") }));
    auto msgs = sm->readMessages();
    BOOST_REQUIRE_EQUAL(msgs.size(), 0);
}

BOOST_AUTO_TEST_CASE(TestSnapshotFailure) {
    auto storage = std::make_shared<MemoryStorage>();
    auto sm = newTestRaft(1, { 1, 2 }, 10, 1, storage);
    sm->restore(*testingSnap);

    sm->becomeCandidate();
    sm->becomeLeader();

    sm->prs[2]->Next = 1;
    sm->prs[2]->becomeSnapshot(11);

    sm->Step(*make_message(2, 1, MsgSnapStatus, 0, 0, true));
    BOOST_REQUIRE_EQUAL(sm->prs[2]->PendingSnapshot, 0);
    BOOST_REQUIRE_EQUAL(sm->prs[2]->Next, 1);
    BOOST_REQUIRE_EQUAL(sm->prs[2]->Paused, true);
}

BOOST_AUTO_TEST_CASE(TestSnapshotSucceed) {
    auto storage = std::make_shared<MemoryStorage>();
    auto sm = newTestRaft(1, { 1, 2 }, 10, 1, storage);
    sm->restore(*testingSnap);

    sm->becomeCandidate();
    sm->becomeLeader();

    sm->prs[2]->Next = 1;
    sm->prs[2]->becomeSnapshot(11);

    sm->Step(*make_message(2, 1, MsgSnapStatus, 0, 0, false));
    BOOST_REQUIRE_EQUAL(sm->prs[2]->PendingSnapshot, 0);
    BOOST_REQUIRE_EQUAL(sm->prs[2]->Next, 12);
    BOOST_REQUIRE_EQUAL(sm->prs[2]->Paused, true);
}

BOOST_AUTO_TEST_CASE(TestSnapshotAbort) {
    auto storage = std::make_shared<MemoryStorage>();
    auto sm = newTestRaft(1, { 1, 2 }, 10, 1, storage);
    sm->restore(*testingSnap);

    sm->becomeCandidate();
    sm->becomeLeader();

    sm->prs[2]->Next = 1;
    sm->prs[2]->becomeSnapshot(11);

    // A successful msgAppResp that has a higher/equal index than the
    // pending snapshot should abort the pending snapshot.
    sm->Step(*make_message(2, 1, MsgAppResp, 11));
    BOOST_REQUIRE_EQUAL(sm->prs[2]->PendingSnapshot, 0);
    BOOST_REQUIRE_EQUAL(sm->prs[2]->Next, 12);
}
