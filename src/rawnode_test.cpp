#include <boost/test/unit_test.hpp>
#include "test_common.hpp"
#include <raft/rawnode.hpp>
#include "utils.hpp"
using namespace raft;
using namespace raftpb;
Config newTestConfig(uint64_t id, vector<uint64_t> &&peers, int election, int heartbeat, StoragePtr storage);

// TestRawNodeStep ensures that RawNode.Step ignore local message.
BOOST_AUTO_TEST_CASE(TestRawNodeStep) {
    for (int i = MessageType_MIN; i <= MessageType_MAX; i++) {
        auto msgn = MessageType_Name((MessageType)i);
        auto s = std::make_shared<MemoryStorage>();
        RawNode rawNode;
        rawNode.Init(newTestConfig(1, {}, 10, 1, s), { {1} });
        auto msgt = MessageType(i);
        Message msg;
        msg.set_type(msgt);
        auto err = rawNode.Step(msg);
        // LocalMsg should be ignored.
        if (IsLocalMsg(msgt)) {
            BOOST_REQUIRE_EQUAL(err, ErrStepLocalMsg);
        }
    }
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
BOOST_AUTO_TEST_CASE(TestRawNodeProposeAndConfChange) {
    auto s = std::make_shared<MemoryStorage>();
    RawNode rawNode;
    rawNode.Init(newTestConfig(1, {}, 10, 1, s), { {1} });
    auto rd = rawNode.Ready();
    s->Append(rd.Entries);
    rawNode.Advance(rd);

    auto d = rawNode.Ready();
    BOOST_REQUIRE_EQUAL(d.MustSync, false);
    BOOST_REQUIRE_EQUAL(IsEmptyHardState(d.HardState), true);
    BOOST_REQUIRE_EQUAL(d.Entries.empty(), true);

    rawNode.Campaign();
    bool proposed = false;
    Result<uint64_t> lastIndex;
    string ccdata;
    for (;;) {
        rd = rawNode.Ready();
        s->Append(rd.Entries);
        // Once we are the leader, propose a command and a ConfChange.
        if (!proposed && rd.SoftState->Lead == rawNode.raft->id) {
            rawNode.Propose("somedata");

            ConfChange cc;
            cc.set_type(ConfChangeAddNode);
            cc.set_nodeid(1);
            ccdata = cc.SerializeAsString();
            rawNode.ProposeConfChange(cc);

            proposed = true;
        }
        rawNode.Advance(rd);

        // Exit when we have four entries: one ConfChange, one no-op for the election,
        // our proposed command and proposed ConfChange.
        lastIndex = std::move(s->LastIndex());
        BOOST_REQUIRE_EQUAL(lastIndex.err, OK);
        if (lastIndex.value >= 4) {
            break;
        }
    }

    auto entries = s->Entries(lastIndex.value - 1, lastIndex.value + 1, noLimit);
    BOOST_REQUIRE_EQUAL(entries.err, OK);
    BOOST_REQUIRE_EQUAL(entries.value->size(), 2);
    BOOST_REQUIRE_EQUAL(entries.value->at(0).data(), "somedata");
    BOOST_REQUIRE_EQUAL(entries.value->at(1).type(), EntryConfChange);
    BOOST_REQUIRE_EQUAL(entries.value->at(1).data(), ccdata);
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
BOOST_AUTO_TEST_CASE(TestRawNodeProposeAddDuplicateNode) {
    auto s = std::make_shared<MemoryStorage>();
    RawNode rawNode;
    rawNode.Init(newTestConfig(1, {}, 10, 1, s), { {1} });
    auto rd = rawNode.Ready();
    s->Append(rd.Entries);
    rawNode.Advance(rd);

    rawNode.Campaign();
    for (;;) {
        rd = rawNode.Ready();
        s->Append(rd.Entries);
        if (rd.SoftState->Lead == rawNode.raft->id) {
            rawNode.Advance(rd);
            break;
        }
        rawNode.Advance(rd);
    }

    auto proposeConfChangeAndApply = [&](const ConfChange &cc) {
        rawNode.ProposeConfChange(cc);
        auto rd = rawNode.Ready();
        s->Append(rd.Entries);
        for (auto &entry : rd.CommittedEntries) {
            if (entry.type() == EntryConfChange) {
                ConfChange cc;
                cc.ParseFromString(entry.data());
                rawNode.ApplyConfChange(cc);
            }
        }
        rawNode.Advance(rd);
    };

    ConfChange cc1;
    cc1.set_type(ConfChangeAddNode);
    cc1.set_nodeid(1);
    auto ccdata1 = cc1.SerializeAsString();
    proposeConfChangeAndApply(cc1);

    // try to add the same node again
    proposeConfChangeAndApply(cc1);

    // the new node join should be ok
    ConfChange cc2;
    cc2.set_type(ConfChangeAddNode);
    cc2.set_nodeid(2);
    auto ccdata2 = cc2.SerializeAsString();
    proposeConfChangeAndApply(cc2);

    auto lastIndex = s->LastIndex();
    BOOST_REQUIRE_EQUAL(lastIndex.err, OK);

    // the last three entries should be: ConfChange cc1, cc1, cc2
    auto entries = s->Entries(lastIndex.value - 2, lastIndex.value + 1, noLimit);
    BOOST_REQUIRE_EQUAL(entries.err, OK);
    BOOST_REQUIRE_EQUAL(entries.value->size(), 3);
    BOOST_REQUIRE_EQUAL(entries.value->at(0).data(), ccdata1);
    BOOST_REQUIRE_EQUAL(entries.value->at(2).data(), ccdata2);
}

template<class RD1, class RD2>
void equal_readstate(const RD1 &rd1, const RD2 &rd2) {
    euqal_vec(rd1.Entries, rd2.Entries, [](const Entry &a, const Entry &b) {
        BOOST_REQUIRE_EQUAL(a.index(), b.index());
        BOOST_REQUIRE_EQUAL(a.term(), b.term());
        BOOST_REQUIRE_EQUAL(a.data(), b.data());
        BOOST_REQUIRE_EQUAL(a.type(), b.type());
        });
    euqal_vec(rd1.CommittedEntries, rd2.CommittedEntries, [](const Entry &a, const Entry &b) {
        BOOST_REQUIRE_EQUAL(a.index(), b.index());
        BOOST_REQUIRE_EQUAL(a.term(), b.term());
        BOOST_REQUIRE_EQUAL(a.data(), b.data());
        BOOST_REQUIRE_EQUAL(a.type(), b.type());
        });
    BOOST_REQUIRE_EQUAL(rd1.HardState.has_value(), rd2.HardState.has_value());
    if (rd1.HardState.has_value()) {
        BOOST_REQUIRE_EQUAL(rd1.HardState->term(), rd2.HardState->term());
        BOOST_REQUIRE_EQUAL(rd1.HardState->vote(), rd2.HardState->vote());
        BOOST_REQUIRE_EQUAL(rd1.HardState->commit(), rd2.HardState->commit());
    }
    BOOST_REQUIRE_EQUAL(rd1.MustSync, rd2.MustSync);
}

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
BOOST_AUTO_TEST_CASE(TestRawNodeReadIndex) {
    vector<Message> msgs;
    auto appendStep = [&msgs](Raft *r, Message &m) {
        msgs.emplace_back(m);
        return OK;
    };
    vector<ReadState> wrs = { {1, "somedata"} };

    auto s = std::make_shared<MemoryStorage>();
    auto c = newTestConfig(1, {}, 10, 1, s);
    RawNode rawNode;
    rawNode.Init(std::move(c), { {1} });
    rawNode.raft->readStates = wrs;
    // ensure the ReadStates can be read out
    auto hasReady = rawNode.HasReady();
    BOOST_REQUIRE_EQUAL(hasReady, true);
    auto rd = rawNode.Ready();
    euqal_vec(rd.ReadStates, wrs, [](const ReadState &a, const ReadState &b) {
        BOOST_REQUIRE_EQUAL(a.Index, b.Index);
        });
    s->Append(rd.Entries);
    rawNode.Advance(rd);
    // ensure raft.readStates is reset after advance
    BOOST_REQUIRE_EQUAL(rawNode.raft->readStates.empty(), true);

    auto wrequestCtx = "somedata2";
    rawNode.Campaign();
    for (;;) {
        rd = rawNode.Ready();
        s->Append(rd.Entries);

        if (rd.SoftState->Lead == rawNode.raft->id) {
            rawNode.Advance(rd);

            // Once we are the leader, issue a ReadIndex request
            rawNode.raft->step = appendStep;
            rawNode.ReadIndex(wrequestCtx);
            break;
        }
        rawNode.Advance(rd);
    }
    // ensure that MsgReadIndex message is sent to the underlying raft
    BOOST_REQUIRE_EQUAL(msgs.size(), 1);
    BOOST_REQUIRE_EQUAL(msgs[0].type(), MsgReadIndex);
    BOOST_REQUIRE_EQUAL(msgs[0].entries(0).data(), wrequestCtx);
}

// TestBlockProposal from node_test.go has no equivalent in rawNode because there is
// no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because
// it reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
BOOST_AUTO_TEST_CASE(TestRawNodeStart) {
    ConfChange cc;
    cc.set_type(ConfChangeAddNode);
    cc.set_nodeid(1);
    auto ccdata = cc.SerializeAsString();
    auto makeHardState = [](uint64_t term, uint64_t commit, uint64_t vote) {
        HardState hs;
        hs.set_term(term);
        hs.set_commit(commit);
        hs.set_vote(vote);
        return hs;
    };
    Entry ent1 = makeEntry(1, 1, string(ccdata), EntryConfChange);
    Entry ent2 = makeEntry(3, 2, "foo");
    struct {
        boost::optional<raftpb::HardState> HardState;
        EntryVec Entries;
        EntryVec CommittedEntries;
        bool MustSync;
    } wants[] = {
        {makeHardState(1, 1, 0),EntryVec{ent1},EntryVec{ent1},true},
        {makeHardState(2, 3, 1),EntryVec{ent2},EntryVec{ent2},true},
    };

    auto storage = std::make_shared<MemoryStorage>();
    RawNode rawNode;
    rawNode.Init(newTestConfig(1, {}, 10, 1, storage), { {1} });
    auto rd = rawNode.Ready();

    equal_readstate(rd, wants[0]);
    storage->Append(rd.Entries);
    rawNode.Advance(rd);

    rawNode.Campaign();
    rd = rawNode.Ready();
    storage->Append(rd.Entries);
    rawNode.Advance(rd);

    rawNode.Propose("foo");
    rd = rawNode.Ready();
    equal_readstate(rd, wants[1]);
    storage->Append(rd.Entries);
    rawNode.Advance(rd);
    BOOST_REQUIRE_EQUAL(rawNode.HasReady(), false);
}

BOOST_AUTO_TEST_CASE(TestRawNodeRestart) {
    vector<Entry> entries = {
        makeEntry(1, 1),
        makeEntry(2, 1, "foo"),
    };
    HardState st;
    st.set_term(1);
    st.set_commit(1);

    struct {
        boost::optional<raftpb::HardState> HardState;
        EntryVec Entries;
        EntrySlice<EntryVec> CommittedEntries;
        bool MustSync;
    } want[] = { {
        boost::optional<raftpb::HardState>(),
        {},
        // commit up to commit index in st
        make_slice(entries, 0, st.commit()),
        false,
    } };

    auto storage = std::make_shared<MemoryStorage>();
    storage->SetHardState(st);
    storage->Append(entries);
    RawNode rawNode;
    rawNode.Init(newTestConfig(1, {}, 10, 1, storage), {});
    auto rd = rawNode.Ready();
    equal_readstate(rd, want[0]);
    rawNode.Advance(rd);
    BOOST_REQUIRE_EQUAL(rawNode.HasReady(), false);
}

BOOST_AUTO_TEST_CASE(TestRawNodeRestartFromSnapshot) {
    Snapshot snap;
    auto meta = snap.mutable_metadata();
    meta->set_index(2);
    meta->set_term(1);
    for (uint64_t i : { 1, 2 }) {
        meta->mutable_conf_state()->mutable_nodes()->Add(i);
    }
    vector<Entry> entries = {
        makeEntry(3, 1, "foo"),
    };
    HardState st;
    st.set_term(1);
    st.set_commit(3);

    struct {
        boost::optional<raftpb::HardState> HardState;
        EntryVec Entries;
        EntryVec CommittedEntries;
        bool MustSync;
    } want[] = { {
        boost::optional<raftpb::HardState>(),
        {},
        // commit up to commit index in st
        entries,
        false,
    } };

    auto s = std::make_shared<MemoryStorage>();
    s->SetHardState(st);
    s->ApplySnapshot(snap);
    s->Append(entries);
    RawNode rawNode;
    rawNode.Init(newTestConfig(1, {}, 10, 1, s), {});
    auto rd = rawNode.Ready();
    equal_readstate(rd, want[0]);
    rawNode.Advance(rd);
    BOOST_REQUIRE_EQUAL(rawNode.HasReady(), false);
}

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there is
// no dependency check between Ready() and Advance()

BOOST_AUTO_TEST_CASE(TestRawNodeStatus) {
    auto storage = std::make_shared<MemoryStorage>();
    RawNode rawNode;
    rawNode.Init(newTestConfig(1, {}, 10, 1, storage), { {1} });
    auto status = rawNode.Status();
    /*if status == nil {
        t.Errorf("expected status struct, got nil")
    }*/
}

class ignoreSizeHintMemStorage : public MemoryStorage {
    virtual Result<IEntrySlicePtr> Entries(uint64_t lo, uint64_t hi, uint64_t max_size) {
        return MemoryStorage::Entries(lo, hi, std::numeric_limits<uint64_t>().max());
    }
};

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
// - node learns that index 11 is committed
// - nextEnts returns index 1..10 in CommittedEntries (but index 10 already
//   exceeds maxBytes), which isn't noticed internally by Raft
// - Commit index gets bumped to 10
// - the node persists the HardState, but crashes before applying the entries
// - upon restart, the storage returns the same entries, but `slice` takes a
//   different code path and removes the last entry.
// - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//   its internal applied index cursor to 10 (when it should be 9)
// - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//    write.
BOOST_AUTO_TEST_CASE(TestRawNodeCommitPaginationAfterRestart) {
    auto s = std::make_shared<ignoreSizeHintMemStorage>();
    HardState persistedHardState;
    persistedHardState.set_term(1);
    persistedHardState.set_commit(10);
    persistedHardState.set_vote(1);

    s->hard_state = persistedHardState;
    uint64_t size = 0;
    for (uint64_t i = 0; i < 10; i++) {
        Entry ent = makeEntry(i + 1, 1, "a", EntryNormal);
        s->entries.push_back(ent);
        size += uint64_t(ent.ByteSize());
    }

    auto cfg = newTestConfig(1, { 1 }, 10, 1, s);
    // Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
    // not be included in the initial rd.CommittedEntries. However, our storage will ignore
    // this and *will* return it (which is how the Commit index ended up being 10 initially).
    cfg.MaxSizePerMsg = size - uint64_t(s->entries[s->entries.size() - 1].ByteSize()) - 1;
    s->entries.push_back(makeEntry(11, 1, "boom", EntryNormal));

    RawNode rawNode;
    rawNode.Init(std::move(cfg), { {1} });

    for (uint64_t highestApplied = 0; highestApplied != 11;) {
        auto rd = rawNode.Ready();
        auto n = rd.CommittedEntries.size();
        BOOST_REQUIRE_NE(n, 0);
        auto next = rd.CommittedEntries[0].index();
        BOOST_REQUIRE_EQUAL(highestApplied != 0 && highestApplied + 1 != next, false);
        highestApplied = rd.CommittedEntries[n - 1].index();
        rawNode.Advance(rd);
        auto msg = make_message(1, 1, MsgHeartbeat, 0, 1, false, {}, 0, 11);
        rawNode.Step(*msg);
    }
}

// TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the MaxUncommittedEntriesSize configuration.
BOOST_AUTO_TEST_CASE(TestRawNodeBoundedLogGrowthWithPartition) {
    StackLogLevel d(DefaultLogger::instance(), LogLevel::off);
    const uint64_t maxEntries = 16;
    auto data = "testdata";
    auto testEntry = makeEntry(0, 0, data);
    uint64_t maxEntrySize = maxEntries * PayloadSize(testEntry);

    auto s = std::make_shared<MemoryStorage>();
    auto cfg = newTestConfig(1, { 1 }, 10, 1, s);
    cfg.MaxUncommittedEntriesSize = maxEntrySize;
    RawNode rawNode;
    rawNode.Init(std::move(cfg), { {1} });
    auto rd = rawNode.Ready();
    s->Append(rd.Entries);
    rawNode.Advance(rd);

    // Become the leader.
    rawNode.Campaign();
    for (;;) {
        rd = rawNode.Ready();
        s->Append(rd.Entries);
        if (rd.SoftState->Lead == rawNode.raft->id) {
            rawNode.Advance(rd);
            break;
        }
        rawNode.Advance(rd);
    }

    // Simulate a network partition while we make our proposals by never
    // committing anything. These proposals should not cause the leader's
    // log to grow indefinitely.
    for (int i = 0; i < 1024; i++) {
        rawNode.Propose(data);
    }

    // Check the size of leader's uncommitted log tail. It should not exceed the
    // MaxUncommittedEntriesSize limit.
    auto checkUncommitted = [&rawNode](uint64_t exp) {
        auto a = rawNode.raft->uncommittedSize;
        BOOST_REQUIRE_EQUAL(a, exp);
    };
    checkUncommitted(maxEntrySize);

    // Recover from the partition. The uncommitted tail of the Raft log should
    // disappear as entries are committed.
    rd = rawNode.Ready();
    BOOST_REQUIRE_EQUAL(rd.CommittedEntries.size(), maxEntries);
    s->Append(rd.Entries);
    rawNode.Advance(rd);
    checkUncommitted(0);
}
