#pragma once
#include <raft/raft.hpp>
#include <raft/entrys.hpp>
#include <boost/optional.hpp>

namespace raft
{
    using namespace raftpb;

    struct Peer {
        uint64_t ID;
        std::string Context;
    };

    enum SnapshotStatus {
        SnapshotFinish = 1,
        SnapshotFailure = 2,
    };

    // Ready encapsulates the entries and messages that are ready to read,
    // be saved to stable storage, committed or sent to other peers.
    // All fields in Ready are read-only.
    struct Ready {
        // The current volatile state of a Node.
        // SoftState will be nil if there is no update.
        // It is not required to consume or store SoftState.
        boost::optional<raft::SoftState> SoftState;

        // The current state of a Node to be saved to stable storage BEFORE
        // Messages are sent.
        // HardState will be equal to empty state if there is no update.
        boost::optional<raftpb::HardState> HardState;

        // ReadStates can be used for node to serve linearizable read requests locally
        // when its applied index is greater than the index in ReadState.
        // Note that the readState will be returned when raft receives msgReadIndex.
        // The returned is only valid for the request that requested to read.
        vector<raft::ReadState> ReadStates;

        // Entries specifies entries to be saved to stable storage BEFORE
        // Messages are sent.
        EntrySlice<EntryUnstableVec> Entries;

        // Snapshot specifies the snapshot to be saved to stable storage.
        boost::optional<raftpb::Snapshot> Snapshot;

        // CommittedEntries specifies entries to be committed to a
        // store/state-machine. These have previously been committed to stable
        // store.
        EntryRange CommittedEntries;

        // Messages specifies outbound messages to be sent AFTER Entries are
        // committed to stable storage.
        // If it contains a MsgSnap message, the application MUST report back to raft
        // when the snapshot has been received or has failed by calling ReportSnapshot.
        vector<MessagePtr> Messages;

        // MustSync indicates whether the HardState and Entries must be synchronously
        // written to disk or if an asynchronous write is permissible.
        bool MustSync;

        Ready(Raft *r, const raft::SoftState &prevSoftSt, const raft::HardState &prevHardSt);
        Ready(Ready &&v);

        Ready &operator= (const Ready &v);

        bool containsUpdates();
        uint64_t appliedCursor();
    };
} // namespace raft
