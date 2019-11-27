#include <raft/node.hpp>

namespace raft {
    // MustSync returns true if the hard state and count of Raft entries indicate
    // that a synchronous write to persistent storage is required.
    bool MustSync_(const HardState &st, const HardState &prevst, size_t entsnum) {
        // Persistent state on all servers:
        // (Updated on stable storage before responding to RPCs)
        // currentTerm
        // votedFor
        // log entries[]
        return entsnum != 0 || st.vote() != prevst.vote() || st.term() != prevst.term();
    }

    Ready::Ready(Raft *r, const raft::SoftState &prevSoftSt, const raft::HardState &prevHardSt) {
        init(r, prevSoftSt, prevHardSt);
    }

    void Ready::init(Raft *r, const raft::SoftState &prevSoftSt, const raft::HardState &prevHardSt) {
        Entries = r->raftLog->unstableEntries();
        CommittedEntries = r->raftLog->nextEnts();
        Messages = std::move(r->msgs);
        auto softSt = r->softState();
        if (softSt != prevSoftSt) {
            this->SoftState = softSt;
        } else {
            this->SoftState.reset();
        }
        auto hardSt = r->hardState();
        if (!isHardStateEqual(hardSt, prevHardSt)) {
            this->HardState = hardSt;
        } else {
            this->HardState.reset();
        }

        if (r->raftLog->unstable.snapshot) {
            this->Snapshot = *r->raftLog->unstable.snapshot;
        } else {
            this->Snapshot.reset();
        }

        if (!r->readStates.empty()) {
            this->ReadStates = r->readStates;
        }
        this->MustSync = MustSync_(hardSt, prevHardSt, Entries.size());
    }

    bool Ready::containsUpdates() {
        return !IsEmptyHardState(this->HardState) ||
            !IsEmptySnap(this->Snapshot) || Entries.size() > 0 ||
            CommittedEntries.size() > 0 || Messages.size() > 0 || ReadStates.size() != 0;
    }

    // appliedCursor extracts from the Ready the highest index the client has
    // applied (once the Ready is confirmed via Advance). If no information is
    // contained in the Ready, returns zero.
    uint64_t Ready::appliedCursor() {
        size_t n = CommittedEntries.size();
        if (n > 0) {
            return CommittedEntries[n - 1].index();
        }
        if (this->Snapshot) {
            auto index = this->Snapshot->metadata().index();
            if (index > 0) {
                return index;
            }
        }
        return 0;
    }
}
