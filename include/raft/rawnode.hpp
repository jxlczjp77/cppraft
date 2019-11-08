#pragma once
#include <raft/node.hpp>

namespace raft {
    using namespace raftpb;

    // RawNode is a thread-unsafe Node.
    // The methods of this struct correspond to the methods of Node and are described
    // more fully there.
    struct RawNode {
        std::unique_ptr<Raft> raft;
        SoftState prevSoftSt;
        HardState prevHardSt;

        void Init(Config &&config, const vector<Peer> &peers);
        void commitReady(raft::Ready &rd);
        void Tick();
        void TickQuiesced();
        ErrorCode Campaign();
        ErrorCode Propose(const string &data);
        ErrorCode ProposeConfChange(const ConfChange &cc);
        ConfState ApplyConfChange(const ConfChange &cc);
        ErrorCode Step(Message &m);
        raft::Ready Ready();
        bool HasReady();
        void Advance(raft::Ready &rd);
        raft::Status Status();
        void ReportUnreachable(uint64_t id);
        void ReportSnapshot(uint64_t id, SnapshotStatus status);
        void TransferLeader(uint64_t transferee);
        void ReadIndex(const string &rctx);
    };
}
