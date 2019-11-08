#pragma once
#include <vector>
#include <map>
#include <raft/raft.pb.h>

namespace raft {
    using namespace std;
    using namespace raftpb;

    // ReadState provides state for read only query.
    // It's caller's responsibility to call ReadIndex first before getting
    // this state from ready, it's also caller's duty to differentiate if this
    // state is what it requests through RequestCtx, eg. given a unique id as
    // RequestCtx
    struct ReadState {
        uint64_t Index;
        string RequestCtx;
    };

    struct readIndexStatus {
        uint64_t index;
        Message req;
        map<uint64_t, bool> acks;
        readIndexStatus(uint64_t idx, const Message &msg)
            : index(idx), req(msg) {
        }
    };
    typedef std::unique_ptr<readIndexStatus> readIndexStatusPtr;

    enum ReadOnlyOption {
        // ReadOnlySafe guarantees the linearizability of the read only request by
        // communicating with the quorum. It is the default and suggested option.
        ReadOnlySafe,
        // ReadOnlyLeaseBased ensures linearizability of the read only request by
        // relying on the leader lease. It can be affected by clock drift.
        // If the clock drift is unbounded, leader might keep the lease longer than it
        // should (clock can move backward/pause without any bound). ReadIndex is not safe
        // in that case.
        ReadOnlyLeaseBased
    };

    struct ReadOnly {
        ReadOnlyOption option;
        map<string, readIndexStatusPtr> pendingReadIndex;
        vector<string> readIndexQueue;

        ReadOnly(ReadOnlyOption option);
        void addRequest(uint64_t index, const Message &m);
        int recvAck(const Message& msg);
        void advance(const Message& m, vector<readIndexStatusPtr> &rss);
        string lastPendingRequestCtx();
    };
}
