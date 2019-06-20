#pragma once
#include <raft/raft.pb.h>
#include <raft/utils.hpp>

namespace raft {
	using namespace raftpb;
	using namespace std;
	struct Progress;
    class Raft;

    // SoftState provides state that is useful for logging and debugging.
    // The state is volatile and does not need to be persisted to the WAL.
    struct SoftState {
        uint64_t Lead; // must use atomic operations to access; keep 64-bit aligned.
        StateType RaftState;
        SoftState(uint64_t lead = 0, StateType state = StateFollower) : Lead(lead), RaftState(state) {}

        friend bool operator==(const SoftState &a, const SoftState &b) {
            return a.Lead == b.Lead && a.RaftState == b.RaftState;
        }
        friend bool operator!=(const SoftState &a, const SoftState &b) {
            return !(a == b);
        }
    };

	struct Status {
		uint64_t ID;

		HardState HardState;
		SoftState SoftState;

		uint64_t Applied;
		map<uint64_t, Progress*> Progress;

		uint64_t LeadTransferee;

		string ToJson();
		string ToString();
	};

	Status getStatus(Raft *r);
}
