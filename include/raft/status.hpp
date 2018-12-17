#pragma once
#include <raft/raft.pb.h>
#include <raft/raft.hpp>

namespace raft {
	using namespace raftpb;
	using namespace std;
	struct Progress;

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
