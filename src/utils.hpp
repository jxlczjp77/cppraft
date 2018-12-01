#pragma once
#include <vector>
#include <raft/raft.pb.h>

namespace raft {
	void limitSize(std::vector <raftpb::Entry> &ents, uint64_t maxSize);
}
