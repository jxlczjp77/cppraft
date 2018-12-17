#pragma once
#include <vector>
#include <raft/raft.pb.h>

namespace raft {
	bool isHardStateEqual(const HardState &a, const HardState &b);
	bool IsEmptyHardState(const HardState &st);
	bool IsEmptySnap(const Snapshot &sp);
	void limitSize(std::vector <raftpb::Entry> &ents, uint64_t maxSize);
	size_t PayloadSize(const Entry &e);
	bool IsLocalMsg(MessageType msgt);
	bool IsResponseMsg(MessageType msgt);
	MessageType voteRespMsgType(MessageType msgt);
}
