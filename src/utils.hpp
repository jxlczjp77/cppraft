#pragma once
#include <vector>
#include <raft/raft.pb.h>
#include <raft/entrys.hpp>

namespace raft {
	bool isHardStateEqual(const HardState &a, const HardState &b);
	bool IsEmptyHardState(const boost::optional<HardState> &st);
	bool IsEmptySnap(const boost::optional<Snapshot> &sp);
	void limitSize(IEntrySlice &ents, uint64_t maxSize);
	size_t PayloadSize(const Entry &e);
	bool IsLocalMsg(MessageType msgt);
	bool IsResponseMsg(MessageType msgt);
	MessageType voteRespMsgType(MessageType msgt);
}
