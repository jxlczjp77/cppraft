#pragma once
#include <vector>
#include <raft/raft.pb.h>
#include <raft/entrys.hpp>
#include <boost/optional.hpp>

namespace raft {
    const uint64_t None = 0;
    const uint64_t noLimit = std::numeric_limits<int64_t>().max();

    enum ErrorCode {
        OK = 0,
        ErrCompacted,
        ErrSnapOutOfDate,
        ErrUnavailable,
        ErrSnapshotTemporarilyUnavailable,
        ErrSeriaFail,
        ErrAppendOutOfData,
        ErrProposalDropped,
        ErrStepLocalMsg,
        ErrStepPeerNotFound,
        ErrFalse
    };

    const char *error_string(ErrorCode c);

    template<class Value> struct Result {
        ErrorCode err;
        Value value;

        Result(Value &&v = Value(), ErrorCode e = OK) : value(std::move(v)), err(e) {}
        Result(ErrorCode e) : err(e) {}
        bool Ok() const { return err == OK; }
    };

    enum StateType {
        StateFollower,
        StateCandidate,
        StateLeader,
        StatePreCandidate,
        numStates,
    };

	bool isHardStateEqual(const HardState &a, const HardState &b);
	bool IsEmptyHardState(const boost::optional<HardState> &st);
	bool IsEmptySnap(const boost::optional<Snapshot> &sp);
	void limitSize(IEntrySlice &ents, uint64_t maxSize);
	size_t PayloadSize(const Entry &e);
	bool IsLocalMsg(MessageType msgt);
	bool IsResponseMsg(MessageType msgt);
	MessageType voteRespMsgType(MessageType msgt);
}
