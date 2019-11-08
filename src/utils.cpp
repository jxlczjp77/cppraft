#include <raft/raft.hpp>
#include <boost/throw_exception.hpp>
#include <boost/format.hpp>

using namespace std;
using namespace raftpb;

namespace raft {
    const char *error_string(ErrorCode c) {
        return "";
    }

    void limitSize(IEntrySlice &ents, uint64_t maxSize) {
        if (!ents.empty()) {
            size_t size = ents[0].ByteSize();
            size_t limit;
            for (limit = 1; limit < ents.size(); limit++) {
                size += ents[limit].ByteSize();
                if (uint64_t(size) > maxSize) {
                    break;
                }
            }
            ents.truncate(limit);
        }
    }

    bool isHardStateEqual(const HardState &a, const HardState &b) {
        return a.term() == b.term() && a.vote() == b.vote() && a.commit() == b.commit();
    }

    const HardState emptyState;

    // IsEmptyHardState returns true if the given HardState is empty.
    bool IsEmptyHardState(const boost::optional<HardState> &st) {
        return !st.has_value() || isHardStateEqual(*st, emptyState);
    }

    // IsEmptySnap returns true if the given Snapshot is empty.
    bool IsEmptySnap(const boost::optional<Snapshot> &sp) {
        return !sp.has_value() || sp->metadata().index() == 0;
    }

    // PayloadSize is the size of the payload of this Entry. Notably, it does not
    // depend on its Index or Term.
    size_t PayloadSize(const Entry &e) {
        return e.data().length();
    }


    bool IsLocalMsg(MessageType msgt) {
        return msgt == MsgHup || msgt == MsgBeat || msgt == MsgUnreachable ||
            msgt == MsgSnapStatus || msgt == MsgCheckQuorum;
    }

    bool IsResponseMsg(MessageType msgt) {
        return msgt == MsgAppResp || msgt == MsgVoteResp || msgt == MsgHeartbeatResp || msgt == MsgUnreachable || msgt == MsgPreVoteResp;
    }

    // voteResponseType maps vote and prevote message types to their corresponding responses.
    MessageType voteRespMsgType(MessageType msgt) {
        switch (msgt) {
        case MsgVote:
            return MsgVoteResp;
        case MsgPreVote:
            return MsgPreVoteResp;
        default:
            BOOST_THROW_EXCEPTION(std::runtime_error((boost::format("not a vote message: %s") % msgt).str()));
        }
    }

}
