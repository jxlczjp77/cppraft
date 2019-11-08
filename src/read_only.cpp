#include <raft/read_only.hpp>
#include <boost/throw_exception.hpp>

namespace raft {
    ReadOnly::ReadOnly(ReadOnlyOption o) : option(o) {

    }

    // addRequest adds a read only reuqest into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    void ReadOnly::addRequest(uint64_t index, const Message &m) {
        auto ctx = string(m.entries(0).data());
        if (pendingReadIndex.find(ctx) != pendingReadIndex.end()) {
            return;
        }
        pendingReadIndex[ctx] = std::make_unique<readIndexStatus>(index, m);
        readIndexQueue.push_back(ctx);
    }

    // recvAck notifies the readonly struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the read only request
    // context.
    int ReadOnly::recvAck(const Message &msg) {
        auto iter = pendingReadIndex.find(msg.context());
        if (iter == pendingReadIndex.end()) {
            return 0;
        }

        readIndexStatusPtr &rs = iter->second;
        rs->acks[msg.from()] = true;
        return int(rs->acks.size() + 1);
    }

    // advance advances the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given `m`.
    void ReadOnly::advance(const Message& m, vector<readIndexStatusPtr> &rss) {
        int i = 0;
        bool found;

        auto &ctx = m.context();
        for (auto &okctx : readIndexQueue) {
            i++;
            auto it = pendingReadIndex.find(okctx);
            if (it == pendingReadIndex.end()) {
                BOOST_THROW_EXCEPTION(std::runtime_error("cannot find corresponding read state from pending map"));
            }
            rss.emplace_back(std::move(it->second));
            if (okctx == ctx) {
                found = true;
                break;
            }
        }

        if (found) {
            readIndexQueue.erase(readIndexQueue.begin(), readIndexQueue.begin() + i);
            for (auto &rs : rss) {
                pendingReadIndex.erase(rs->req.entries(0).data());
            }
        }
    }

    // lastPendingRequestCtx returns the context of the last pending read only
    // request in readonly struct.
    string ReadOnly::lastPendingRequestCtx() {
        if (readIndexQueue.empty()) {
            return string();
        }
        return readIndexQueue[readIndexQueue.size() - 1];
    }
}
