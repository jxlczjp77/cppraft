#include <raft/rawnode.hpp>
#include <boost/throw_exception.hpp>

namespace raft {
    void RawNode::Init(Config &&config, const vector<Peer> &peers) {
        if (config.ID == 0) {
            BOOST_THROW_EXCEPTION(std::runtime_error("config.ID must not be zero"));
        }
        auto r = std::make_unique<Raft>();
        r->Init(std::move(config));
        auto lastIndex = config.Storage->LastIndex();
        if (lastIndex.err != OK) {
            abort(); // TODO(bdarnell)
        }
        // If the log is empty, this is a new RawNode (like StartNode); otherwise it's
        // restoring an existing RawNode (like RestartNode).
        // TODO(bdarnell): rethink RawNode initialization and whether the application needs
        // to be able to tell us when it expects the RawNode to exist.
        if (lastIndex.value == 0) {
            r->becomeFollower(1, None);
            vector<Entry> ents;
            ents.reserve(peers.size());
            for (size_t i = 0; i < peers.size(); i++) {
                auto &peer = peers[i];
                ConfChange cc;
                cc.set_type(ConfChangeAddNode);
                cc.set_nodeid(peer.ID);
                if (!peer.Context.empty()) cc.set_context(peer.Context);
                auto data = cc.SerializeAsString();
                Entry tmp;
                tmp.set_type(EntryConfChange);
                tmp.set_term(1);
                tmp.set_index(uint64_t(i + 1));
                tmp.set_data(data);
                ents.emplace_back(std::move(tmp));
            }
            r->raftLog->append(make_slice(ents));
            r->raftLog->committed = uint64_t(ents.size());
            for (auto &peer : peers) {
                r->addNode(peer.ID);
            }
        }

        // Set the initial hard and soft states after performing all initialization.
        prevSoftSt = r->softState();
        if (lastIndex.value == 0) {
            prevHardSt = HardState();
        } else {
            prevHardSt = r->hardState();
        }
        raft = std::move(r);
    }

    void RawNode::commitReady(raft::Ready &rd) {
        if (rd.SoftState) {
            prevSoftSt = *rd.SoftState;
        }

        if (!IsEmptyHardState(rd.HardState)) {
            prevHardSt = *rd.HardState;
        }

        // If entries were applied (or a snapshot), update our cursor for
        // the next Ready. Note that if the current HardState contains a
        // new Commit index, this does not mean that we're also applying
        // all of the new entries due to commit pagination by size.
        auto index = rd.appliedCursor();
        if (index > 0) {
            this->raft->raftLog->appliedTo(index);
        }

        if (!rd.Entries.empty()) {
            auto &e = rd.Entries[rd.Entries.size() - 1];
            this->raft->raftLog->stableTo(e.index(), e.term());
        }
        if (!IsEmptySnap(rd.Snapshot)) {
            this->raft->raftLog->stableSnapTo(rd.Snapshot->metadata().index());
        }
        if (!rd.ReadStates.empty()) {
            this->raft->readStates.clear();
        }
    }

    // Tick advances the internal logical clock by a single tick.
    void RawNode::Tick() {
        raft->tick();
    }

    // TickQuiesced advances the internal logical clock by a single tick without
    // performing any other state machine processing. It allows the caller to avoid
    // periodic heartbeats and elections when all of the peers in a Raft group are
    // known to be at the same state. Expected usage is to periodically invoke Tick
    // or TickQuiesced depending on whether the group is "active" or "quiesced".
    //
    // WARNING: Be very careful about using this method as it subverts the Raft
    // state machine. You should probably be using Tick instead.
    void RawNode::TickQuiesced() {
        raft->electionElapsed++;
    }

    // Campaign causes this RawNode to transition to candidate state.
    ErrorCode RawNode::Campaign() {
        Message msg;
        msg.set_type(MsgHup);
        return raft->Step(msg);
    }

    // Propose proposes data be appended to the raft log.
    ErrorCode RawNode::Propose(const string &data) {
        Message msg;
        msg.set_type(MsgProp);
        msg.set_from(raft->id);
        msg.mutable_entries()->Add()->set_data(data);
        return raft->Step(msg);
    }

    // ProposeConfChange proposes a config change.
    ErrorCode RawNode::ProposeConfChange(const ConfChange &cc) {
        auto data = cc.SerializeAsString();
        Message msg;
        msg.set_type(MsgProp);
        msg.set_from(raft->id);
        auto ent = msg.mutable_entries()->Add();
        ent->set_data(data);
        ent->set_type(EntryConfChange);
        return raft->Step(msg);
    }

    // ApplyConfChange applies a config change to the local node.
    ConfState RawNode::ApplyConfChange(const ConfChange &cc) {
        ConfState cs;
        if (cc.nodeid() != None) {
            switch (cc.type()) {
            case ConfChangeAddNode:
                raft->addNode(cc.nodeid());
                break;
            case ConfChangeAddLearnerNode:
                raft->addLearner(cc.nodeid());
                break;
            case ConfChangeRemoveNode:
                raft->removeNode(cc.nodeid());
                break;
            case ConfChangeUpdateNode:
                break;
            default:
                BOOST_THROW_EXCEPTION(std::runtime_error("unexpected conf type"));
            }
        }
        for (auto n : raft->nodes()) cs.mutable_nodes()->Add(n);
        for (auto n : raft->learnerNodes()) cs.mutable_learners()->Add(n);
        return cs;
    }

    // Step advances the state machine using the given message.
    ErrorCode RawNode::Step(Message &m) {
        // ignore unexpected local messages receiving over network
        if (IsLocalMsg(m.type())) {
            return ErrStepLocalMsg;
        }
        auto pr = raft->getProgress(m.from());
        if (pr || !IsResponseMsg(m.type())) {
            return raft->Step(m);
        }
        return ErrStepPeerNotFound;
    }

    // Ready returns the current point-in-time state of this RawNode.
    raft::Ready RawNode::Ready() {
        raft::Ready rd(this->raft.get(), prevSoftSt, prevHardSt);
        raft->reduceUncommittedSize(rd.CommittedEntries);
        return std::move(rd);
    }

    void RawNode::Ready(raft::Ready &rd) {
        rd.init(this->raft.get(), prevSoftSt, prevHardSt);
        raft->reduceUncommittedSize(rd.CommittedEntries);
    }

    // HasReady called when RawNode user need to check if any Ready pending.
    // Checking logic in this method should be consistent with Ready.containsUpdates().
    bool RawNode::HasReady() {
        if (raft->softState() != prevSoftSt) {
            return true;
        }
        auto hardSt = raft->hardState();
        if (!IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, prevHardSt)) {
            return true;
        }
        if (raft->raftLog->unstable.snapshot && !IsEmptySnap(*raft->raftLog->unstable.snapshot)) {
            return true;
        }
        if (!raft->msgs.empty() || !raft->raftLog->unstableEntries().empty() || raft->raftLog->hasNextEnts()) {
            return true;
        }
        if (!raft->readStates.empty()) {
            return true;
        }
        return false;
    }

    // Advance notifies the RawNode that the application has applied and saved progress in the
    // last Ready results.
    void RawNode::Advance(raft::Ready &rd) {
        commitReady(rd);
    }

    // Status returns the current status of the given group.
    raft::Status RawNode::Status() {
        return getStatus(raft.get());
    }

    // ReportUnreachable reports the given node is not reachable for the last send.
    void RawNode::ReportUnreachable(uint64_t id) {
        Message msg;
        msg.set_type(MsgUnreachable);
        msg.set_from(id);
        raft->Step(msg);
    }

    // ReportSnapshot reports the status of the sent snapshot.
    void RawNode::ReportSnapshot(uint64_t id, SnapshotStatus status) {
        bool rej = status == SnapshotFailure;
        Message msg;
        msg.set_type(MsgSnapStatus);
        msg.set_from(id);
        msg.set_reject(rej);
        raft->Step(msg);
    }

    // TransferLeader tries to transfer leadership to the given transferee.
    void RawNode::TransferLeader(uint64_t transferee) {
        Message msg;
        msg.set_type(MsgTransferLeader);
        msg.set_from(transferee);
        raft->Step(msg);
    }

    // ReadIndex requests a read state. The read state will be set in ready.
    // Read State has a read index. Once the application advances further than the read
    // index, any linearizable read requests issued before the read request can be
    // processed safely. The read state will have the same rctx attached.
    void RawNode::ReadIndex(const string &rctx) {
        Message msg;
        msg.set_type(MsgReadIndex);
        msg.mutable_entries()->Add()->set_data(rctx);
        raft->Step(msg);
    }

    bool RawNode::HasLeader() {
        return raft->lead != None;
    }
}
