#include <Raft/Raft.hpp>
#include <boost/throw_exception.hpp>
#include <boost/format.hpp>
#include <boost/random.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

namespace raft {
	CampaignType campaignPreElection = "CampaignPreElection";
	CampaignType campaignElection = "CampaignElection";
	CampaignType campaignTransfer = "CampaignTransfer";

	MessagePtr make_message(uint64_t to, MessageType type = MessageType(0), uint64_t term = 0, bool reject = false) {
		MessagePtr msg = make_unique<Message>();
		msg->set_to(to);
		msg->set_type(type);
		msg->set_term(term);
		msg->set_reject(reject);
		return msg;
	}

	int numOfPendingConf(const EntryRange &ents) {
		int n = 0;
		for (auto &ent : ents) {
			if (ent.type() == EntryConfChange) {
				n++;
			}
		}
		return n;
	}

	string entryString(const Entry &entry) {
		return (boost::format("term:%1%, index : %2%, type : %3%, data: %4%") % entry.term() % entry.index() % entry.type() % entry.data()).str();
	}

	Config::Config() {
		ID = 0;
		ElectionTick = 0;
		HeartbeatTick = 0;
		Storage = nullptr;
		Applied = 0;
		MaxSizePerMsg = 0;
		MaxCommittedSizePerReady = 0;
		MaxUncommittedEntriesSize = 0;
		MaxInflightMsgs = 0;
		CheckQuorum = false;
		PreVote = false;
		ReadOnlyOption = ReadOnlySafe;
		Logger = nullptr;
		DisableProposalForwarding = false;
	}

	string Config::validate() {
		if (ID == None) {
			return "cannot use none as id";
		}

		if (HeartbeatTick <= 0) {
			return "heartbeat tick must be greater than 0";
		}

		if (ElectionTick <= HeartbeatTick) {
			return "election tick must be greater than heartbeat tick";
		}

		if (!Storage) {
			return "storage cannot be nil";
		}

		if (MaxUncommittedEntriesSize == 0) {
			MaxUncommittedEntriesSize = noLimit;
		}

		// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
		// previously the same parameter.
		if (MaxCommittedSizePerReady == 0) {
			MaxCommittedSizePerReady = MaxSizePerMsg;
		}

		if (MaxInflightMsgs <= 0) {
			return "max inflight messages must be greater than 0";
		}

		if (!Logger) {
			Logger = &DefaultLogger::instance();
		}

		if (ReadOnlyOption == ReadOnlyLeaseBased && !CheckQuorum) {
			return "CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased";
		}
		return string();
	}

	void Raft::Init(Config &&c) {
		string configErr = c.validate();
		if (!configErr.empty()) {
			BOOST_THROW_EXCEPTION(std::runtime_error(configErr));
		}
		auto raftlog = std::make_unique<raft_log>(c.Storage, c.Logger, c.MaxCommittedSizePerReady);
		HardState hs;
		ConfState cs;
		ErrorCode err = c.Storage->InitialState(hs, cs);
		if (err != OK) {
			abort(); // TODO(bdarnell)
		}
		auto peers = c.peers;
		auto learners = c.learners;
		if (cs.nodes_size() > 0 || cs.learners_size() > 0) {
			if (peers.size() > 0 || learners.size() > 0) {
				// TODO(bdarnell): the peers argument is always nil except in
				// tests; the argument should be removed and these tests should be
				// updated to specify their nodes through a snapshot.
				BOOST_THROW_EXCEPTION(std::runtime_error("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)"));
			}
			peers.clear();
			learners.clear();
			std::copy(cs.nodes().begin(), cs.nodes().end(), std::back_inserter(peers));
			std::copy(cs.learners().begin(), cs.learners().end(), std::back_inserter(learners));
		}
		id = c.ID;
		lead = None;
		isLearner = false;
		raftLog = std::move(raftlog);
		maxMsgSize = c.MaxSizePerMsg;
		maxInflight = c.MaxInflightMsgs;
		maxUncommittedSize = c.MaxUncommittedEntriesSize;
		electionTimeout = c.ElectionTick;
		heartbeatTimeout = c.HeartbeatTick;
		logger = c.Logger;
		checkQuorum = c.CheckQuorum;
		preVote = c.PreVote;
		readOnly = std::make_unique<ReadOnly>(c.ReadOnlyOption);
		disableProposalForwarding = c.DisableProposalForwarding;
		Term = 0;
		Vote = 0;
		for (auto p : peers) {
			auto progress = std::make_unique<Progress>();
			progress->Next = 1;
			progress->ins = std::make_unique<inflights>(maxInflight);
			prs[p] = std::move(progress);
		}
		for (auto p : learners) {
			if (prs.find(p) != prs.end()) {
				BOOST_THROW_EXCEPTION(std::runtime_error((boost::format("node %1% is in both learner and peer list") % p).str()));
			}
			auto progress = std::make_unique<Progress>();
			progress->Next = 1;
			progress->ins = std::make_unique<inflights>(maxInflight);
			progress->IsLearner = true;
			learnerPrs[p] = std::move(progress);
			if (id == p) {
				isLearner = true;
			}
		}

		if (!isHardStateEqual(hs, HardState{})) {
			loadState(hs);
		}
		if (c.Applied > 0) {
			raftLog->appliedTo(c.Applied);
		}
		becomeFollower(Term, None);

		vector<string> nodesStrs;
		for (auto n : nodes()) {
			nodesStrs.push_back((boost::format("%X") % n).str());
		}
		string nodesStr = boost::join(nodesStrs, ",");
		iLog(logger, "newRaft %1% [peers: [%2%], term: %3%, commit: %4%, applied: %5%, lastindex: %6%, lastterm: %7%]",
			id, nodesStr.c_str(), Term, raftLog->committed, raftLog->applied, raftLog->lastIndex(), raftLog->lastTerm());
	}

	void Raft::becomeFollower(uint64_t term, uint64_t lead) {
		step = stepFollower;
		reset(term);
		tick = [&]() { tickElection(); };
		this->lead = lead;
		state = StateFollower;
		iLog(logger, "%1% became follower at term %2%", id, Term);
	}

	void Raft::becomeCandidate() {
		// TODO(xiangli) remove the panic when the Raft implementation is stable
		if (state == StateLeader) {
			BOOST_THROW_EXCEPTION(std::runtime_error("invalid transition [leader -> candidate]"));
		}
		step = stepCandidate;
		reset(Term + 1);
		tick = [&]() { tickElection(); };
		Vote = id;
		state = StateCandidate;
		iLog(logger, "%1% became candidate at term %2%", id, Term);
	}

	void Raft::becomePreCandidate() {
		// TODO(xiangli) remove the panic when the Raft implementation is stable
		if (state == StateLeader) {
			BOOST_THROW_EXCEPTION(std::runtime_error("invalid transition [leader -> pre-candidate]"));
		}
		// Becoming a pre-candidate changes our step functions and state,
		// but doesn't change anything else. In particular it does not increase
		// r.Term or change r.Vote.
		step = stepCandidate;
		votes.clear();
		tick = [&]() { tickElection(); };
		state = StatePreCandidate;
		iLog(logger, "%1% became pre-candidate at term %2%", id, Term);
	}

	void Raft::becomeLeader() {
		// TODO(xiangli) remove the panic when the Raft implementation is stable
		if (state == StateFollower) {
			BOOST_THROW_EXCEPTION(std::runtime_error("invalid transition [follower -> leader]"));
		}
		step = stepLeader;
		reset(Term);
		tick = [&]() { tickHeartbeat(); };
		lead = id;
		state = StateLeader;
		// Followers enter replicate mode when they've been successfully probed
		// (perhaps after having received a snapshot as a result). The leader is
		// trivially in this state. Note that r.reset() has initialized this
		// progress with the last index already.
		prs[id]->becomeReplicate();

		// Conservatively set the pendingConfIndex to the last index in the
		// log. There may or may not be a pending config change, but it's
		// safe to delay any future proposals until we commit all our
		// pending log entries, and scanning the entire tail of the log
		// could be expensive.
		pendingConfIndex = raftLog->lastIndex();

		std::array<Entry, 1> emptyEnt = { Entry() };
		if (!appendEntry(emptyEnt)) {
			// This won't happen because we just called reset() above.
			fLog(logger, "empty entry was dropped");
		}
		// As a special case, don't count the initial empty entry towards the
		// uncommitted log quota. This is because we want to preserve the
		// behavior of allowing one entry larger than quota if the current
		// usage is zero.
		reduceUncommittedSize(emptyEnt);
		iLog(logger, "%1% became leader at term %2%", id, Term);
	}

	ErrorCode stepFollower(Raft *r, Message &m) {
		switch (m.type()) {
		case MsgProp: {
			if (r->lead == None) {
				iLog(r->logger, "%1% no leader at term %2%; dropping proposal", r->id, r->Term);
				return ErrProposalDropped;
			} else if (r->disableProposalForwarding) {
				iLog(r->logger, "%1% not forwarding to leader %2% at term %3%; dropping proposal", r->id, r->lead, r->Term);
				return ErrProposalDropped;
			}
			m.set_to(r->lead);
			r->send(std::make_unique<Message>(m));
			break;
		}
		case MsgApp: {
			r->electionElapsed = 0;
			r->lead = m.from();
			r->handleAppendEntries(m);
			break;
		}
		case MsgHeartbeat: {
			r->electionElapsed = 0;
			r->lead = m.from();
			r->handleHeartbeat(m);
			break;
		}
		case MsgSnap: {
			r->electionElapsed = 0;
			r->lead = m.from();
			r->handleSnapshot(m);
			break;
		}
		case MsgTransferLeader: {
			if (r->lead == None) {
				iLog(r->logger, "%1% no leader at term %2%; dropping leader transfer msg", r->id, r->Term);
				return OK;
			}
			m.set_to(r->lead);
			r->send(std::make_unique<Message>(m));
			break;
		}
		case MsgTimeoutNow: {
			if (r->promotable()) {
				iLog(r->logger, "%1% [term %2%] received MsgTimeoutNow from %3% and starts an election to get leadership.", r->id, r->Term, m.from());
				// Leadership transfers never use pre-vote even if r.preVote is true; we
				// know we are not recovering from a partition so there is no need for the
				// extra round trip.
				r->campaign(campaignTransfer);
			} else {
				iLog(r->logger, "%1% received MsgTimeoutNow from %2% but is not promotable", r->id, m.from());
			}
			break;
		}
		case MsgReadIndex: {
			if (r->lead == None) {
				iLog(r->logger, "%1% no leader at term %2%; dropping index reading msg", r->id, r->Term);
				return OK;
			}
			m.set_to(r->lead);
			r->send(std::make_unique<Message>(m));
			break;
		}
		case MsgReadIndexResp: {
			if (m.entries().size() != 1) {
				eLog(r->logger, "%1% invalid format of MsgReadIndexResp from %2%, entries count: %3%", r->id, m.from(), m.entries().size());
				return OK;
			}
			r->readStates.push_back(ReadState{ m.index(), m.entries(0).data() });
			break;
		}
		}
		return OK;
	}
	// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
	// whether they respond to MsgVoteResp or MsgPreVoteResp.
	ErrorCode stepCandidate(Raft *r, Message &m) {
		// Only handle vote responses corresponding to our candidacy (while in
		// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
		// our pre-candidate state).
		MessageType myVoteRespType;
		if (r->state == StatePreCandidate) {
			myVoteRespType = MsgPreVoteResp;
		} else {
			myVoteRespType = MsgVoteResp;
		}
		switch (m.type()) {
		case MsgProp:
		{
			iLog(r->logger, "%1% no leader at term %2%; dropping proposal", r->id, r->Term);
			return ErrProposalDropped;
		}
		case MsgApp:
		{
			r->becomeFollower(m.term(), m.from()); // always m.Term == r.Term
			r->handleAppendEntries(m);
			break;
		}
		case MsgHeartbeat:
		{
			r->becomeFollower(m.term(), m.from()); // always m.Term == r.Term
			r->handleHeartbeat(m);
			break;
		}
		case MsgSnap:
		{
			r->becomeFollower(m.term(), m.from()); // always m.Term == r.Term
			r->handleSnapshot(m);
			break;
		}
		case MsgVoteResp:
		case MsgPreVoteResp:
		{
			if (myVoteRespType != m.type())
				break;
			auto gr = r->poll(m.from(), m.type(), !m.reject());
			iLog(r->logger, "%1% [quorum:%2%] has received %3% %4% votes and %5% vote rejections", r->id, r->quorum(), gr, m.type(), r->votes.size() - gr);
			int quorum = r->quorum();
			if (quorum == gr) {
				if (r->state == StatePreCandidate) {
					r->campaign(campaignElection);
				} else {
					r->becomeLeader();
					r->bcastAppend();
				}
			} else if (quorum == r->votes.size() - gr) {
				// pb.MsgPreVoteResp contains future term of pre-candidate
				// m.Term > r.Term; reuse r.Term
				r->becomeFollower(r->Term, None);
			}
			break;
		}
		case MsgTimeoutNow:
		{
			dLog(r->logger, "%1% [term %2% state %3%] ignored MsgTimeoutNow from %4%", r->id, r->Term, r->state, m.from());
			break;
		}
		}
		return OK;
	}

	ErrorCode stepLeader(Raft *r, Message &m) {
		// These message types do not require any progress for m.From.
		switch (m.type()) {
		case MsgBeat:
		{
			r->bcastHeartbeat();
			return OK;
		}
		case MsgCheckQuorum:
		{
			if (!r->checkQuorumActive()) {
				wLog(r->logger, "%1% stepped down to follower since quorum is not active", r->id);
				r->becomeFollower(r->Term, None);
			}
			return OK;
		}
		case MsgProp:
		{
			if (m.entries().empty()) {
				fLog(r->logger, "%1% stepped empty MsgProp", r->id);
			}
			if (r->prs.find(r->id) == r->prs.end()) {
				// If we are not currently a member of the range (i.e. this node
				// was removed from the configuration while serving as leader),
				// drop any new proposals.
				return ErrProposalDropped;
			}
			if (r->leadTransferee != None) {
				dLog(r->logger, "%1% [term %2%] transfer leadership to %3% is in progress; dropping proposal", r->id, r->Term, r->leadTransferee);
				return ErrProposalDropped;
			}

			for (int i = 0; i < m.entries().size(); i++) {
				auto &e = m.entries(i);
				if (e.type() == EntryConfChange) {
					if (r->pendingConfIndex > r->raftLog->applied) {
						iLog(r->logger, "propose conf %1% ignored since pending unapplied configuration [index %2%, applied %3%]",
							entryString(e).c_str(), r->pendingConfIndex, r->raftLog->applied);
						Entry ent;
						ent.set_type(EntryNormal);
						*m.mutable_entries(i) = ent;
					} else {
						r->pendingConfIndex = r->raftLog->lastIndex() + uint64_t(i) + 1;
					}
				}
			}
			auto ents = make_slice(m.entries());
			if (!r->appendEntry(ents)) {
				return ErrProposalDropped;
			}
			r->bcastAppend();
			return OK;
		}
		case MsgReadIndex:
		{
			if (r->quorum() > 1) {
				if (r->raftLog->zeroTermOnErrCompacted(r->raftLog->term(r->raftLog->committed)) != r->Term) {
					// Reject read only request when this leader has not committed any log entry at its term.
					return OK;
				}

				// thinking: use an interally defined context instead of the user given context.
				// We can express this in terms of the term and index instead of a user-supplied value.
				// This would allow multiple reads to piggyback on the same message.
				switch (r->readOnly->option) {
				case ReadOnlySafe:
				{
					r->readOnly->addRequest(r->raftLog->committed, m);
					r->bcastHeartbeatWithCtx(m.entries(0).data());
					break;
				}
				case ReadOnlyLeaseBased:
					uint64_t ri = r->raftLog->committed;
					if (m.from() == None || m.from() == r->id) { // from local member
						r->readStates.push_back(ReadState{ r->raftLog->committed, m.entries(0).data() });
					} else {
						auto msg = make_message(m.from(), MsgReadIndexResp);
						msg->set_index(ri);
						*msg->mutable_entries() = m.entries();
						r->send(std::move(msg));
					}
					break;
				}
			} else {
				r->readStates.push_back(ReadState{ r->raftLog->committed, m.entries(0).data() });
			}
			return OK;
		}
		}
		// All other message types require a progress for m.From (pr).
		auto pr = r->getProgress(m.from());
		if (!pr) {
			dLog(r->logger, "%1% no progress available for %2%", r->id, m.from());
			return OK;
		}
		switch (m.type()) {
		case MsgAppResp:
		{
			pr->RecentActive = true;

			if (m.reject()) {
				dLog(r->logger, "%1% received msgApp rejection(lastindex: %2%) from %3% for index %4%",
					r->id, m.rejecthint(), m.from(), m.index());
				if (pr->maybeDecrTo(m.index(), m.rejecthint())) {
					dLog(r->logger, "%1% decreased progress of %2% to [%3%]", r->id, m.from(), pr->to_string());
					if (pr->State == ProgressStateReplicate) {
						pr->becomeProbe();
					}
					r->sendAppend(m.from());
				}
			} else {
				bool oldPaused = pr->IsPaused();
				if (pr->maybeUpdate(m.index())) {
					switch (pr->State) {
					case ProgressStateProbe:
						pr->becomeReplicate();
						break;
					case ProgressStateSnapshot:
						if (pr->needSnapshotAbort()) {
							dLog(r->logger, "%1% snapshot aborted, resumed sending replication messages to %2% [%3%]", r->id, m.from(), pr->to_string());
							pr->becomeProbe();
						}
						break;
					case ProgressStateReplicate:
						pr->ins->freeTo(m.index());
						break;
					}

					if (r->maybeCommit()) {
						r->bcastAppend();
					} else if (oldPaused) {
						// If we were paused before, this node may be missing the
						// latest commit index, so send it.
						r->sendAppend(m.from());
					}
					// We've updated flow control information above, which may
					// allow us to send multiple (size-limited) in-flight messages
					// at once (such as when transitioning from probe to
					// replicate, or when freeTo() covers multiple messages). If
					// we have more entries to send, send as many messages as we
					// can (without sending empty messages for the commit index)
					for (; r->maybeSendAppend(m.from(), false);) {
					}
					// Transfer leadership is in progress.
					if (m.from() == r->leadTransferee && pr->Match == r->raftLog->lastIndex()) {
						iLog(r->logger, "%1% sent MsgTimeoutNow to %2% after received MsgAppResp", r->id, m.from());
						r->sendTimeoutNow(m.from());
					}
				}
			}
			break;
		}
		case MsgHeartbeatResp:
		{
			pr->RecentActive = true;
			pr->resume();

			// free one slot for the full inflights window to allow progress.
			if (pr->State == ProgressStateReplicate && pr->ins->full()) {
				pr->ins->freeFirstOne();
			}
			if (pr->Match < r->raftLog->lastIndex()) {
				r->sendAppend(m.from());
			}

			if (r->readOnly->option != ReadOnlySafe || m.context().empty()) {
				return OK;
			}

			auto ackCount = r->readOnly->recvAck(m);
			if (ackCount < r->quorum()) {
				return OK;
			}

			std::vector<readIndexStatusPtr> rss;
			r->readOnly->advance(m, rss);
			for (auto &rs : rss) {
				auto &req = rs->req;
				if (req.from() == None || req.from() == r->id) { // from local member
					r->readStates.push_back(ReadState{ rs->index, req.entries(0).data() });
				} else {
					auto msg = make_message(req.from(), MsgReadIndexResp);
					msg->set_index(rs->index);
					*msg->mutable_entries() = req.entries();
					r->send(std::move(msg));
				}
			}
			break;
		}
		case MsgSnapStatus:
		{
			if (pr->State != ProgressStateSnapshot) {
				return OK;
			}
			if (!m.reject()) {
				pr->becomeProbe();
				dLog(r->logger, "%1% snapshot succeeded, resumed sending replication messages to %2% [%3%]", r->id, m.from(), pr->to_string());
			} else {
				pr->snapshotFailure();
				pr->becomeProbe();
				dLog(r->logger, "%1% snapshot failed, resumed sending replication messages to %2% [%3%]", r->id, m.from(), pr->to_string());
			}
			// If snapshot finish, wait for the msgAppResp from the remote node before sending
			// out the next msgApp.
			// If snapshot failure, wait for a heartbeat interval before next try
			pr->pause();
			break;
		}
		case MsgUnreachable:
		{
			// During optimistic replication, if the remote becomes unreachable,
			// there is huge probability that a MsgApp is lost.
			if (pr->State == ProgressStateReplicate) {
				pr->becomeProbe();
			}
			dLog(r->logger, "%1% failed to send message to %2% because it is unreachable [%3%]", r->id, m.from(), pr->to_string());
			break;
		}
		case MsgTransferLeader:
		{
			if (pr->IsLearner) {
				dLog(r->logger, "%1% is learner. Ignored transferring leadership", r->id);
				return OK;
			}
			uint64_t leadTransferee = m.from();
			uint64_t lastLeadTransferee = r->leadTransferee;
			if (lastLeadTransferee != None) {
				if (lastLeadTransferee == leadTransferee) {
					iLog(r->logger, "%1% [term %2%] transfer leadership to %3% is in progress, ignores request to same node %4%",
						r->id, r->Term, leadTransferee, leadTransferee);
					return OK;
				}
				r->abortLeaderTransfer();
				iLog(r->logger, "%1% [term %2%] abort previous transferring leadership to %3%", r->id, r->Term, lastLeadTransferee);
			}
			if (leadTransferee == r->id) {
				dLog(r->logger, "%1% is already leader. Ignored transferring leadership to self", r->id);
				return OK;
			}
			// Transfer leadership to third party.
			iLog(r->logger, "%1% [term %2%] starts to transfer leadership to %3%", r->id, r->Term, leadTransferee);
			// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
			r->electionElapsed = 0;
			r->leadTransferee = leadTransferee;
			if (pr->Match == r->raftLog->lastIndex()) {
				r->sendTimeoutNow(leadTransferee);
				iLog(r->logger, "%1% sends MsgTimeoutNow to %2% immediately as %3% already has up-to-date log", r->id, leadTransferee, leadTransferee);
			} else {
				r->sendAppend(leadTransferee);
			}
			break;
		}
		}
		return OK;
	}

	// tickElection is run by followers and candidates after electionTimeout.
	void Raft::tickElection() {
		electionElapsed++;
		if (promotable() && pastElectionTimeout()) {
			electionElapsed = 0;
			Message msg;
			msg.set_from(id);
			msg.set_type(MsgHup);
			Step(msg);
		}
	}

	// promotable indicates whether state machine can be promoted to leader,
	// which is true when its own id is in progress list.
	bool Raft::promotable() {
		return prs.find(id) != prs.end();
	}

	// pastElectionTimeout returns true iff electionElapsed is greater
	// than or equal to the randomized election timeout in
	// [electiontimeout, 2 * electiontimeout - 1].
	bool Raft::pastElectionTimeout() {
		return electionElapsed >= randomizedElectionTimeout;
	}

	ErrorCode Raft::Step(Message &m) {
		// Handle the message term, which may result in our stepping down to a follower.
		if (m.term() == 0) {
			// local message
		} else if (m.term() > Term) {
			if (m.type() == MsgVote || m.type() == MsgPreVote) {
				bool force = (m.context() == campaignTransfer);
				bool inLease = checkQuorum && lead != None && electionElapsed < electionTimeout;
				if (!force && inLease) {
					// If a server receives a RequestVote request within the minimum election timeout
					// of hearing from a current leader, it does not update its term or grant its vote
					iLog(logger, "%1% [logterm: %2%, index: %3%, vote: %4%] ignored %5% from %6% [logterm: %7%, index: %8%] at term %9%: lease is not expired (remaining ticks: %10%)",
						id, raftLog->lastTerm(), raftLog->lastIndex(), Vote, m.type(), m.from(), m.logterm(), m.index(), Term, electionTimeout - electionElapsed);
					return OK;
				}
			}
			if (m.type() == MsgPreVote) {
				// Never change our term in response to a PreVote
			} else if (m.type() == MsgPreVoteResp && !m.reject()) {
				// We send pre-vote requests with a term in our future. If the
				// pre-vote is granted, we will increment our term when we get a
				// quorum. If it is not, the term comes from the node that
				// rejected our vote so we should become a follower at the new
				// term.
			} else {
				iLog(logger, "%1% [term: %2%] received a %3% message with higher term from %4% [term: %5%]",
					id, Term, m.type(), m.from(), m.term());
				if (m.type() == MsgApp || m.type() == MsgHeartbeat || m.type() == MsgSnap) {
					becomeFollower(m.term(), m.from());
				} else {
					becomeFollower(m.term(), None);
				}
			}
		} else if (m.term() < Term) {
			if ((checkQuorum || preVote) && (m.type() == MsgHeartbeat || m.type() == MsgApp)) {
				// We have received messages from a leader at a lower term. It is possible
				// that these messages were simply delayed in the network, but this could
				// also mean that this node has advanced its term number during a network
				// partition, and it is now unable to either win an election or to rejoin
				// the majority on the old term. If checkQuorum is false, this will be
				// handled by incrementing term numbers in response to MsgVote with a
				// higher term, but if checkQuorum is true we may not advance the term on
				// MsgVote and must generate other messages to advance the term. The net
				// result of these two features is to minimize the disruption caused by
				// nodes that have been removed from the cluster's configuration: a
				// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
				// but it will not receive MsgApp or MsgHeartbeat, so it will not create
				// disruptive term increases, by notifying leader of this node's activeness.
				// The above comments also true for Pre-Vote
				//
				// When follower gets isolated, it soon starts an election ending
				// up with a higher term than leader, although it won't receive enough
				// votes to win the election. When it regains connectivity, this response
				// with "pb.MsgAppResp" of higher term would force leader to step down.
				// However, this disruption is inevitable to free this stuck node with
				// fresh election. This can be prevented with Pre-Vote phase.
				send(make_message(m.from(), MsgAppResp));
			} else if (m.type() == MsgPreVote) {
				// Before Pre-Vote enable, there may have candidate with higher term,
				// but less log. After update to Pre-Vote, the cluster may deadlock if
				// we drop messages with a lower term.
				iLog(logger, "%1% [logterm: %2%, index: %3%, vote: %4%] rejected %5% from %6% [logterm: %7%, index: %8%] at term %9%",
					id, raftLog->lastTerm(), raftLog->lastIndex(), Vote, m.type(), m.from(), m.logterm(), m.index(), Term);
				send(make_message(m.from(), MsgPreVoteResp, Term, true));
			} else {
				// ignore other cases
				iLog(logger, "%1% [term: %2%] ignored a %3% message with lower term from %4% [term: %5%]",
					id, Term, m.type(), m.from(), m.term());
			}
			return OK;
		}
		switch (m.type()) {
		case MsgHup:
		{
			if (state != StateLeader) {
				auto ents = raftLog->slice(raftLog->applied + 1, raftLog->committed + 1, noLimit);
				if (!ents.Ok()) {
					fLog(logger, "unexpected error getting unapplied entries (%1%)", ents.err);
				}
				int n = numOfPendingConf(ents.value);
				if (n != 0 && raftLog->committed > raftLog->applied) {
					wLog(logger, "%1% cannot campaign at term %2% since there are still %3% pending configuration changes to apply", id, Term, n);
					return OK;
				}

				iLog(logger, "%1% is starting a new election at term %2%", id, Term);
				if (preVote) {
					campaign(campaignPreElection);
				} else {
					campaign(campaignElection);
				}
			} else {
				dLog(logger, "%1% ignoring MsgHup because already leader", id);
			}
			break;
		}
		case MsgVote:
		case MsgPreVote:
		{
			if (isLearner) {
				// TODO: learner may need to vote, in case of node down when confchange.
				iLog(logger, "%1% [logterm: %2%, index: %3%, vote: %4%] ignored %5% from %6% [logterm: %7%, index: %8%] at term %9%: learner can not vote",
					id, raftLog->lastTerm(), raftLog->lastIndex(), Vote, m.type(), m.from(), m.logterm(), m.index(), Term);
				return OK;
			}
			// We can vote if this is a repeat of a vote we've already cast...
			bool canVote = Vote == m.from() ||
				// ...we haven't voted and we don't think there's a leader yet in this term...
				(Vote == None && lead == None) ||
				// ...or this is a PreVote for a future term...
				(m.type() == MsgPreVote && m.term() > Term);
			// ...and we believe the candidate is up to date.
			if (canVote && raftLog->isUpToDate(m.index(), m.logterm())) {
				iLog(logger, "%1% [logterm: %2%, index: %3%, vote: %4%] cast %5% for %6% [logterm: %7%, index: %8%] at term %9%",
					id, raftLog->lastTerm(), raftLog->lastIndex(), Vote, m.type(), m.from(), m.logterm(), m.index(), Term);
				// When responding to Msg{Pre,}Vote messages we include the term
				// from the message, not the local term. To see why consider the
				// case where a single node was previously partitioned away and
				// it's local term is now of date. If we include the local term
				// (recall that for pre-votes we don't update the local term), the
				// (pre-)campaigning node on the other end will proceed to ignore
				// the message (it ignores all out of date messages).
				// The term in the original message and current local term are the
				// same in the case of regular votes, but different for pre-votes.
				send(make_message(m.from(), voteRespMsgType(m.type()), m.term()));
				if (m.type() == MsgVote) {
					// Only record real votes.
					electionElapsed = 0;
					Vote = m.from();
				}
			} else {
				iLog(logger, "%1% [logterm: %2%, index: %3%, vote: %4%] rejected %5% from %6% [logterm: %7%, index: %8%] at term %9%",
					id, raftLog->lastTerm(), raftLog->lastIndex(), Vote, m.type(), m.from(), m.logterm(), m.index(), Term);
				send(make_message(m.from(), voteRespMsgType(m.type()), Term, true));
			}
			break;
		}
		default:
		{
			auto err = step(this, m);
			if (err != OK) {
				return err;
			}
			break;
		}
		}
		return OK;
	}

	// send persists state to stable storage and then sends to its mailbox.
	void Raft::send(MessagePtr &&m) {
		m->set_from(id);
		if (m->type() == MsgVote || m->type() == MsgVoteResp || m->type() == MsgPreVote || m->type() == MsgPreVoteResp) {
			if (m->term() == 0) {
				// All {pre-,}campaign messages need to have the term set when
				// sending.
				// - MsgVote: m.Term is the term the node is campaigning for,
				//   non-zero as we increment the term when campaigning.
				// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
				//   granted, non-zero for the same reason MsgVote is
				// - MsgPreVote: m.Term is the term the node will campaign,
				//   non-zero as we use m.Term to indicate the next term we'll be
				//   campaigning for
				// - MsgPreVoteResp: m.Term is the term received in the original
				//   MsgPreVote if the pre-vote was granted, non-zero for the
				//   same reasons MsgPreVote is
				fLog(logger, "term should be set when sending %1%", m->type());
			}
		} else {
			if (m->term() != 0) {
				fLog(logger, "term should not be set when sending %1% (was %2%)", m->type(), m->term());
			}
			// do not attach term to MsgProp, MsgReadIndex
			// proposals are a way to forward to the leader and
			// should be treated as local message.
			// MsgReadIndex is also forwarded to leader.
			if (m->type() != MsgProp && m->type() != MsgReadIndex) {
				m->set_term(Term);
			}
		}
		msgs.emplace(msgs.end(), std::move(m));
	}

	void Raft::campaign(CampaignType t) {
		uint64_t term;
		MessageType voteMsg;
		if (t == campaignPreElection) {
			becomePreCandidate();
			voteMsg = MsgPreVote;
			// PreVote RPCs are sent for the next term before we've incremented r.Term.
			term = Term + 1;
		} else {
			becomeCandidate();
			voteMsg = MsgVote;
			term = Term;
		}
		if (quorum() == poll(id, voteRespMsgType(voteMsg), true)) {
			// We won the election after voting for ourselves (which must mean that
			// this is a single-node cluster). Advance to the next state.
			if (t == campaignPreElection) {
				campaign(campaignElection);
			} else {
				becomeLeader();
			}
			return;
		}
		for (auto it = prs.begin(); it != prs.end(); ++it) {
			if (it->first == id) {
				continue;
			}
			iLog(logger, "%1% [logterm: %2%, index: %3%] sent %4% request to %5% at term %6%",
				id, raftLog->lastTerm(), raftLog->lastIndex(), voteMsg, it->first, Term);

			string ctx;
			if (t == campaignTransfer) {
				ctx = t;
			}
			MessagePtr msg = make_message(it->first, voteMsg, term);
			msg->set_index(raftLog->lastIndex());
			msg->set_logterm(raftLog->lastTerm());
			msg->set_context(ctx);
			send(std::move(msg));
		}
	}

	int Raft::quorum() { return int(prs.size() / 2 + 1); }

	int Raft::poll(uint64_t id, MessageType t, bool v) {
		int granted = 0;
		if (v) {
			iLog(logger, "%1% received %2% from %3% at term %4%", id, t, id, Term);
		} else {
			iLog(logger, "%1% received %2% rejection from %3% at term %4%", id, t, id, Term);
		}
		auto it = votes.find(id);
		if (it == votes.end()) {
			votes[id] = v;
		}
		for (auto it = votes.begin(); it != votes.end(); ++it) {
			if (it->second) {
				granted++;
			}
		}
		return granted;
	}

	void Raft::reset(uint64_t term) {
		if (Term != term) {
			Term = term;
			Vote = None;
		}
		lead = None;

		electionElapsed = 0;
		heartbeatElapsed = 0;
		resetRandomizedElectionTimeout();

		abortLeaderTransfer();

		votes.clear();
		forEachProgress([&](uint64_t id, Progress *pr) {
			pr->Next = raftLog->lastIndex() + 1;
			pr->ins.reset(new inflights(maxInflight));
			if (id == this->id) {
				pr->Match = raftLog->lastIndex();
			}
		});

		pendingConfIndex = 0;
		uncommittedSize = 0;
		readOnly.reset(new ReadOnly(readOnly->option));
	}
	boost::mt19937 gen;
	void Raft::resetRandomizedElectionTimeout() {
		boost::random::uniform_int_distribution<> dist(0, electionTimeout - 1);
		randomizedElectionTimeout = electionTimeout + dist(gen);
	}

	void Raft::abortLeaderTransfer() {
		leadTransferee = None;
	}

	void Raft::forEachProgress(const std::function<void(uint64_t id, Progress *pr)> &f) {
		for (auto it = prs.begin(); it != prs.end(); ++it) {
			f(it->first, it->second.get());
		}

		for (auto it = learnerPrs.begin(); it != learnerPrs.end(); ++it) {
			f(it->first, it->second.get());
		}
	}

	// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
	void Raft::tickHeartbeat() {
		heartbeatElapsed++;
		electionElapsed++;

		if (electionElapsed >= electionTimeout) {
			electionElapsed = 0;
			if (checkQuorum) {
				Message msg;
				msg.set_type(MsgCheckQuorum);
				msg.set_from(id);
				Step(msg);
			}
			// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
			if (state == StateLeader && leadTransferee != None) {
				abortLeaderTransfer();
			}
		}

		if (state != StateLeader) {
			return;
		}

		if (heartbeatElapsed >= heartbeatTimeout) {
			heartbeatElapsed = 0;
			Message msg;
			msg.set_type(MsgBeat);
			msg.set_from(id);
			Step(msg);
		}
	}

	bool Raft::appendEntry(IEntrySlice &es) {
		uint64_t li = raftLog->lastIndex();
		for (size_t i = 0; i < es.size(); ++i) {
			es[i].set_term(Term);
			es[i].set_index(li + 1 + uint64_t(i));
		}
		// Track the size of this uncommitted proposal.
		if (!increaseUncommittedSize(es)) {
			dLog(logger,
				"%1% appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
				id);
			// Drop the proposal.
			return false;
		}
		// use latest "last" index after truncate/append
		li = raftLog->append(es);
		getProgress(id)->maybeUpdate(li);
		// Regardless of maybeCommit's return, our caller will call bcastAppend.
		maybeCommit();
		return true;
	}

	// increaseUncommittedSize computes the size of the proposed entries and
	// determines whether they would push leader over its maxUncommittedSize limit.
	// If the new entries would exceed the limit, the method returns false. If not,
	// the increase in uncommitted entry size is recorded and the method returns
	// true.
	bool Raft::increaseUncommittedSize(const IEntrySlice &ents) {
		uint64_t s = 0;
		for (const Entry &e : ents) {
			s += uint64_t(PayloadSize(e));
		}

		if (uncommittedSize > 0 && uncommittedSize + s > maxUncommittedSize) {
			// If the uncommitted tail of the Raft log is empty, allow any size
			// proposal. Otherwise, limit the size of the uncommitted tail of the
			// log and drop any proposal that would push the size over the limit.
			return false;
		}
		uncommittedSize += s;
		return true;
	}

	Progress *Raft::getProgress(uint64_t id) {
		auto it = prs.find(id);
		if (it != prs.end()) {
			return it->second.get();
		}
		auto it1 = learnerPrs.find(id);
		if (it1 != learnerPrs.end()) {
			return it1->second.get();
		}
		return nullptr;
	}

	// maybeCommit attempts to advance the commit index. Returns true if
	// the commit index changed (in which case the caller should call
	// r.bcastAppend).
	bool Raft::maybeCommit() {
		// Preserving matchBuf across calls is an optimization
		// used to avoid allocating a new slice on each call.
		if (matchBuf.capacity() < prs.size()) {
			matchBuf.reserve(prs.size());
		}
		matchBuf.resize(prs.size());
		int idx = 0;
		for (auto it = prs.begin(); it != prs.end(); ++it) {
			matchBuf[idx++] = it->second->Match;
		}
		sort(matchBuf.begin(), matchBuf.end());
		uint64_t mci = matchBuf[matchBuf.size() - quorum()];
		return raftLog->maybeCommit(mci, Term);
	}

	// reduceUncommittedSize accounts for the newly committed entries by decreasing
	// the uncommitted entry size limit.
	void Raft::reduceUncommittedSize(const IEntrySlice &ents) {
		if (uncommittedSize == 0) {
			// Fast-path for followers, who do not track or enforce the limit.
			return;
		}

		uint64_t s = 0;
		for (auto &e : ents) {
			s += uint64_t(PayloadSize(e));
		}
		if (s > uncommittedSize) {
			// uncommittedSize may underestimate the size of the uncommitted Raft
			// log tail but will never overestimate it. Saturate at 0 instead of
			// allowing overflow.
			uncommittedSize = 0;
		} else {
			uncommittedSize -= s;
		}
	}

	void Raft::handleAppendEntries(const Message &m) {
		if (m.index() < raftLog->committed) {
			MessagePtr msg = make_message(m.from(), MsgAppResp);
			msg->set_index(raftLog->committed);
			send(std::move(msg));
			return;
		}

		uint64_t mlastIndex;
		auto ents = make_slice(m.entries());
		bool ret = raftLog->maybeAppend(m.index(), m.logterm(), m.commit(), ents, mlastIndex);
		if (ret) {
			MessagePtr msg = make_message(m.from(), MsgAppResp);
			msg->set_index(mlastIndex);
			send(std::move(msg));
		} else {
			auto t = raftLog->term(m.index());
			dLog(logger, "%1% [logterm: %2%, index: %3%] rejected msgApp [logterm: %4%, index: %5%] from %6%",
				id, raftLog->zeroTermOnErrCompacted(t), m.index(), m.logterm(), m.index(), m.from());
			MessagePtr msg = make_message(m.from(), MsgAppResp, 0, true);
			msg->set_index(m.index());
			msg->set_rejecthint(raftLog->lastIndex());
			send(std::move(msg));
		}
	}

	void Raft::handleHeartbeat(Message &m) {
		raftLog->commitTo(m.commit());
		auto msg = make_message(m.from(), MsgHeartbeatResp);
		msg->set_context(m.context());
		send(std::move(msg));
	}

	void Raft::handleSnapshot(Message &m) {
		auto &metadata = m.snapshot().metadata();
		uint64_t sindex = metadata.index();
		uint64_t sterm = metadata.term();
		if (restore(m.snapshot())) {
			iLog(logger, "%1% [commit: %2%] restored snapshot [index: %3%, term: %4%]",
				id, raftLog->committed, sindex, sterm);
			auto msg = make_message(m.from(), MsgAppResp);
			msg->set_index(raftLog->lastIndex());
			send(std::move(msg));
		} else {
			iLog(logger, "%1% [commit: %2%] ignored snapshot [index: %3%, term: %4%]",
				id, raftLog->committed, sindex, sterm);
			auto msg = make_message(m.from(), MsgAppResp);
			msg->set_index(raftLog->committed);
			send(std::move(msg));
		}
	}

	// restore recovers the state machine from a snapshot. It restores the log and the
	// configuration of state machine.
	bool Raft::restore(const Snapshot &s) {
		if (s.metadata().index() <= raftLog->committed) {
			return false;
		}
		if (raftLog->matchTerm(s.metadata().index(), s.metadata().term())) {
			iLog(logger, "%1% [commit: %2%, lastindex: %3%, lastterm: %4%] fast-forwarded commit to snapshot [index: %5%, term: %6%]",
				id, raftLog->committed, raftLog->lastIndex(), raftLog->lastTerm(), s.metadata().index(), s.metadata().term());
			raftLog->commitTo(s.metadata().index());
			return false;
		}

		// The normal peer can't become learner.
		if (!isLearner) {
			auto &learners = s.metadata().conf_state().learners();
			for (auto it = learners.begin(); it != learners.end(); ++it) {
				if (*it == id) {
					eLog(logger, "%1% can't become learner when restores snapshot [index: %2%, term: %3%]", id, s.metadata().index(), s.metadata().term());
					return false;
				}
			}
		}

		iLog(logger, "%1% [commit: %2%, lastindex: %3%, lastterm: %4%] starts to restore snapshot [index: %5%, term: %6%]",
			id, raftLog->committed, raftLog->lastIndex(), raftLog->lastTerm(), s.metadata().index(), s.metadata().term());

		raftLog->restore(s);
		prs.clear();
		learnerPrs.clear();
		auto &nodes_ = s.metadata().conf_state().nodes();
		auto &learners_ = s.metadata().conf_state().learners();
		vector<uint64_t> nodes, learners;
		nodes.reserve(nodes_.size());
		learners.reserve(learners_.size());
		for (auto it = nodes_.begin(); it != nodes_.end(); ++it)
			nodes.push_back(*it);
		for (auto it = learners_.begin(); it != learners_.end(); ++it)
			learners.push_back(*it);
		restoreNode(nodes, false);
		restoreNode(learners, true);
		return true;
	}

	void Raft::restoreNode(vector<uint64_t> &nodes, bool isLearner) {
		for (auto n : nodes) {
			uint64_t match = 0, next = raftLog->lastIndex() + 1;
			if (n == id) {
				match = next - 1;
				this->isLearner = isLearner;
			}
			setProgress(n, match, next, isLearner);
			iLog(logger, "%1% restored progress of %2% [%3%]", id, n, getProgress(n)->to_string());
		}
	}

	void Raft::setProgress(uint64_t id, uint64_t match, uint64_t next, bool isLearner) {
		if (!isLearner) {
			learnerPrs.erase(id);
			auto progress = std::make_unique<Progress>();
			progress->Next = next;
			progress->Match = match;
			progress->ins = std::make_unique<inflights>(maxInflight);
			prs[id] = std::move(progress);
			return;
		}

		if (prs.find(id) != prs.end()) {
			fLog(logger, "%1% unexpected changing from voter to learner for %2%", this->id, id);
		}
		auto progress = std::make_unique<Progress>();
		progress->Next = next;
		progress->Match = match;
		progress->ins = std::make_unique<inflights>(maxInflight);
		progress->IsLearner = true;
		learnerPrs[id] = std::move(progress);
	}

	void Raft::delProgress(uint64_t id) {
		prs.erase(id);
		learnerPrs.erase(id);
	}

	// bcastAppend sends RPC, with entries to all peers that are not up-to-date
	// according to the progress recorded in r.prs.
	void Raft::bcastAppend() {
		forEachProgress([&](uint64_t id, Progress *pr) {
			if (id == this->id) {
				return;
			}
			sendAppend(id);
		});
	}
	// bcastHeartbeat sends RPC, without entries to all the peers.
	void Raft::bcastHeartbeat() {
		auto lastCtx = readOnly->lastPendingRequestCtx();
		bcastHeartbeatWithCtx(lastCtx);
	}

	void Raft::bcastHeartbeatWithCtx(const string &ctx) {
		forEachProgress([&](uint64_t id, Progress *pr) {
			if (id == this->id) {
				return;
			}
			sendHeartbeat(id, ctx);
		});
	}

	// sendHeartbeat sends a heartbeat RPC to the given peer.
	void Raft::sendHeartbeat(uint64_t to, const string &ctx) {
		// Attach the commit as min(to.matched, r.committed).
		// When the leader sends out heartbeat message,
		// the receiver(follower) might not be matched with the leader
		// or it might not have all the committed entries.
		// The leader MUST NOT forward the follower's commit to
		// an unmatched index.
		auto commit = min(getProgress(to)->Match, raftLog->committed);
		auto m = make_message(to, MsgHeartbeat);
		m->set_commit(commit);
		m->set_context(ctx);
		send(std::move(m));
	}

	// sendAppend sends an append RPC with new entries (if any) and the
	// current commit index to the given peer.
	void Raft::sendAppend(uint64_t to) {
		maybeSendAppend(to, true);
	}

	// maybeSendAppend sends an append RPC with new entries to the given peer,
	// if necessary. Returns true if a message was sent. The sendIfEmpty
	// argument controls whether messages with no entries will be sent
	// ("empty" messages are useful to convey updated Commit indexes, but
	// are undesirable when we're sending multiple messages in a batch).
	bool Raft::maybeSendAppend(uint64_t to, bool sendIfEmpty) {
		auto pr = getProgress(to);
		if (pr->IsPaused()) {
			return false;
		}
		auto m = make_message(to);

		auto rterm = raftLog->term(pr->Next - 1);
		auto rents = raftLog->entries(pr->Next, maxMsgSize);
		if (rents.value.empty() && !sendIfEmpty) {
			return false;
		}

		if (!rterm.Ok() || !rents.Ok()) { // send snapshot if we failed to get term or entries
			if (!pr->RecentActive) {
				dLog(logger, "ignore sending snapshot to %1% since it is not recently active", to);
				return false;
			}

			m->set_type(MsgSnap);
			auto rsn = raftLog->snapshot();
			if (!rsn.Ok()) {
				if (rsn.err == ErrSnapshotTemporarilyUnavailable) {
					dLog(logger, "%1% failed to send snapshot to %2% because snapshot is temporarily unavailable", id, to);
					return false;
				}
				abort(); // TODO(bdarnell)
			}
			if (IsEmptySnap(*rsn.value)) {
				fLog(logger, "need non-empty snapshot");
			}
			*m->mutable_snapshot() = *rsn.value;
			uint64_t sindex = rsn.value->metadata().index(), sterm = rsn.value->metadata().term();
			dLog(logger, "%1% [firstindex: %2%, commit: %3%] sent snapshot[index: %4%, term: %5%] to %6% [%7%]",
				id, raftLog->firstIndex(), raftLog->committed, sindex, sterm, to, pr->to_string());
			pr->becomeSnapshot(sindex);
			dLog(logger, "%1% paused sending replication messages to %2% [%3%]", id, to, pr->to_string());
		} else {
			m->set_type(MsgApp);
			m->set_index(pr->Next - 1);
			m->set_logterm(rterm.value);
			auto ents_ = m->mutable_entries();
			auto &ents = rents.value;
			for (auto &ent : ents) *ents_->Add() = ent;
			m->set_commit(raftLog->committed);
			if (!ents.empty()) {
				switch (pr->State) {
					// optimistically increase the next when in ProgressStateReplicate
				case ProgressStateReplicate:
				{
					uint64_t last = ents[ents.size() - 1].index();
					pr->optimisticUpdate(last);
					pr->ins->add(last);
					break;
				}
				case ProgressStateProbe:
				{
					pr->pause();
					break;
				}
				default:
				{
					fLog(logger, "%1% is sending append in unhandled state %2%", id, pr->State);
					break;
				}
				}
			}
		}
		send(std::move(m));
		return true;
	}

	// checkQuorumActive returns true if the quorum is active from
	// the view of the local Raft state machine. Otherwise, it returns
	// false.
	// checkQuorumActive also resets all RecentActive to false.
	bool Raft::checkQuorumActive() {
		int act = 0;
		forEachProgress([&](uint64_t id, Progress *pr) {
			if (id == this->id) { // self is always active
				act++;
				return;
			}

			if (pr->RecentActive && !pr->IsLearner) {
				act++;
			}

			pr->RecentActive = false;
		});

		return act >= quorum();
	}

	void Raft::sendTimeoutNow(uint64_t to) {
		send(make_message(to, MsgTimeoutNow));
	}

	void Raft::loadState(const HardState &state) {
		if (state.commit() < raftLog->committed || state.commit() > raftLog->lastIndex()) {
			fLog(logger, "%1% state.commit %2% is out of range [%3%, %4%]", id, state.commit(), raftLog->committed, raftLog->lastIndex());
		}
		raftLog->committed = state.commit();
		Term = state.term();
		Vote = state.vote();
	}

	vector<uint64_t> Raft::nodes() {
		vector<uint64_t> nodes;
		nodes.reserve(prs.size());
		for (auto it = prs.begin(); it != prs.end(); ++it) {
			nodes.push_back(it->first);
		}
		std::sort(nodes.begin(), nodes.end());
		return nodes;
	}

	vector<uint64_t> Raft::learnerNodes() {
		vector<uint64_t> nodes;
		nodes.reserve(learnerPrs.size());
		for (auto it = learnerPrs.begin(); it != learnerPrs.end(); ++it) {
			nodes.push_back(it->first);
		}
		std::sort(nodes.begin(), nodes.end());
		return nodes;
	}

	void Raft::addNode(uint64_t id) {
		addNodeOrLearnerNode(id, false);
	}


	void Raft::addLearner(uint64_t id) {
		addNodeOrLearnerNode(id, true);
	}

	void Raft::addNodeOrLearnerNode(uint64_t id, bool isLearner) {
		auto pr = getProgress(id);
		if (!pr) {
			setProgress(id, 0, raftLog->lastIndex() + 1, isLearner);
		} else {
			if (isLearner && !pr->IsLearner) {
				// can only change Learner to Voter
				iLog(logger, "%1% ignored addLearner: do not support changing %2% from raft peer to learner.", id, id);
				return;
			}

			if (isLearner == pr->IsLearner) {
				// Ignore any redundant addNode calls (which can happen because the
				// initial bootstrapping entries are applied twice).
				return;
			}

			// change Learner to Voter, use origin Learner progress
			pr->IsLearner = false;
			prs[id] = std::move(learnerPrs[id]);
			learnerPrs.erase(id);
		}

		if (id == this->id) {
			this->isLearner = isLearner;
		}

		// When a node is first added, we should mark it as recently active.
		// Otherwise, CheckQuorum may cause us to step down if it is invoked
		// before the added node has a chance to communicate with us.
		pr = getProgress(id);
		pr->RecentActive = true;
	}

	void Raft::removeNode(uint64_t id) {
		delProgress(id);

		// do not try to commit or abort transferring if there is no nodes in the cluster.
		if (prs.empty() && learnerPrs.empty()) {
			return;
		}

		// The quorum size is now smaller, so see if any pending entries can
		// be committed.
		if (maybeCommit()) {
			bcastAppend();
		}
		// If the removed node is the leadTransferee, then abort the leadership transferring.
		if (state == StateLeader && leadTransferee == id) {
			abortLeaderTransfer();
		}
	}

	SoftState Raft::softState() {
		return SoftState{ lead, state };
	}

	HardState Raft::hardState() {
		HardState hs;
		hs.set_term(Term);
		hs.set_vote(Vote);
		hs.set_commit(raftLog->committed);
		return hs;
	}

} // namespace raft
