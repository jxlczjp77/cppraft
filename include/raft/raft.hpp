#pragma once
#include <vector>
#include <stdarg.h>
#include <boost/throw_exception.hpp>
#include <raft/read_only.hpp>
#include <raft/logger.hpp>
#include <map>

namespace raft {
	using namespace std;
	class Storage;
	class raft_log;
	struct Progress;
	class Logger;

	// None is a placeholder node ID used when there is no leader.
	const uint64_t None = 0;
	const uint64_t noLimit = std::numeric_limits<uint64_t>().max();
	typedef std::unique_ptr<Message> MessagePtr;
	typedef std::unique_ptr<Progress> ProgressPtr;

	enum ErrorCode {
		OK = 0,
		ErrCompacted,
		ErrSnapOutOfDate,
		ErrUnavailable,
		ErrSnapshotTemporarilyUnavailable,
		ErrSeriaFail,
		ErrAppendOutOfData,
		ErrProposalDropped,
	};
#define SUCCESS(c) (c == OK)
	const char *error_string(ErrorCode c);

	// Config contains the parameters to start a raft.
	struct Config {
		// ID is the identity of the local raft. ID cannot be 0.
		uint64_t ID;

		// peers contains the IDs of all nodes (including self) in the raft cluster. It
		// should only be set when starting a new raft cluster. Restarting raft from
		// previous configuration will panic if peers is set. peer is private and only
		// used for testing right now.
		vector<uint64_t> peers;

		// learners contains the IDs of all learner nodes (including self if the
		// local node is a learner) in the raft cluster. learners only receives
		// entries from the leader node. It does not vote or promote itself.
		vector<uint64_t> learners;

		// ElectionTick is the number of Node.Tick invocations that must pass between
		// elections. That is, if a follower does not receive any message from the
		// leader of current term before ElectionTick has elapsed, it will become
		// candidate and start an election. ElectionTick must be greater than
		// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
		// unnecessary leader switching.
		int ElectionTick;
		// HeartbeatTick is the number of Node.Tick invocations that must pass between
		// heartbeats. That is, a leader sends heartbeat messages to maintain its
		// leadership every HeartbeatTick ticks.
		int HeartbeatTick;

		// Storage is the storage for raft. raft generates entries and states to be
		// stored in storage. raft reads the persisted entries and states out of
		// Storage when it needs. raft reads out the previous state and configuration
		// out of storage when restarting.
		Storage *Storage;
		// Applied is the last applied index. It should only be set when restarting
		// raft. raft will not return entries to the application smaller or equal to
		// Applied. If Applied is unset when restarting, raft might return previous
		// applied entries. This is a very application dependent configuration.
		uint64_t Applied;

		// MaxSizePerMsg limits the max byte size of each append message. Smaller
		// value lowers the raft recovery cost(initial probing and message lost
		// during normal operation). On the other side, it might affect the
		// throughput during normal replication. Note: math.MaxUint64 for unlimited,
		// 0 for at most one entry per message.
		uint64_t MaxSizePerMsg;
		// MaxCommittedSizePerReady limits the size of the committed entries which
		// can be applied.
		uint64_t MaxCommittedSizePerReady;
		// MaxUncommittedEntriesSize limits the aggregate byte size of the
		// uncommitted entries that may be appended to a leader's log. Once this
		// limit is exceeded, proposals will begin to return ErrProposalDropped
		// errors. Note: 0 for no limit.
		uint64_t MaxUncommittedEntriesSize;
		// MaxInflightMsgs limits the max number of in-flight append messages during
		// optimistic replication phase. The application transportation layer usually
		// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
		// overflowing that sending buffer. TODO (xiangli): feedback to application to
		// limit the proposal rate?
		int MaxInflightMsgs;

		// CheckQuorum specifies if the leader should check quorum activity. Leader
		// steps down when quorum is not active for an electionTimeout.
		bool CheckQuorum;

		// PreVote enables the Pre-Vote algorithm described in raft thesis section
		// 9.6. This prevents disruption when a node that has been partitioned away
		// rejoins the cluster.
		bool PreVote;

		// ReadOnlyOption specifies how the read only request is processed.
		//
		// ReadOnlySafe guarantees the linearizability of the read only request by
		// communicating with the quorum. It is the default and suggested option.
		//
		// ReadOnlyLeaseBased ensures linearizability of the read only request by
		// relying on the leader lease. It can be affected by clock drift.
		// If the clock drift is unbounded, leader might keep the lease longer than it
		// should (clock can move backward/pause without any bound). ReadIndex is not safe
		// in that case.
		// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
		ReadOnlyOption ReadOnlyOption;

		// Logger is the logger used for raft log. For multinode which can host
		// multiple raft group, each raft group can have its own logger
		Logger *Logger;

		// DisableProposalForwarding set to true means that followers will drop
		// proposals, rather than forwarding them to the leader. One use case for
		// this feature would be in a situation where the Raft leader is used to
		// compute the data of a proposal, for example, adding a timestamp from a
		// hybrid logical clock to data in a monotonically increasing way. Forwarding
		// should be disabled to prevent a follower with an inaccurate hybrid
		// logical clock from assigning the timestamp and then forwarding the data
		// to the leader.
		bool DisableProposalForwarding;

		Config();
		string validate();
	};

	enum StateType {
		StateFollower,
		StateCandidate,
		StateLeader,
		StatePreCandidate,
		numStates,
	};

	typedef string CampaignType;
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	extern CampaignType campaignPreElection;
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	extern CampaignType campaignElection;
	// campaignTransfer represents the type of leader transfer
	extern CampaignType campaignTransfer;

	class Raft;
	ErrorCode stepFollower(Raft *r, Message &m);
	ErrorCode stepCandidate(Raft *r, Message &m);
	ErrorCode stepLeader(Raft *r, Message &m);
	typedef ErrorCode(*stepFunc)(class Raft *, Message &);

	class Raft {
	public:
		uint64_t m_id;

		uint64_t m_Term;
		uint64_t m_Vote;

		vector<ReadState> m_readStates;

		// the log
		std::unique_ptr<raft_log> m_raftLog;

		uint64_t m_maxMsgSize;
		uint64_t m_maxUncommittedSize;
		int m_maxInflight;
		map<uint64_t, ProgressPtr> m_prs;
		map<uint64_t, ProgressPtr> m_learnerPrs;
		vector<uint64_t> m_matchBuf;

		StateType m_state;

		// isLearner is true if the local raft node is a learner.
		bool m_isLearner;

		map<uint64_t, bool> m_votes;

		vector<MessagePtr> m_msgs;

		// the leader id
		uint64_t m_lead;
		// leadTransferee is id of the leader transfer target when its value is not zero.
		// Follow the procedure defined in raft thesis 3.10.
		uint64_t m_leadTransferee;
		// Only one conf change may be pending (in the log, but not yet
		// applied) at a time. This is enforced via pendingConfIndex, which
		// is set to a value >= the log index of the latest pending
		// configuration change (if any). Config changes are only allowed to
		// be proposed if the leader's applied index is greater than this
		// value.
		uint64_t m_pendingConfIndex;
		// an estimate of the size of the uncommitted tail of the Raft log. Used to
		// prevent unbounded log growth. Only maintained by the leader. Reset on
		// term changes.
		uint64_t m_uncommittedSize;

		std::unique_ptr<readOnly> m_readOnly;

		// number of ticks since it reached last electionTimeout when it is leader
		// or candidate.
		// number of ticks since it reached last electionTimeout or received a
		// valid message from current leader when it is a follower.
		int m_electionElapsed;

		// number of ticks since it reached last heartbeatTimeout.
		// only leader keeps heartbeatElapsed.
		int m_heartbeatElapsed;

		bool m_checkQuorum;
		bool m_preVote;

		int m_heartbeatTimeout;
		int m_electionTimeout;
		// randomizedElectionTimeout is a random number between
		// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
		// when raft changes its state to follower or candidate.
		int m_randomizedElectionTimeout;
		bool m_disableProposalForwarding;

		std::function<void()> m_tick;
		stepFunc m_step;

		Logger *m_logger;
	public:
		Raft(Config &config);

		void becomeFollower(uint64_t term, uint64_t lead);
		void becomeCandidate();
		void becomePreCandidate();
		void becomeLeader();
		void tickElection();
		void tickHeartbeat();
		void send(MessagePtr &&m);
		ErrorCode Step(Message &m);
		void handleAppendEntries(const Message &m);
		void handleHeartbeat(Message &m);
		void handleSnapshot(Message &m);
		bool promotable();
		void campaign(CampaignType t);
		int poll(uint64_t id, MessageType t, bool v);
		int quorum();
		void bcastAppend();
		void bcastHeartbeat();
		void bcastHeartbeatWithCtx(const string &ctx);
		bool checkQuorumActive();
		bool appendEntry(vector<Entry> &ents);
		void sendAppend(uint64_t to);
		Progress *getProgress(uint64_t id);
		bool maybeCommit();
		bool maybeSendAppend(uint64_t to, bool sendIfEmpty);
		void sendTimeoutNow(uint64_t to);
		void abortLeaderTransfer();
		vector<uint64_t> nodes();
		vector<uint64_t> learnerNodes();

	private:
		bool pastElectionTimeout();
		void reset(uint64_t term);
		void resetRandomizedElectionTimeout();
		bool increaseUncommittedSize(const vector<Entry> &ents);
		void forEachProgress(const std::function<void(uint64_t id, Progress *pr)> &f);
		void reduceUncommittedSize(const vector<Entry> &ents);
		bool restore(const Snapshot &s);
		void restoreNode(vector<uint64_t> &nodes, bool isLearner);
		void setProgress(uint64_t id, uint64_t match, uint64_t next, bool isLearner);
		void delProgress(uint64_t id);
		void sendHeartbeat(uint64_t to, const string &ctx);
		void loadState(const HardState &state);
	};
}
