#pragma once
#include <vector>
#include <stdarg.h>
#include <boost/throw_exception.hpp>
#include <raft/read_only.hpp>
#include <iostream>
#include <map>

namespace raft {
	using namespace std;
	class storage;
	class raft_log;
	class Progress;
	class logger;

	enum error_code {
		OK = 0,
		ErrCompacted,
		ErrSnapOutOfDate,
		ErrUnavailable,
		ErrSnapshotTemporarilyUnavailable,
		ErrSeriaFail,
		ErrAppendOutOfData,
	};
#define SUCCESS(c) (c == OK)
	const char *error_string(error_code c);

	class logger {
	public:
		virtual void Debugf(const char *file, int line, const char *fmt, ...) = 0;
		virtual void Infof(const char *file, int line, const char *fmt, ...) = 0;
		virtual void Warningf(const char *file, int line, const char *fmt, ...) = 0;
		virtual void Errorf(const char *file, int line, const char *fmt, ...) = 0;
		virtual void Fatalf(const char *file, int line, const char *fmt, ...) = 0;
	};

#define dLog(l, fmt, ...) (l)->Debugf(__FILE__, __LINE__, fmt, __VA_ARGS__)
#define iLog(l, fmt, ...) (l)->Infof(__FILE__, __LINE__, fmt, __VA_ARGS__)
#define wLog(l, fmt, ...) (l)->Warningf(__FILE__, __LINE__, fmt, __VA_ARGS__)
#define eLog(l, fmt, ...) (l)->Errorf(__FILE__, __LINE__, fmt, __VA_ARGS__)
#define fLog(l, fmt, ...) (l)->Fatalf(__FILE__, __LINE__, fmt, __VA_ARGS__)

#define dlog(c) char buf[2048] = { '\0' }; va_list args; va_start(args, fmt); log(c, buf, file, line, fmt, args); va_end(args);
	class default_logger : public logger {
	public:
		static default_logger &instance() {
			static default_logger _log;
			return _log;
		}
		virtual void Debugf(const char *file, int line, const char *fmt, ...) {
			dlog("DEBUG"); std::cout << buf << std::endl;
		}
		virtual void Infof(const char *file, int line, const char *fmt, ...) {
			dlog("INFO"); std::cout << buf << std::endl;
		}
		virtual void Warningf(const char *file, int line, const char *fmt, ...) {
			dlog("WARN"); std::cout << buf << std::endl;
		}
		virtual void Errorf(const char *file, int line, const char *fmt, ...) {
			dlog("ERROR"); std::cout << buf << std::endl;
		}
		virtual void Fatalf(const char *file, int line, const char *fmt, ...) {
			dlog("FATAL");
			BOOST_THROW_EXCEPTION(std::runtime_error(buf));
		}
		void log(const char *level, char (&buf)[2048], const char *file, int line, const char *fmt, va_list args) {
			int n = snprintf(buf, sizeof(buf), "[%s %s:%d]", level, file, line);
			vsnprintf(buf + n, sizeof(buf) - n, fmt, args);
		}
	};
#undef dlog

	struct config {
		uint64_t id;
		vector<uint64_t> peers;
		vector<uint64_t> learners;
		int ElectionTick;
		int HeartbeatTick;
		storage *Storage;
		uint64_t Applied;
		uint64_t MaxSizePerMsg;
		uint64_t MaxCommittedSizePerReady;
		uint64_t MaxUncommittedEntriesSize;
		int MaxInflightMsgs;
		bool CheckQuorum;
		bool PreVote;
		ReadOnlyOption ReadOnlyOption;
		logger *Logger;
		bool DisableProposalForwarding;
	};

	enum StateType {
		StateFollower,
		StateCandidate,
		StateLeader,
		StatePreCandidate,
		numStates,
	};


	class raft {
	public:
		uint64_t id;

		uint64_t Term;
		uint64_t Vote;

		vector<ReadState> readStates;

		// the log
		raft_log *raftLog;

		uint64_t maxMsgSize;
		uint64_t maxUncommittedSize;
		int maxInflight;
		map<uint64_t, Progress*> prs;
		map<uint64_t, Progress*> learnerPrs;
		vector<uint64_t> matchBuf;

		StateType state;

		// isLearner is true if the local raft node is a learner.
		bool isLearner;

		map<uint64_t, bool> votes;

		vector<Entry> msgs;

		// the leader id
		uint64_t lead;
		// leadTransferee is id of the leader transfer target when its value is not zero.
		// Follow the procedure defined in raft thesis 3.10.
		uint64_t leadTransferee;
		// Only one conf change may be pending (in the log, but not yet
		// applied) at a time. This is enforced via pendingConfIndex, which
		// is set to a value >= the log index of the latest pending
		// configuration change (if any). Config changes are only allowed to
		// be proposed if the leader's applied index is greater than this
		// value.
		uint64_t pendingConfIndex;
		// an estimate of the size of the uncommitted tail of the Raft log. Used to
		// prevent unbounded log growth. Only maintained by the leader. Reset on
		// term changes.
		uint64_t uncommittedSize;

		readOnly *readOnly;

		// number of ticks since it reached last electionTimeout when it is leader
		// or candidate.
		// number of ticks since it reached last electionTimeout or received a
		// valid message from current leader when it is a follower.
		int electionElapsed;

		// number of ticks since it reached last heartbeatTimeout.
		// only leader keeps heartbeatElapsed.
		int heartbeatElapsed;

		bool checkQuorum;
		bool preVote;

		int heartbeatTimeout;
		int electionTimeout;
		// randomizedElectionTimeout is a random number between
		// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
		// when raft changes its state to follower or candidate.
		int randomizedElectionTimeout;
		bool disableProposalForwarding;

		std::function<void()> tick;
		std::function<void(raft *, Message)> stepFunc;

		logger *logger;
	public:
	};
}
