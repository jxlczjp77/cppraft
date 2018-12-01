#pragma once
#include <raft/storage.hpp>
#include <raft/raft.hpp>
#include "log_unstable.hpp"

namespace raft {
	class raft_log {
	public:
		raft_log(storage *Storage, logger *Logger, uint64_t maxNextEntsSize = std::numeric_limits<uint64_t>::max());
		~raft_log();

		error_code entries(vector<Entry> &out, uint64_t i, uint64_t maxsize = std::numeric_limits<uint64_t>::max());
		error_code slice(vector<Entry> &out, uint64_t lo, uint64_t hi, uint64_t maxSize = std::numeric_limits<uint64_t>::max());
		bool maybeAppend(uint64_t index, uint64_t logTerm, uint64_t committed, const vector<Entry> &ents, uint64_t &lastnewi);
		bool matchTerm(uint64_t i, uint64_t term);
		bool maybeCommit(uint64_t maxIndex, uint64_t term);
		const vector<Entry> &unstableEntries();
		bool hasNextEnts();
		uint64_t lastTerm();
		vector<Entry> nextEnts();
		vector<Entry> allEntries();
		error_code mustCheckOutOfBounds(uint64_t lo, uint64_t hi);
		error_code term(uint64_t i, uint64_t &t);
		uint64_t append(const vector<Entry> &ents);
		uint64_t findConflict(const vector<Entry> &ents);
		uint64_t firstIndex();
		uint64_t lastIndex();
		void restore(const Snapshot &s);
		string to_string();
		bool isUpToDate(uint64_t lasti, uint64_t term);
		void commitTo(uint64_t tocommit);
		void appliedTo(uint64_t i);
		void stableTo(uint64_t i, uint64_t t);
		void stableSnapTo(uint64_t i);
		uint64_t zeroTermOnErrCompacted(uint64_t t, error_code err);

	public:
		storage *m_storage;
		unstable m_unstable;

		uint64_t m_committed;
		uint64_t m_applied;

		logger *m_logger;
		uint64_t m_maxNextEntsSize;
	};
}
