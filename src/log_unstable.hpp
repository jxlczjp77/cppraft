#pragma once
#include <raft/entrys.hpp>
#include <raft/logger.hpp>

namespace raft {
	class Logger;

	class unstable {
		friend class raft_log;
	public:
		unstable();
		~unstable();
		unstable(unstable &&u);

		bool maybeFirstIndex(uint64_t &i) const;
		bool maybeLastIndex(uint64_t &i) const;
		bool maybeTerm(uint64_t i, uint64_t &term) const;
		void stableTo(uint64_t i, uint64_t t);
		void stableSnapTo(uint64_t i);
		void restore(const Snapshot &sh);
		void truncateAndAppend(const IEntrySlice &ents);
		void slice(uint64_t lo, uint64_t hi, vector<Entry> &entries);

	private:
		void mustCheckOutOfBounds(uint64_t lo, uint64_t hi);

	public:
		unique_ptr<Snapshot> snapshot;
		EntryUnstableVec entries;
		uint64_t offset;
		Logger *logger;
	};
}
