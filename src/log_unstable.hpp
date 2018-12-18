#pragma once
#include <raft/raft.hpp>
#include <raft/logger.hpp>

namespace raft {
	class Logger;

	class unstable {
		friend class raft_log;
	public:
		unstable();
		~unstable();
		unstable(unstable &&u);

		Result<uint64_t> maybeFirstIndex() const;
		Result<uint64_t> maybeLastIndex() const;
		Result<uint64_t> maybeTerm(uint64_t i) const;
		void stableTo(uint64_t i, uint64_t t);
		void stableSnapTo(uint64_t i);
		void restore(const Snapshot &sh);
		void truncateAndAppend(const IEntrySlice &ents);
		IEntrySlicePtr slice(uint64_t lo, uint64_t hi);

	private:
		void mustCheckOutOfBounds(uint64_t lo, uint64_t hi);

	public:
		unique_ptr<Snapshot> snapshot;
		EntryUnstableVec entries;
		uint64_t offset;
		Logger *logger;
	};
}
