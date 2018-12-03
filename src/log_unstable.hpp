#pragma once
#include <raft/raft.pb.h>
#include <vector>

namespace raft {
	using namespace raftpb;
	using namespace std;
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
		void truncateAndAppend(const vector<Entry> &ents);
		void slice(uint64_t lo, uint64_t hi, vector<Entry> &entries);

	private:
		void mustCheckOutOfBounds(uint64_t lo, uint64_t hi);

	public:
		unique_ptr<Snapshot> m_snapshot;
		vector<Entry> m_entries;
		uint64_t m_offset;
		Logger *m_logger;
	};
}
