#pragma once
#include <raft/raft.pb.h>
#include <raft/Raft.hpp>

namespace raft {
	using namespace raftpb;
	class Storage {
	public:
		virtual ErrorCode InitialState(HardState &hs, ConfState &cs) = 0;
		virtual ErrorCode entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out) = 0;
		virtual ErrorCode term(uint64_t i, uint64_t &t) = 0;
		virtual ErrorCode last_index(uint64_t &i) = 0;
		virtual ErrorCode first_index(uint64_t &i) = 0;
		virtual ErrorCode snapshot(Snapshot **sn) = 0;
	};

	class MemoryStorage : public Storage {
	public:
		MemoryStorage();
		~MemoryStorage();

		virtual ErrorCode InitialState(HardState &hs, ConfState &cs);
		virtual ErrorCode entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out);
		virtual ErrorCode term(uint64_t i, uint64_t &t);
		virtual ErrorCode last_index(uint64_t &i);
		virtual ErrorCode first_index(uint64_t &i);
		virtual ErrorCode snapshot(Snapshot **sn);

		ErrorCode append(const vector<Entry> &entries);
		ErrorCode apply_snapshot(const Snapshot &snapshot);
		ErrorCode compact(uint64_t compactIndex);

	private:
		uint64_t firstIndex();
		uint64_t lastIndex();

	private:
		HardState m_hard_state;
		Snapshot m_snapshot;
		std::vector<Entry> m_entries;
	};
}
