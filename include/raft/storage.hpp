#pragma once
#include <raft/raft.pb.h>
#include <raft/raft.hpp>

namespace raft {
	using namespace raftpb;
	class storage {
	public:
		virtual error_code initial_state(HardState &hs, ConfState &cs) = 0;
		virtual error_code entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out) = 0;
		virtual error_code term(uint64_t i, uint64_t &t) = 0;
		virtual error_code last_index(uint64_t &i) = 0;
		virtual error_code first_index(uint64_t &i) = 0;
		virtual error_code snapshot(Snapshot **sn) = 0;
	};

	class memory_storage : public storage {
	public:
		memory_storage();
		~memory_storage();

		virtual error_code initial_state(HardState &hs, ConfState &cs);
		virtual error_code entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out);
		virtual error_code term(uint64_t i, uint64_t &t);
		virtual error_code last_index(uint64_t &i);
		virtual error_code first_index(uint64_t &i);
		virtual error_code snapshot(Snapshot **sn);

		error_code append(const vector<Entry> &entries);
		error_code apply_snapshot(const Snapshot &snapshot);
		error_code compact(uint64_t compactIndex);

	private:
		uint64_t firstIndex();
		uint64_t lastIndex();

	private:
		HardState m_hard_state;
		Snapshot m_snapshot;
		std::vector<Entry> m_entries;
	};
}
