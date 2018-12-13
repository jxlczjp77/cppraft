#pragma once
#include <raft/raft.pb.h>
#include <raft/Raft.hpp>

namespace raft {
	using namespace raftpb;
	class Storage {
	public:
		virtual ErrorCode InitialState(HardState &hs, ConfState &cs) = 0;
		virtual ErrorCode Entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out) = 0;
		virtual ErrorCode Term(uint64_t i, uint64_t &t) = 0;
		virtual ErrorCode LastIndex(uint64_t &i) = 0;
		virtual ErrorCode FirstIndex(uint64_t &i) = 0;
		virtual ErrorCode Snapshot(Snapshot **sn) = 0;
	};
	typedef std::shared_ptr<Storage> StoragePtr;

	class MemoryStorage : public Storage {
	public:
		MemoryStorage(const vector<Entry> &ents = { Entry() });
		~MemoryStorage();

		virtual ErrorCode InitialState(HardState &hs, ConfState &cs);
		virtual ErrorCode Entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out);
		virtual ErrorCode Term(uint64_t i, uint64_t &t);
		virtual ErrorCode LastIndex(uint64_t &i);
		virtual ErrorCode FirstIndex(uint64_t &i);
		virtual ErrorCode Snapshot(raftpb::Snapshot **sn);

		ErrorCode Append(const vector<Entry> &entries);
		ErrorCode ApplySnapshot(const raftpb::Snapshot &snapshot);
		ErrorCode CreateSnapshot(uint64_t i, const ConfState *cs, const string &data, raftpb::Snapshot &sh);
		ErrorCode Compact(uint64_t compactIndex);
		ErrorCode SetHardState(const HardState &st);

	private:
		uint64_t firstIndex();
		uint64_t lastIndex();

	public:
		HardState hard_state;
		raftpb::Snapshot snapshot;
		std::vector<Entry> entries;
	};
}
