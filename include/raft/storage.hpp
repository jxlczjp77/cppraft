#pragma once
#include <raft/raft.pb.h>
#include <raft/entrys.hpp>
#include <raft/utils.hpp>

namespace raft {
	using namespace raftpb;
	class Storage {
	public:
		virtual ErrorCode InitialState(HardState &hs, ConfState &cs) = 0;
		virtual Result<IEntrySlicePtr> Entries(uint64_t lo, uint64_t hi, uint64_t max_size) = 0;
		virtual Result<uint64_t> Term(uint64_t i) = 0;
		virtual Result<uint64_t> LastIndex() = 0;
		virtual Result<uint64_t> FirstIndex() = 0;
		virtual Result<raftpb::Snapshot*> Snapshot() = 0;
	};
	typedef std::shared_ptr<Storage> StoragePtr;

	class MemoryStorage : public Storage {
	public:
		MemoryStorage(const vector<Entry> &ents = { Entry() });
		~MemoryStorage();

		virtual ErrorCode InitialState(HardState &hs, ConfState &cs);
		virtual Result<IEntrySlicePtr> Entries(uint64_t lo, uint64_t hi, uint64_t max_size);
		virtual Result<uint64_t> Term(uint64_t i);
		virtual Result<uint64_t> LastIndex();
		virtual Result<uint64_t> FirstIndex();
		virtual Result<raftpb::Snapshot*> Snapshot();
		template<class EntryContainer> ErrorCode Append(const EntryContainer &ents) {
			return AppendSlice(make_slice(ents));
		}
		ErrorCode Append(const Entry &ent) {
			std::array<Entry, 1> s = { std::move(ent) };
			return AppendSlice(make_slice(s));
		}
		ErrorCode AppendSlice(const IEntrySlice &ents);
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
