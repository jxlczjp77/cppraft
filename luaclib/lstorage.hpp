#pragma once
#include <raft/storage.hpp>
#include <lua.hpp>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
using namespace raft;

class LDBStorage : public Storage {
public:
    LDBStorage(leveldb::DB *db);
    ~LDBStorage();

    virtual ErrorCode InitialState(HardState &hs, ConfState &cs);
    virtual Result<IEntrySlicePtr> Entries(uint64_t lo, uint64_t hi, uint64_t max_size);
    virtual Result<uint64_t> Term(uint64_t i);
    virtual Result<uint64_t> LastIndex();
    virtual Result<uint64_t> FirstIndex();
    virtual Result<raftpb::Snapshot*> Snapshot();

    const Entry &operator[](size_t i);
    const Entry &operator[](size_t i) const { return (*const_cast<LDBStorage *>(this))[i]; }
    size_t size() const { return m_count; }

    template<class EntryContainer> ErrorCode Append(const EntryContainer &ents) {
        return AppendSlice(make_slice(ents));
    }
    template<> ErrorCode Append<Entry>(const Entry &ent) {
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

    void push_entry(const Entry &ent);
    void get_entry(uint64_t i, Entry &ent) const;
    std::string entry_data(uint64_t i) const;
    void remove(uint64_t s, uint64_t e);

private:
    uint64_t             m_lastCompactIndex;
    uint64_t             m_count;

    leveldb::DB         *m_db;
    leveldb::WriteBatch  m_batch;
    raftpb::Snapshot     m_snapshot;
    Entry                m_entry_tmp;
};
