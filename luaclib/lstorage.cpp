#include "lstorage.hpp"
#include <raft/logger.hpp>
#include <leveldb/slice.h>
#include <boost/endian/conversion.hpp>
#define KEY_HARD_STATE "hard_state"
#define KEY_MIN_INDEX "min_index"

LDBStorage::LDBStorage(leveldb::DB *db): m_db(db) {
    m_lastCompactIndex = 0;
    m_count = 0;
}

LDBStorage::~LDBStorage() {
}

ErrorCode LDBStorage::InitialState(HardState &hs, ConfState &cs) {
    std::string hs_data;
    auto s = m_db->Get(leveldb::ReadOptions(), KEY_HARD_STATE, &hs_data);
    if (!s.ok()) {
        hs = HardState();
    } else if(!hs.ParsePartialFromString(hs_data)) {
        return ErrFalse;
    }

    cs = m_snapshot.metadata().conf_state();
    return OK;
}

Result<IEntrySlicePtr> LDBStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size) {
    uint64_t offset = m_lastCompactIndex;
    if (lo <= offset) {
        return ErrCompacted;
    } else if (hi > lastIndex() + 1) {
        return ErrUnavailable;
    }
    if (m_count == 1) { // 仅包含dumy entry
        return ErrUnavailable;
    }
    auto ents = std::make_unique<EntrySlice<LDBStorage>>(*this, lo, hi);
    limitSize(*ents, max_size);
    return { std::move(ents) };
}

Result<uint64_t> LDBStorage::Term(uint64_t i) {
    uint64_t offset = m_lastCompactIndex;
    if (i < offset) {
        return { 0, ErrCompacted };
    } else if (i - offset >= m_count) {
        return { 0, ErrUnavailable };
    }
    Entry ent;
    get_entry(i, ent);
    return { ent.term() };
}

Result<uint64_t> LDBStorage::LastIndex() {
    return { lastIndex() };
}

Result<uint64_t> LDBStorage::FirstIndex() {
    return { firstIndex() };
}

Result<raftpb::Snapshot*> LDBStorage::Snapshot() {
    return { &m_snapshot };
}

ErrorCode LDBStorage::AppendSlice(const IEntrySlice &ents) {
    if (ents.empty()) {
        return OK;
    }

    uint64_t old_first = firstIndex();
    uint64_t new_first = ents[0].index();
    uint64_t new_last = new_first + ents.size() - 1;
    if (new_last < old_first) {
        return OK;
    }

    size_t start_pos = old_first > new_first ? size_t(old_first - new_first) : 0;
    uint64_t offset = ents[start_pos].index() - m_lastCompactIndex;
    if (offset > m_count) {
        return ErrAppendOutOfData;
    } else if (offset < m_count) {
        remove(m_lastCompactIndex + offset, lastIndex() + 1);
        m_count = offset;
    }
    for (auto it = ents.begin() + start_pos; it != ents.end(); ++it) {
        push_entry(*it);
    }
    return OK;
}

ErrorCode LDBStorage::ApplySnapshot(const raftpb::Snapshot &sn) {
    uint64_t old_index = m_snapshot.metadata().index();
    uint64_t new_index = sn.metadata().index();
    if (new_index < old_index) {
        return ErrSnapOutOfDate;
    }
    m_snapshot.CopyFrom(sn);
    remove(m_lastCompactIndex, lastIndex() + 1);
    m_count = 0;

    Entry dumy_entry;
    dumy_entry.set_index(new_index);
    dumy_entry.set_term(sn.metadata().term());
    push_entry(dumy_entry);
    return OK;
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
ErrorCode LDBStorage::CreateSnapshot(uint64_t i, const ConfState *cs, const string &data, raftpb::Snapshot &sh) {
    if (i <= m_snapshot.metadata().index()) {
        sh.Clear();
        return ErrSnapOutOfDate;
    }

    if (i > lastIndex()) {
        fLog(&DefaultLogger::instance(), "snapshot %1% is out of bound lastindex(%2%)", i, lastIndex());
    }
    Entry tmp;
    get_entry(i, tmp);
    m_snapshot.mutable_metadata()->set_index(i);
    m_snapshot.mutable_metadata()->set_term(tmp.term());
    if (cs != nullptr) {
        *m_snapshot.mutable_metadata()->mutable_conf_state() = *cs;
    }
    m_snapshot.set_data(data);
    sh = m_snapshot;
    return OK;
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
ErrorCode LDBStorage::Compact(uint64_t compactIndex) {
    uint64_t offset = m_lastCompactIndex;
    if (compactIndex <= offset) {
        return ErrCompacted;
    }
    if (compactIndex > lastIndex()) {
        fLog(&DefaultLogger::instance(), "compact %1% is out of bound lastindex(%2%)", compactIndex, lastIndex());
    }
    remove(firstIndex(), compactIndex);
    m_lastCompactIndex = compactIndex;
    return OK;
}

// SetHardState saves the current HardState.
ErrorCode LDBStorage::SetHardState(const HardState &st) {
    auto data = st.SerializeAsString();
    auto s = m_db->Put(leveldb::WriteOptions(), KEY_HARD_STATE, data);
    if (!s.ok()) {
        return ErrFalse;
    }
    return OK;
}

void LDBStorage::push_entry(const Entry &ent) {
    auto data = ent.SerializeAsString();
    uint64_t key = boost::endian::native_to_big(ent.index());
    m_batch.Put(leveldb::Slice((const char*)&key, sizeof(key)), data);
    m_count++;
}

void LDBStorage::get_entry(uint64_t i, Entry &ent) const {
    std::string val = entry_data(i);
    if (!ent.ParsePartialFromString(val)) {
        fLog(&DefaultLogger::instance(), "parser entry failed, index(%1%)", i);
    }
}

std::string LDBStorage::entry_data(uint64_t i) const {
    std::string val;
    uint64_t key = boost::endian::native_to_big(i);
    auto s = m_db->Get(leveldb::ReadOptions(), leveldb::Slice((const char *)&key, sizeof(key)), &val);
    if (!s.ok()) {
        fLog(&DefaultLogger::instance(), "get entry failed, index(%1%)", i);
    }
    return val;
}

void LDBStorage::remove(uint64_t s, uint64_t e) {
    for (uint64_t i = s; i < e; i++) {
        uint64_t key = boost::endian::native_to_big(i);
        m_batch.Delete(leveldb::Slice((const char*)&key, sizeof(key)));
    }
}

const Entry &LDBStorage::operator[](size_t i) {
    get_entry(i, m_entry_tmp);
    return m_entry_tmp;
}

uint64_t LDBStorage::firstIndex() {
    return m_lastCompactIndex + 1;
}

uint64_t LDBStorage::lastIndex() {
    return m_lastCompactIndex + m_count - 1;
}
