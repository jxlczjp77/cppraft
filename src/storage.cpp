#include <raft/Storage.hpp>

namespace raft {
	MemoryStorage::MemoryStorage(const vector<Entry> &ents) : entries(ents) {
	}

	MemoryStorage::~MemoryStorage() {
	}

	ErrorCode MemoryStorage::InitialState(HardState &hs, ConfState &cs) {
		hs = hard_state;
		cs = snapshot.metadata().conf_state();
		return OK;
	}

	ErrorCode MemoryStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out) {
		uint64_t offset = entries[0].index();
		if (lo <= offset) {
			return ErrCompacted;
		} else if (hi > lastIndex() + 1) {
			return ErrUnavailable;
		}
		if (entries.size() == 1) { // 仅包含dumy entry
			return ErrUnavailable;
		}
		size_t i = lo - offset, end = hi - offset;
		size_t byteCount = entries[i].ByteSize();
		out.push_back(entries[i]);
		for (i++; i < end; i++) {
			byteCount += entries[i].ByteSize();
			if (byteCount > max_size) {
				break;
			}
			out.push_back(entries[i]);
		}
		return OK;
	}

	ErrorCode MemoryStorage::Term(uint64_t i, uint64_t &t) {
		t = 0;
		uint64_t offset = entries[0].index();
		if (i < offset) {
			return ErrCompacted;
		} else if (i - offset >= entries.size()) {
			return ErrUnavailable;
		}
		t = entries[i - offset].term();
		return OK;
	}

	ErrorCode MemoryStorage::LastIndex(uint64_t &i) {
		i = lastIndex();
		return OK;
	}

	ErrorCode MemoryStorage::FirstIndex(uint64_t &i) {
		i = firstIndex();
		return OK;
	}

	ErrorCode MemoryStorage::Snapshot(raftpb::Snapshot **sn) {
		*sn = &snapshot;
		return OK;
	}

	uint64_t MemoryStorage::firstIndex() {
		return entries[0].index() + 1;
	}

	uint64_t MemoryStorage::lastIndex() {
		return entries[0].index() + entries.size() - 1;
	}

	ErrorCode MemoryStorage::Append(const vector<Entry> &ents) {
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
		uint64_t offset = ents[start_pos].index() - entries[0].index();
		if (offset > entries.size()) {
			return ErrAppendOutOfData;
		} else if (offset < entries.size()) {
			entries.erase(entries.begin() + offset, entries.end());
		}
		for (auto it = ents.begin() + start_pos; it != ents.end(); ++it) {
			entries.push_back(*it);
		}
		return OK;
	}

	ErrorCode MemoryStorage::ApplySnapshot(const raftpb::Snapshot &sn) {
		uint64_t old_index = snapshot.metadata().index();
		uint64_t new_index = sn.metadata().index();
		if (new_index < old_index) {
			return ErrSnapOutOfDate;
		}
		snapshot.CopyFrom(sn);
		entries.clear();
		Entry dumy_entry;
		dumy_entry.set_index(new_index);
		dumy_entry.set_term(sn.metadata().term());
		entries.push_back(dumy_entry);
		return OK;
	}

	// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
	ErrorCode MemoryStorage::CreateSnapshot(uint64_t i, const ConfState *cs, const string &data, raftpb::Snapshot &sh) {
		if (i <= snapshot.metadata().index()) {
			sh.Clear();
			return ErrSnapOutOfDate;
		}

		auto offset = entries[0].index();
		if (i > lastIndex()) {
			fLog(&DefaultLogger::instance(), "snapshot %1% is out of bound lastindex(%2%)", i, lastIndex());
		}

		snapshot.mutable_metadata()->set_index(i);
		snapshot.mutable_metadata()->set_term(entries[i - offset].term());
		if (cs != nullptr) {
			*snapshot.mutable_metadata()->mutable_conf_state() = *cs;
		}
		snapshot.set_data(data);
		sh = snapshot;
		return OK;
	}


	// Compact discards all log entries prior to compactIndex.
	// It is the application's responsibility to not attempt to compact an index
	// greater than raftLog.applied.
	ErrorCode MemoryStorage::Compact(uint64_t compactIndex) {
		uint64_t offset = entries[0].index();
		if (compactIndex <= offset) {
			return ErrCompacted;
		}
		if (compactIndex > lastIndex()) {
			fLog(&DefaultLogger::instance(), "compact %1% is out of bound lastindex(%2%)", compactIndex, lastIndex());
		}

		uint64_t i = compactIndex - offset;
		vector<Entry> ents;
		ents.reserve(1 + entries.size() - i);
		ents.resize(1);
		ents[0].set_index(entries[i].index());
		ents[0].set_term(entries[i].term());
		ents.insert(ents.end(), entries.begin() + (i + 1), entries.end());
		entries.swap(ents);
		return OK;
	}

	// SetHardState saves the current HardState.
	ErrorCode MemoryStorage::SetHardState(const HardState &st) {
		hard_state = st;
		return OK;
	}
} // namespace raft
