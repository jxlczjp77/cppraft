#include <raft/storage.hpp>

namespace raft {
	memory_storage::memory_storage() : m_entries{ Entry() } {

	}

	memory_storage::~memory_storage() {

	}

	error_code memory_storage::initial_state(HardState &hs, ConfState &cs) {
		hs = m_hard_state;
		cs = m_snapshot.metadata().conf_state();
		return OK;
	}

	error_code memory_storage::entries(uint64_t lo, uint64_t hi, uint64_t max_size, vector<Entry> &out) {
		uint64_t offset = m_entries[0].index();
		if (lo <= offset) {
			return ErrCompacted;
		} else if (hi > lastIndex() + 1) {
			return ErrUnavailable;
		}
		if (m_entries.size() == 1) { // 仅包含dumy entry
			return ErrUnavailable;
		}
		size_t byteCount = 0;
		for (size_t i = lo - offset, end = hi - offset; i < end; i++) {
			out.push_back(m_entries[i]);
			byteCount += m_entries[i].ByteSize();
			if (byteCount >= max_size) {
				break;
			}
		}
		return OK;
	}

	error_code memory_storage::term(uint64_t i, uint64_t &t) {
		t = 0;
		uint64_t offset = m_entries[0].index();
		if (i < offset) {
			return ErrCompacted;
		} else if (i - offset >= m_entries.size()) {
			return ErrUnavailable;
		}
		t = m_entries[i - offset].term();
		return OK;
	}

	error_code memory_storage::last_index(uint64_t &i) {
		i = lastIndex();
		return OK;
	}

	error_code memory_storage::first_index(uint64_t &i) {
		i = firstIndex();
		return OK;
	}

	error_code memory_storage::snapshot(Snapshot **sn) {
		*sn = &m_snapshot;
		return OK;
	}

	uint64_t memory_storage::firstIndex() {
		return m_entries[0].index() + 1;
	}

	uint64_t memory_storage::lastIndex() {
		return m_entries[0].index() + m_entries.size() - 1;
	}

	error_code memory_storage::append(const vector<Entry> &entries) {
		if (entries.empty()) {
			return OK;
		}

		uint64_t old_first = firstIndex();
		uint64_t new_first = entries[0].index();
		uint64_t new_last = new_first + entries.size() - 1;
		if (new_last < old_first) {
			return OK;
		}

		size_t start_pos = old_first > new_first ? size_t(old_first - new_first) : 0;
		uint64_t offset = entries[start_pos].index() - m_entries[0].index();
		if (offset > m_entries.size()) {
			return ErrAppendOutOfData;
		} else if (offset < m_entries.size()) {
			m_entries.erase(m_entries.begin() + offset, m_entries.end());
		}
		for (auto it = entries.begin() + start_pos; it != entries.end(); ++it) {
			m_entries.push_back(*it);
		}
		return OK;
	}

	error_code memory_storage::apply_snapshot(const Snapshot &snapshot) {
		uint64_t old_index = m_snapshot.metadata().index();
		uint64_t new_index = snapshot.metadata().index();
		if (new_index < old_index) {
			return ErrSnapOutOfDate;
		}
		m_snapshot.CopyFrom(snapshot);
		m_entries.clear();
		Entry dumy_entry;
		dumy_entry.set_index(new_index);
		dumy_entry.set_term(snapshot.metadata().term());
		m_entries.push_back(dumy_entry);
		return OK;
	}

	// Compact discards all log entries prior to compactIndex.
	// It is the application's responsibility to not attempt to compact an index
	// greater than raftLog.applied.
	error_code memory_storage::compact(uint64_t compactIndex) {
		uint64_t offset = m_entries[0].index();
		if (compactIndex <= offset) {
			return ErrCompacted;
		}
		if (compactIndex > lastIndex()) {
			fLog(&default_logger::instance(), "compact %d is out of bound lastindex(%d)", compactIndex, lastIndex());
		}

		uint64_t i = compactIndex - offset;
		vector<Entry> ents;
		ents.reserve(1 + m_entries.size() - i);
		ents.resize(1);
		ents[0].set_index(m_entries[i].index());
		ents[0].set_term(m_entries[i].term());
		ents.insert(ents.end(), m_entries.begin() + (i + 1), m_entries.end());
		m_entries.swap(ents);
		return OK;
	}
}
