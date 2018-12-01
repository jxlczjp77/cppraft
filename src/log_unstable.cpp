#include "log_unstable.hpp"
#include <raft/raft.hpp>

namespace raft {
	unstable::unstable() : m_snapshot(nullptr) {
	} 

	unstable::~unstable() {
	}

	unstable::unstable(unstable &&u)
		: m_snapshot(std::move(u.m_snapshot))
		, m_entries(std::move(u.m_entries))
		, m_offset(u.m_offset)
		, m_logger(u.m_logger)
	{
		u.m_offset = 0;
		u.m_logger = nullptr;
	}

	bool unstable::maybeFirstIndex(uint64_t &i) const {
		if (m_snapshot) {
			i = m_snapshot->metadata().index() + 1;
			return true;
		}
		i = 0;
		return false;
	}

	bool unstable::maybeLastIndex(uint64_t &i) const {
		if (!m_entries.empty()) {
			i = m_offset + uint64_t(m_entries.size()) - 1;
			return true;
		}
		if (m_snapshot) {
			i = m_snapshot->metadata().index();
			return true;
		}
		i = 0;
		return false;
	}

	bool unstable::maybeTerm(uint64_t i, uint64_t &term) const {
		uint64_t last;
		if (i < m_offset) {
			if (m_snapshot && m_snapshot->metadata().index() == i) {
				term = m_snapshot->metadata().term();
				return true;
			}
		} else if (maybeLastIndex(last) && i <= last) {
			term = m_entries[i - m_offset].term();
			return true;
		}
		term = 0;
		return false;
	}

	void unstable::stableTo(uint64_t i, uint64_t t) {
		uint64_t gt;
		if (!maybeTerm(i, gt)) {
			return;
		}

		if (gt == t && i >= m_offset) {
			m_entries.erase(m_entries.begin(), m_entries.begin() + (i + 1 - m_offset));
			m_offset = i + 1;
		}
	}

	void unstable::stableSnapTo(uint64_t i) {
		if (m_snapshot && m_snapshot->metadata().index() == i) {
			m_snapshot.reset();
		}
	}

	void unstable::restore(const Snapshot &sh) {
		m_offset = sh.metadata().index() + 1;
		m_entries.clear();
		m_snapshot = std::make_unique<Snapshot>(sh);
	}

	void unstable::truncateAndAppend(const vector<Entry> &ents) {
		uint64_t after = ents[0].index();
		if (after == m_offset + uint64_t(m_entries.size())) {
			m_entries.insert(m_entries.end(), ents.begin(), ents.end());
		} else if (after <= m_offset) {
			iLog(m_logger, "replace the unstable entries from index %d", after);
			m_entries = std::move(ents);
			m_offset = after;
		} else {
			iLog(m_logger, "truncate the unstable entries before index %d", after);
			mustCheckOutOfBounds(m_offset, after);
			m_entries.erase(m_entries.begin() + (after - m_offset), m_entries.end());
			m_entries.insert(m_entries.end(), ents.begin(), ents.end());
		}
	}

	void unstable::slice(uint64_t lo, uint64_t hi, vector<Entry> &entries) {
		mustCheckOutOfBounds(lo, hi);
		entries.insert(entries.end(), m_entries.begin() + (lo - m_offset), m_entries.begin() + (hi - m_offset));
	}

	void unstable::mustCheckOutOfBounds(uint64_t lo, uint64_t hi) {
		if (lo > hi) {
			fLog(m_logger, "invalid unstable.slice %d > %d", lo, hi);
		}

		uint64_t upper = m_offset + uint64_t(m_entries.size());
		if (lo < m_offset || hi > upper) {
			fLog(m_logger, "unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, m_offset, upper);
		}
	}
}
