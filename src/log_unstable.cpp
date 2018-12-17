#include "log_unstable.hpp"
#include <raft/Raft.hpp>
#define DEFAULT_ENTRY_LEN 100

namespace raft {
	unstable::unstable() : snapshot(nullptr), entries(100) {
	}

	unstable::~unstable() {
	}

	unstable::unstable(unstable &&u)
		: snapshot(std::move(u.snapshot)), entries(std::move(u.entries)), offset(u.offset), logger(u.logger) {
		u.offset = 0;
		u.logger = nullptr;
	}

	bool unstable::maybeFirstIndex(uint64_t &i) const {
		if (snapshot) {
			i = snapshot->metadata().index() + 1;
			return true;
		}
		i = 0;
		return false;
	}

	bool unstable::maybeLastIndex(uint64_t &i) const {
		if (!entries.empty()) {
			i = offset + uint64_t(entries.size()) - 1;
			return true;
		}
		if (snapshot) {
			i = snapshot->metadata().index();
			return true;
		}
		i = 0;
		return false;
	}

	bool unstable::maybeTerm(uint64_t i, uint64_t &term) const {
		uint64_t last;
		if (i < offset) {
			if (snapshot && snapshot->metadata().index() == i) {
				term = snapshot->metadata().term();
				return true;
			}
		} else if (maybeLastIndex(last) && i <= last) {
			term = entries[i - offset].term();
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

		if (gt == t && i >= offset) {
			entries.erase(entries.begin(), entries.begin() + (i + 1 - offset));
			offset = i + 1;
		}
	}

	void unstable::stableSnapTo(uint64_t i) {
		if (snapshot && snapshot->metadata().index() == i) {
			snapshot.reset();
		}
	}

	void unstable::restore(const Snapshot &sh) {
		offset = sh.metadata().index() + 1;
		entries.clear();
		snapshot = std::make_unique<Snapshot>(sh);
	}

	void unstable::truncateAndAppend(const IEntrySlice &ents) {
		uint64_t after = ents[0].index();
		if (after == offset + uint64_t(entries.size())) {
		} else if (after <= offset) {
			iLog(logger, "replace the unstable entries from index %1%", after);
			entries.clear();
			offset = after;
		} else {
			iLog(logger, "truncate the unstable entries before index %1%", after);
			mustCheckOutOfBounds(offset, after);
			entries.erase(entries.begin() + (after - offset), entries.end());
		}
		if (entries.capacity() < ents.size() + entries.size()) {
			entries.rset_capacity(entries.capacity() * 2);
		}
		entries.insert(entries.end(), ents.begin(), ents.end());
	}

	void unstable::slice(uint64_t lo, uint64_t hi, vector<Entry> &out) {
		mustCheckOutOfBounds(lo, hi);
		out.insert(out.end(), entries.begin() + (lo - offset), entries.begin() + (hi - offset));
	}

	void unstable::mustCheckOutOfBounds(uint64_t lo, uint64_t hi) {
		if (lo > hi) {
			fLog(logger, "invalid unstable.slice %1% > %2%", lo, hi);
		}

		uint64_t upper = offset + uint64_t(entries.size());
		if (lo < offset || hi > upper) {
			fLog(logger, "unstable.slice[%1%,%2%) out of bound [%3%,%4%]", lo, hi, offset, upper);
		}
	}
} // namespace raft
