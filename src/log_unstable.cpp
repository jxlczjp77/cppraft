#include <raft/log_unstable.hpp>
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

	Result<uint64_t> unstable::maybeFirstIndex() const {
		if (snapshot) {
			return { snapshot->metadata().index() + 1 };
		}
		return { 0, ErrFalse };
	}

	Result<uint64_t> unstable::maybeLastIndex() const {
		if (!entries.empty()) {
			return { offset + uint64_t(entries.size()) - 1 };
		}
		if (snapshot) {
			return { snapshot->metadata().index() };
		}
		return { 0, ErrFalse };
	}

	Result<uint64_t> unstable::maybeTerm(uint64_t i) const {
		if (i < offset) {
			if (snapshot && snapshot->metadata().index() == i) {
				return { snapshot->metadata().term() };
			}
		} else {
			auto last = maybeLastIndex();
			if (last.Ok() && i <= last.value) {
				return { entries[i - offset].term() };
			}
		}
		return { 0, ErrFalse };
	}

	void unstable::stableTo(uint64_t i, uint64_t t) {
		auto gt = maybeTerm(i);
		if (!gt.Ok()) {
			return;
		}

		if (gt.value == t && i >= offset) {
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

	IEntrySlicePtr unstable::slice(uint64_t lo, uint64_t hi) {
		mustCheckOutOfBounds(lo, hi);
		return std::make_unique<EntrySlice<EntryUnstableVec>>(entries, lo - offset, hi - offset);
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
