#include "raft_log.hpp"
#include "utils.hpp"
#include <boost/format.hpp>
#include <raft/entrys.hpp>

namespace raft {
	raft_log::raft_log(StoragePtr storage_, Logger *logger_, uint64_t maxNextEntsSize_) {
		storage = storage_;
		logger = logger_;
		maxNextEntsSize = maxNextEntsSize_;
		auto firstIndex = storage->FirstIndex();
		if (!firstIndex.Ok()) {
			abort();
		}
		auto lastIndex = storage->LastIndex();
		if (!lastIndex.Ok()) {
			abort();
		}
		unstable.offset = lastIndex.value + 1;
		unstable.logger = logger;
		committed = firstIndex.value - 1;
		applied = firstIndex.value - 1;
	}

	raft_log::~raft_log() {
	}

	// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
	// it returns (last index of new entries, true).
	bool raft_log::maybeAppend(uint64_t index, uint64_t logTerm, uint64_t committed, const IEntrySlice &ents, uint64_t &lastnewi) {
		if (matchTerm(index, logTerm)) {
			lastnewi = index + ents.size();
			uint64_t ci = findConflict(ents);
			if (ci == 0) {
			} else if (ci <= this->committed) {
				fLog(logger, "entry %1% conflict with committed entry [committed(%2%)]", ci, this->committed);
			} else {
				uint64_t offset = index + 1;
				append(make_slice(ents, ci - offset));
			}
			commitTo(min(committed, lastnewi));
			return true;
		}
		lastnewi = 0;
		return false;
	}

	void raft_log::commitTo(uint64_t tocommit) {
		if (committed < tocommit) {
			if (lastIndex() < tocommit) {
				fLog(logger, "tocommit(%1%) is out of range [lastIndex(%2%)]. Was the raft log corrupted, truncated, or lost?", tocommit, lastIndex());
			}
			committed = tocommit;
		}
	}

	void raft_log::appliedTo(uint64_t i) {
		if (i == 0) {
			return;
		}
		if (committed < i || i < applied) {
			fLog(logger, "applied(%1%) is out of range [prevApplied(%2%), committed(%3%)]", i, applied, committed);
		}
		applied = i;
	}

	void raft_log::stableTo(uint64_t i, uint64_t t) { unstable.stableTo(i, t); }

	void raft_log::stableSnapTo(uint64_t i) { unstable.stableSnapTo(i); }
	uint64_t raft_log::appendSlice(const IEntrySlice &ents) {
		if (ents.empty()) {
			return lastIndex();
		}
		uint64_t after = ents[0].index() - 1;
		if (after < committed) {
			fLog(logger, "after(%1%) is out of range [committed(%2%)]", after, committed);
		}
		unstable.truncateAndAppend(ents);
		return lastIndex();
	}

	bool raft_log::matchTerm(uint64_t i, uint64_t t) {
		auto lt = term(i);
		if (!lt.Ok()) {
			return false;
		}
		return lt.value == t;
	}

	Result<uint64_t> raft_log::term(uint64_t i) {
		uint64_t dummyIndex = firstIndex() - 1;
		if (i < dummyIndex || i > lastIndex()) {
			return { 0 };
		}
		auto r = unstable.maybeTerm(i);
		if (r.Ok()) {
			return r;
		}

		return storage->Term(i);
	}

	uint64_t raft_log::firstIndex() {
		auto firstIndex = unstable.maybeFirstIndex();
		if (firstIndex.Ok()) {
			return firstIndex.value;
		}
		firstIndex = storage->FirstIndex();
		if (firstIndex.Ok()) {
			return firstIndex.value;
		}
		abort();
		return 0;
	}

	uint64_t raft_log::lastIndex() {
		auto lastIndex = unstable.maybeLastIndex();
		if (lastIndex.Ok()) {
			return lastIndex.value;
		}
		lastIndex = storage->LastIndex();
		if (lastIndex.Ok()) {
			return lastIndex.value;
		}
		abort();
		return 0;
	}

	uint64_t raft_log::findConflict(const IEntrySlice &ents) {
		for (const Entry &ne : ents) {
			uint64_t i = ne.index();
			uint64_t t = ne.term();
			if (!matchTerm(i, t)) {
				if (i <= lastIndex()) {
					iLog(logger, "found conflict at index %1% [existing term: %2%, conflicting term: %3%]",
						i, zeroTermOnErrCompacted(term(i)), t);
				}
				return ne.index();
			}
		}
		return 0;
	}

	uint64_t raft_log::zeroTermOnErrCompacted(const Result<uint64_t> &t) {
		if (t.Ok()) {
			return t.value;
		}
		if (t.err == ErrCompacted) {
			return 0;
		}
		fLog(logger, "unexpected error: %1%", error_string(t.err));
		return 0;
	}

	// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
	// by comparing the index and term of the last entries in the existing logs.
	// If the logs have last entries with different terms, then the log with the
	// later term is more up-to-date. If the logs end with the same term, then
	// whichever log has the larger lastIndex is more up-to-date. If the logs are
	// the same, the given log is up-to-date.
	bool raft_log::isUpToDate(uint64_t lasti, uint64_t term) {
		uint64_t lastt = lastTerm();
		return term > lastt || (term == lastt && lasti >= lastIndex());
	}

	uint64_t raft_log::lastTerm() {
		auto t = term(lastIndex());
		if (!t.Ok()) {
			fLog(logger, "unexpected error when getting the last term (%1%)", t.err);
		}
		return t.value;
	}

	Result<EntryRange> raft_log::entries(uint64_t i, uint64_t maxsize) {
		uint64_t li = lastIndex();
		if (i > li) {
			return OK;
		}
		return slice(i, li + 1, maxsize);
	}

	// slice returns a slice of log entries from lo through hi-1, inclusive.
	Result<EntryRange> raft_log::slice(uint64_t lo, uint64_t hi, uint64_t maxSize) {
		ErrorCode err = mustCheckOutOfBounds(lo, hi);
		if (err != OK) {
			return err;
		}
		if (lo == hi) {
			return OK;
		}
		EntryRange ents;
		if (lo < unstable.offset) {
			auto storedEnts = storage->Entries(lo, min(hi, unstable.offset), maxSize);
			if (storedEnts.err == ErrCompacted) {
				return ErrCompacted;
			} else if (storedEnts.err == ErrUnavailable) {
				fLog(logger, "entries[%1%:%2%) is unavailable from storage", lo, min(hi, unstable.offset));
			} else if (!storedEnts.Ok()) {
				abort(); // TODO(bdarnell)
			}
			// check if ents has reached the size limitation
			if (uint64_t(storedEnts.value->size()) < min(hi, unstable.offset) - lo) {
				return { EntryRange(std::move(storedEnts.value)) };
			}
			ents.storage = std::move(storedEnts.value);
		}
		if (hi > unstable.offset) {
			ents.unstable = unstable.slice(max(lo, unstable.offset), hi);
		}
		limitSize(ents, maxSize);
		return { std::move(ents) };
	}

	// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
	ErrorCode raft_log::mustCheckOutOfBounds(uint64_t lo, uint64_t hi) {
		if (lo > hi) {
			fLog(logger, "invalid slice %1% > %2%", lo, hi);
		}
		uint64_t fi = firstIndex();
		if (lo < fi) {
			return ErrCompacted;
		}

		uint64_t length = lastIndex() + 1 - fi;
		if (lo < fi || hi > fi + length) {
			fLog(logger, "slice[%1%,%2%) out of bound [%3%,%4%]", lo, hi, fi, lastIndex());
		}
		return OK;
	}

	bool raft_log::maybeCommit(uint64_t maxIndex, uint64_t term) {
		if (maxIndex > committed) {
			if (zeroTermOnErrCompacted(this->term(maxIndex)) == term) {
				commitTo(maxIndex);
				return true;
			}
		}
		return false;
	}

	const EntryUnstableVec &raft_log::unstableEntries() {
		return unstable.entries;
	}

	bool raft_log::hasNextEnts() {
		uint64_t off = max(applied + 1, firstIndex());
		return committed + 1 > off;
	}

	// nextEnts returns all the available entries for execution.
	// If applied is smaller than the index of snapshot, it returns all committed
	// entries after the index of snapshot.
	EntryRange raft_log::nextEnts() {
		uint64_t off = max(applied + 1, firstIndex());
		if (committed + 1 > off) {
			auto r = std::move(slice(off, committed + 1, maxNextEntsSize));
			if (!r.Ok()) {
				fLog(logger, "unexpected error when getting unapplied entries (%1%)", r.err);
			}
			return std::move(r.value);
		}
		return EntryRange();
	}

	// allEntries returns all entries in the log.
	EntryRange raft_log::allEntries() {
		auto r = entries(firstIndex());
		if (r.Ok()) {
			return std::move(r.value);
		}
		if (r.err == ErrCompacted) { // try again if there was a racing compaction
			return std::move(allEntries());
		}
		// TODO (xiangli): handle error?
		abort();
	}

	void raft_log::restore(const Snapshot &s) {
		iLog(logger, "log [%1%] starts to restore snapshot [index: %2%, term: %3%]", to_string().c_str(), s.metadata().index(), s.metadata().index());
		committed = s.metadata().index();
		unstable.restore(s);
	}

	string raft_log::to_string() {
		return (boost::format("committed=%1%, applied=%2%, unstable.offset=%3%, len(unstable.Entries)=%4%") % committed % applied % unstable.offset % unstable.entries.size()).str();
	}

	Result<Snapshot*> raft_log::snapshot() {
		if (unstable.snapshot) {
			return { &*unstable.snapshot };
		}
		return storage->Snapshot();
	}
} // namespace raft
