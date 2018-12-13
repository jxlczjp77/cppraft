#include "raft_log.hpp"
#include "utils.hpp"
#include <boost/format.hpp>

namespace raft {
	raft_log::raft_log(StoragePtr storage_, Logger *logger_, uint64_t maxNextEntsSize_) {
		storage = storage_;
		logger = logger_;
		maxNextEntsSize = maxNextEntsSize_;
		uint64_t firstIndex, lastIndex;
		if (storage->FirstIndex(firstIndex) != OK) {
			abort();
		}
		if (storage->LastIndex(lastIndex) != OK) {
			abort();
		}
		unstable.offset = lastIndex + 1;
		unstable.logger = logger;
		committed = firstIndex - 1;
		applied = firstIndex - 1;
	}

	raft_log::~raft_log() {
	}

	// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
	// it returns (last index of new entries, true).
	bool raft_log::maybeAppend(uint64_t index, uint64_t logTerm, uint64_t committed, const vector<Entry> &ents, uint64_t &lastnewi) {
		if (matchTerm(index, logTerm)) {
			lastnewi = index + ents.size();
			uint64_t ci = findConflict(ents);
			if (ci == 0) {
			} else if (ci <= this->committed) {
				fLog(logger, "entry %1% conflict with committed entry [committed(%2%)]", ci, this->committed);
			} else {
				uint64_t offset = index + 1;
				append({ ents.begin() + (ci - offset), ents.end() });
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

	uint64_t raft_log::append(const vector<Entry> &ents) {
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
		uint64_t lt;
		if (!SUCCESS(term(i, lt))) {
			return false;
		}
		return lt == t;
	}

	ErrorCode raft_log::term(uint64_t i, uint64_t &t) {
		t = 0;
		uint64_t dummyIndex = firstIndex() - 1;
		if (i < dummyIndex || i > lastIndex() || unstable.maybeTerm(i, t)) {
			return OK;
		}

		return storage->Term(i, t);
	}

	uint64_t raft_log::firstIndex() {
		uint64_t firstIndex;
		if (unstable.maybeFirstIndex(firstIndex)) {
			return firstIndex;
		}
		if (storage->FirstIndex(firstIndex) == OK) {
			return firstIndex;
		}
		abort();
		return 0;
	}

	uint64_t raft_log::lastIndex() {
		uint64_t lastIndex;
		if (unstable.maybeLastIndex(lastIndex)) {
			return lastIndex;
		}
		if (storage->LastIndex(lastIndex) == OK) {
			return lastIndex;
		}
		abort();
		return 0;
	}

	uint64_t raft_log::findConflict(const vector<Entry> &ents) {
		for (const Entry &ne : ents) {
			uint64_t i = ne.index();
			uint64_t t = ne.term();
			if (!matchTerm(i, t)) {
				if (i <= lastIndex()) {
					uint64_t dummy;
					ErrorCode err = term(i, dummy);
					iLog(logger, "found conflict at index %1% [existing term: %2%, conflicting term: %3%]",
						i, zeroTermOnErrCompacted(dummy, err), t);
				}
				return ne.index();
			}
		}
		return 0;
	}

	uint64_t raft_log::zeroTermOnErrCompacted(uint64_t t, ErrorCode err) {
		if (SUCCESS(err)) {
			return t;
		}
		if (err == ErrCompacted) {
			return 0;
		}
		fLog(logger, "unexpected error: %1%", error_string(err));
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
		uint64_t t;
		ErrorCode err = term(lastIndex(), t);
		if (!SUCCESS(err)) {
			fLog(logger, "unexpected error when getting the last term (%1%)", err);
		}
		return t;
	}

	ErrorCode raft_log::entries(vector<Entry> &out, uint64_t i, uint64_t maxsize) {
		out.clear();
		uint64_t li = lastIndex();
		if (i > li) {
			return OK;
		}
		return slice(out, i, li + 1, maxsize);
	}

	// slice returns a slice of log entries from lo through hi-1, inclusive.
	ErrorCode raft_log::slice(vector<Entry> &out, uint64_t lo, uint64_t hi, uint64_t maxSize) {
		ErrorCode err = mustCheckOutOfBounds(lo, hi);
		if (!SUCCESS(err)) {
			return err;
		}
		if (lo == hi) {
			return OK;
		}
		if (lo < unstable.offset) {
			ErrorCode err = storage->Entries(lo, min(hi, unstable.offset), maxSize, out);
			if (err == ErrCompacted) {
				return err;
			} else if (err == ErrUnavailable) {
				fLog(logger, "entries[%1%:%2%) is unavailable from storage", lo, min(hi, unstable.offset));
			} else if (!SUCCESS(err)) {
				abort(); // TODO(bdarnell)
			}
			// check if ents has reached the size limitation
			if (uint64_t(out.size()) < min(hi, unstable.offset) - lo) {
				return OK;
			}
		}
		if (hi > unstable.offset) {
			unstable.slice(max(lo, unstable.offset), hi, out);
		}
		limitSize(out, maxSize);
		return OK;
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
			uint64_t t;
			ErrorCode err = this->term(maxIndex, t);
			if (zeroTermOnErrCompacted(t, err) == term) {
				commitTo(maxIndex);
				return true;
			}
		}
		return false;
	}

	const vector<Entry> &raft_log::unstableEntries() {
		return unstable.entries;
	}

	bool raft_log::hasNextEnts() {
		uint64_t off = max(applied + 1, firstIndex());
		return committed + 1 > off;
	}

	// nextEnts returns all the available entries for execution.
	// If applied is smaller than the index of snapshot, it returns all committed
	// entries after the index of snapshot.
	vector<Entry> raft_log::nextEnts() {
		vector<Entry> ents;
		uint64_t off = max(applied + 1, firstIndex());
		if (committed + 1 > off) {
			ErrorCode err = slice(ents, off, committed + 1, maxNextEntsSize);
			if (!SUCCESS(err)) {
				fLog(logger, "unexpected error when getting unapplied entries (%1%)", err);
			}
		}
		return std::move(ents);
	}

	// allEntries returns all entries in the log.
	vector<Entry> raft_log::allEntries() {
		vector<Entry> ents;
		ErrorCode err = entries(ents, firstIndex());
		if (SUCCESS(err)) {
			return std::move(ents);
		}
		if (err == ErrCompacted) { // try again if there was a racing compaction
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

	ErrorCode raft_log::snapshot(Snapshot **sn) {
		if (unstable.snapshot) {
			*sn = &*unstable.snapshot;
			return OK;
		}
		return storage->Snapshot(sn);
	}
} // namespace raft
