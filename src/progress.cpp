#include "progress.hpp"
#include <algorithm>
#include <boost/format.hpp>

namespace raft {
	const char *ProgressStateTypeStr(ProgressStateType t) {
		static const char *prstmap[] = { "ProgressStateProbe", "ProgressStateReplicate", "ProgressStateSnapshot" };
		return prstmap[t];
	}

	void Progress::resetState(ProgressStateType state) {
		Paused = false;
		PendingSnapshot = 0;
		State = state;
		ins->reset();
	}

	void Progress::becomeProbe() {
		// If the original state is ProgressStateSnapshot, progress knows that
		// the pending snapshot has been sent to this peer successfully, then
		// probes from pendingSnapshot + 1.
		if (State == ProgressStateSnapshot) {
			auto pendingSnapshot = PendingSnapshot;
			resetState(ProgressStateProbe);
			Next = std::max(Match + 1, pendingSnapshot + 1);
		} else {
			resetState(ProgressStateProbe);
			Next = Match + 1;
		}
	}

	void Progress::becomeReplicate() {
		resetState(ProgressStateReplicate);
		Next = Match + 1;
	}

	void Progress::becomeSnapshot(uint64_t snapshoti) {
		resetState(ProgressStateSnapshot);
		PendingSnapshot = snapshoti;
	}

	bool Progress::maybeUpdate(uint64_t n) {
		bool updated = false;
		if (Match < n) {
			Match = n;
			updated = true;
			resume();
		}
		if (Next < n + 1) {
			Next = n + 1;
		}
		return updated;
	}

	void Progress::optimisticUpdate(uint64_t n) {
		Next = n + 1;
	}

	// maybeDecrTo returns false if the given to index comes from an out of order message.
	// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
	bool Progress::maybeDecrTo(uint64_t rejected, uint64_t last) {
		if (State == ProgressStateReplicate) {
			// the rejection must be stale if the progress has matched and "rejected"
			// is smaller than "match".
			if (rejected <= Match) {
				return false;
			}
			// directly decrease next to match + 1
			Next = Match + 1;
			return true;
		}

		// the rejection must be stale if "rejected" does not match next - 1
		if (Next - 1 != rejected) {
			return false;
		}

		Next = min(rejected, last + 1);
		if (Next < 1) {
			Next = 1;
		}
		resume();
		return true;
	}

	void Progress::pause() {
		Paused = true;
	}

	void Progress::resume() {
		Paused = false;
	}

	bool Progress::IsPaused() {
		switch (State) {
		case ProgressStateProbe:
			return Paused;
		case ProgressStateReplicate:
			return ins->full();
		case ProgressStateSnapshot:
			return true;
		default:
			BOOST_THROW_EXCEPTION(std::runtime_error("unexpected state"));
		}
	}

	void Progress::snapshotFailure() {
		PendingSnapshot = 0;
	}

	bool Progress::needSnapshotAbort() {
		return State == ProgressStateSnapshot && Match >= PendingSnapshot;
	}

	string Progress::to_string() {
		return (boost::format("next = %1%, match = %2%, state = %3%, waiting = %4%, pendingSnapshot = %5%") % Next % Match % State % IsPaused() % PendingSnapshot).str();
	}

	inflights::inflights(int s) : size(s), count(0), start(0) {}

	// add adds an inflight into inflights
	void inflights::add(uint64_t inflight) {
		if (full()) {
			BOOST_THROW_EXCEPTION(std::runtime_error("cannot add into a full inflights"));
		}
		int next = start + count;
		if (next >= size) {
			next -= size;
		}
		if (next >= buffer.size()) {
			growBuf();
		}
		buffer[next] = inflight;
		count++;
	}

	// grow the inflight buffer by doubling up to inflights.size. We grow on demand
	// instead of preallocating to inflights.size to handle systems which have
	// thousands of Raft groups per process.
	void inflights::growBuf() {
		size_t newSize = buffer.size() * 2;
		if (newSize == 0) {
			newSize = 1;
		} else if (newSize > size) {
			newSize = size;
		}
		buffer.reserve(newSize);
		buffer.resize(newSize);
	}

	// freeTo frees the inflights smaller or equal to the given `to` flight.
	void inflights::freeTo(uint64_t to) {
		if (count == 0 || to < buffer[start]) {
			// out of the left side of the window
			return;
		}

		int idx = start;
		int i;
		for (i = 0; i < count; i++) {
			if (to < buffer[idx]) { // found the first large inflight
				break;
			}

			// increase index and maybe rotate
			if (++idx >= size) {
				idx -= size;
			}
		}
		// free i inflights and set new start index
		count -= i;
		start = idx;
		if (count == 0) {
			// inflights is empty, reset the start index so that we don't grow the
			// buffer unnecessarily.
			start = 0;
		}
	}

	void inflights::freeFirstOne() {
		freeTo(buffer[start]);
	}

	bool inflights::full() {
		return count == size;
	}

	void inflights::reset() {
		count = 0;
		start = 0;
	}
} // namespace raft
