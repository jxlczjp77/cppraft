#include <raft/Raft.hpp>
#include "utils.hpp"

using namespace std;
using namespace raftpb;

namespace raft {
	const char *error_string(ErrorCode c) {
		return "";
	}

	void limitSize(vector<Entry> &ents, uint64_t maxSize) {
		if (!ents.empty()) {
			size_t size = ents[0].ByteSize();
			size_t limit;
			for (limit = 1; limit < ents.size(); limit++) {
				size += ents[limit].ByteSize();
				if (uint64_t(size) > maxSize) {
					break;
				}
			}
			ents.erase(ents.begin() + limit, ents.end());
		}
	}

	bool isHardStateEqual(const HardState &a, const HardState &b) {
		return a.term() == b.term() && a.vote() == b.vote() && a.commit() == b.commit();
	}

	const HardState emptyState;

	// IsEmptyHardState returns true if the given HardState is empty.
	bool IsEmptyHardState(const HardState &st) {
		return isHardStateEqual(st, emptyState);
	}

	// IsEmptySnap returns true if the given Snapshot is empty.
	bool IsEmptySnap(const Snapshot &sp) {
		return sp.metadata().index() == 0;
	}

	// PayloadSize is the size of the payload of this Entry. Notably, it does not
	// depend on its Index or Term.
	size_t PayloadSize(const Entry &e) {
		return e.data().length();
	}
}
