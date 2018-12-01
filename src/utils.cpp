#include <raft/raft.hpp>
#include "utils.hpp"

using namespace std;
using namespace raftpb;

namespace raft {
	const char *error_string(error_code c) {
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
}
