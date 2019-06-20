#include <raft/status.hpp>
#include <raft/raft.hpp>
#include <boost/format.hpp>

namespace raft {
	
	// getStatus gets a copy of the current raft status.
	Status getStatus(Raft *r) {
		Status s;
		s.ID = r->id;
		s.LeadTransferee = r->leadTransferee;

		s.HardState = r->hardState();
		s.SoftState = r->softState();

		s.Applied = r->raftLog->applied;

		if (s.SoftState.RaftState == StateLeader) {
			for (auto it = r->prs.begin(); it != r->prs.end(); it++) {
				s.Progress[it->first] = it->second.get();
			}

			for (auto it = r->learnerPrs.begin(); it != r->learnerPrs.end(); it++) {
				s.Progress[it->first] = it->second.get();
			}
		}

		return s;
	}

	// MarshalJSON translates the raft status into JSON.
	// TODO: try to simplify this by introducing ID type into raft
	string Status::ToJson() {
		auto j = (boost::format("{id\":\"%1%\",\"term\":%2%,\"vote\":\"%3%\",\"commit\":%4%,\"lead\":\"%5%\",\"raftState\":%6%,\"applied\":%7%,\"progress\":{")
			% ID % HardState.term() % HardState.vote() % HardState.commit() % SoftState.Lead % SoftState.RaftState % Applied).str();

		if (Progress.empty()) {
			j += "},";
		} else {
			for (auto it = Progress.begin(); it != Progress.end(); it++) {
				auto k = it->first;
				auto v = it->second;
				auto subj = (boost::format("\"%1%\":{\"match\":%2%,\"next\":%3%,\"state\":%4%},") % k % v->Match % v->Next % v->State).str();
				j += subj;
			}
			// remove the trailing ","
			j.pop_back();
			j += "},";
		}

		j += (boost::format("\"leadtransferee\":\"%x\"}") % LeadTransferee).str();
		return j;
	}
	string Status::ToString() { return ToJson(); }
}
