#pragma once

#include <log_unstable.hpp>
#include <raft_log.hpp>
#include <raft/raft.hpp>

using namespace raft;
using namespace std;

typedef std::unique_ptr<Snapshot> SnapshotPtr;

Entry makeEntry(uint64_t index, uint64_t term);
SnapshotPtr makeSnapshot(uint64_t index, uint64_t term);
void equal_entrys(const vector<Entry> &left, const vector<Entry> &right);
unstable make_unstable(unique_ptr<Snapshot> &&snapshot, vector<Entry> &&entries, uint64_t offset, logger &logger);
