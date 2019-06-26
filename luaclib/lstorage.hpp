#pragma once
#include <raft/storage.hpp>
#include <lua.hpp>
using namespace raft;

class LDBStorage : public Storage {
    enum {
        FUNC_InitialState = 0,
        FUNC_Entries,
        FUNC_Term,
        FUNC_LastIndex,
        FUNC_FirstIndex,
        FUNC_Snapshot,
        FUNC_COUNT
    };
public:
    LDBStorage(lua_State *L);
    ~LDBStorage();

    void init(int idx);

    virtual ErrorCode InitialState(HardState &hs, ConfState &cs);
    virtual Result<IEntrySlicePtr> Entries(uint64_t lo, uint64_t hi, uint64_t max_size);
    virtual Result<uint64_t> Term(uint64_t i);
    virtual Result<uint64_t> LastIndex();
    virtual Result<uint64_t> FirstIndex();
    virtual Result<raftpb::Snapshot*> Snapshot();

private:
    lua_State *m_l;
    EntryVec m_ents;
    int m_ref;
    int m_funcs[FUNC_COUNT];
};
