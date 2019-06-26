#include <lua.hpp>
#include <raft/rawnode.hpp>
#include "lutils.hpp"
#include "lstorage.hpp"

using namespace raft;

class LRawNode : public RawNode {
public:
    LRawNode(lua_State *L): m_l(L), m_step_ref(0) {}
    ~LRawNode() {
        if (m_step_ref > 0) {
            luaL_unref(m_l, LUA_REGISTRYINDEX, m_step_ref);
            m_step_ref = 0;
        }
    }
  
    lua_State *m_l;
    int m_step_ref;
};

static int lrawnode(lua_State *L) {
    auto node = lua_newuserdata(L, sizeof(LRawNode));
    new (node) LRawNode(L);
    luaL_getmetatable(L, MT_RAWNODE);
    lua_setmetatable(L, -2);
    return 1;
}

static int lreadstate(lua_State *L) {
    auto s = (ReadState *)lua_newuserdata(L, sizeof(ReadState));
    new (s) ReadState();
    auto n = lua_gettop(L);
    if (n >= 1) {
        s->Index = luaL_checkinteger(L, 1);
        if (n >= 2) {
            s->RequestCtx = luaL_checkstring(L, 2);
        }
    }
    luaL_getmetatable(L, MT_READSTATE);
    lua_setmetatable(L, -2);
    return 1;
}

static int lrawnode_init(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    Config *cfg = (Config *)luaL_checkudata(L, 2, MT_CONFIG);
    try {
        Config new_cfg = *cfg;
        vector<Peer> peers;
        if (lua_istable(L, 3)) {
            lua_pushnil(L);
            Peer *peer = nullptr;
            while (lua_next(L, 3) != 0) {
                if (lua_isinteger(L, -1)) {
                    peers.emplace_back(Peer{ (uint64_t)lua_tointeger(L, -1) });
                } else if (peer = (Peer *)luaL_testudata(L, -1, MT_PEER), peer) {
                    peers.push_back(*peer);
                } else {
                    BOOST_THROW_EXCEPTION(std::runtime_error("peers must be integer or Peer type"));
                }
                lua_pop(L, 1);
            }
        }
        node->Init(std::move(new_cfg), peers);
    } catch (const std::exception &e) {
        luaL_error(L, e.what());
    }
    return 0;
}

static int lrawnode_ready(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    auto ready = node->Ready();
    auto r = lua_newuserdata(L, sizeof(Ready));
    new (r) Ready(std::move(ready));
    luaL_getmetatable(L, MT_READY);
    lua_setmetatable(L, -2);
    return 1;
}

static int lrawnode_has_ready(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    lua_pushboolean(L, node->HasReady());
    return 1;
}

static int lrawnode_step(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    auto msg = (Message *)luaL_checkudata(L, 2, MT_MESSAGE);
    lua_pushinteger(L, node->Step(*msg));
    return 1;
}

static int lrawnode_advance(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    auto rd = (Ready *)luaL_checkudata(L, 2, MT_READY);
    node->Advance(*rd);
    return 0;
}

static int lrawnode_campaign(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    node->Campaign();
    return 0;
}

static int lrawnode_propose(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    lua_pushinteger(L, node->Propose(luaL_checkstring(L, 2)));
    return 1;
}

static int lrawnode_propose_confchange(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    auto r = (ConfChange *)luaL_checkudata(L, 2, MT_CONFCHANGE);
    lua_pushinteger(L, node->ProposeConfChange(*r));
    return 1;
}

static int lrawnode_apply_confchange(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    auto r = (ConfChange *)luaL_checkudata(L, 2, MT_CONFCHANGE);
    node->ApplyConfChange(*r);
    return 0;
}

static int lrawnode_readindex(lua_State *L) {
    RawNode *node = (RawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    size_t l = 0;
    auto p = luaL_checklstring(L, 2, &l);
    node->ReadIndex(std::string(p, l));
    return 0;
}

static int lrawnode_id(lua_State *L, void *v) {
    RawNode *node = (RawNode *)v;
    lua_pushinteger(L, node->raft->id);
    return 1;
}

static int lrawnode_uncommittedSize(lua_State *L, void *v) {
    RawNode *node = (RawNode *)v;
    lua_pushinteger(L, node->raft->uncommittedSize);
    return 1;
}

static int lrawnode_read_states(lua_State *L, void *v) {
    RawNode *node = (RawNode *)v;
    lua_pushlightuserdata(L, &node->raft->readStates);
    luaL_getmetatable(L, MT_READSTATEVEC);
    lua_setmetatable(L, -2);
    return 1;
}

static int lrawnode_set_step(lua_State *L, void *v) {
    LRawNode *node = (LRawNode *)v;
    if (!lua_isfunction(L, 3)) {
        luaL_error(L, "invalid step function");
    }
    if (node->m_step_ref > 0) {
        luaL_unref(L, LUA_REGISTRYINDEX, node->m_step_ref);
        node->m_step_ref = 0;
    }
    lua_pushvalue(L, 3);
    node->m_step_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    node->raft->step = [node, L](Raft *r, Message &msg) {
        lua_rawgeti(L, LUA_REGISTRYINDEX, node->m_step_ref);
        lua_pushlightuserdata(L, &msg);
        luaL_getmetatable(L, MT_MESSAGE);
        lua_setmetatable(L, -2);
        if (lua_pcall(L, 1, 1, 0) != 0) {
            std::string s = lua_tostring(L, -1);
            lua_pop(L, 1);
            BOOST_THROW_EXCEPTION(std::runtime_error(s));
        }
        auto err = luaL_checkinteger(L, -1);
        lua_pop(L, 1);
        return (ErrorCode)err;
    };
    return 1;
}

static int lrawnode_set_read_states(lua_State *L, void *v) {
    RawNode *node = (RawNode *)v;
    if (lua_istable(L, 3)) {
        node->raft->readStates.clear();
        lua_pushnil(L);
        while (lua_next(L, 3) != 0) {
            auto s = (ReadState *)luaL_checkudata(L, -1, MT_READSTATE);
            node->raft->readStates.emplace_back(*s);
            lua_pop(L, 1);
        }
    } else if (lua_isuserdata(L, 3)) {
        node->raft->readStates.clear();
        auto states = (vector<ReadState> *)luaL_checkudata(L, 3, MT_READSTATEVEC);
        for (auto &s : *states) {
            node->raft->readStates.emplace_back(s);
        }
    } else {
        luaL_error(L, "invalid read states");
    }
    return 1;
}

static int lrawnode_delete(lua_State *L) {
    LRawNode *node = (LRawNode *)luaL_checkudata(L, 1, MT_RAWNODE);
    node->~LRawNode();
    return 0;
}

static int lconfig(lua_State *L) {
    auto cfg = lua_newuserdata(L, sizeof(Config));
    new (cfg) Config();
    luaL_getmetatable(L, MT_CONFIG);
    lua_setmetatable(L, -2);
    return 1;
}

static int lconfig_delete(lua_State *L) {
    Config *cfg = (Config *)luaL_checkudata(L, 1, MT_CONFIG);
    cfg->~Config();
    return 0;
}

//////////////////////////////////////////////////////////////////////////
// memorystorage
static int lmemorystorage(lua_State *L) {
    auto s = lua_newuserdata(L, sizeof(StoragePtr));
    new (s) StoragePtr(new MemoryStorage());
    luaL_getmetatable(L, MT_MEMORYSTORAGE);
    lua_setmetatable(L, -2);
    return 1;
}

static int lmemorystorage_delete(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    s->~StoragePtr();
    return 0;
}

static int lmemorystorage_append(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    if (lua_istable(L, 2)) {
        std::vector<Entry> ents;
        ents.reserve(luaL_len(L, 2));
        lua_pushnil(L);
        while (lua_next(L, 2) != 0) {
            auto entry = (Entry *)luaL_checkudata(L, -1, MT_ENTRY);
            ents.push_back(*entry);
            lua_pop(L, 1);
        }
        ((MemoryStorage *)s->get())->Append(ents);
    } else {
        IEntrySlice *slice;
        auto pp = (IEntrySlicePtr *)luaL_testudata(L, 2, MT_SLICE_PTR);
        if (!pp) {
            slice = (IEntrySlice *)luaL_checkudata(L, 2, MT_SLICE);
        } else {
            slice = pp->get();
        }
        ((MemoryStorage *)s->get())->Append(*slice);
    }
    return 0;
}

static int lmemorystorage_apply_snapshot(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    auto snapshot = (Snapshot *)luaL_checkudata(L, 2, MT_SNAPSHOT);
    lua_pushinteger(L, ((MemoryStorage *)s->get())->ApplySnapshot(*snapshot));
    return 1;
}

static int lmemorystorage_firstindex(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    auto result = (*s)->FirstIndex();
    lua_pushinteger(L, result.err);
    lua_pushinteger(L, result.value);
    return 2;
}

static int lmemorystorage_lastindex(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    auto result = (*s)->LastIndex();
    lua_pushinteger(L, result.err);
    lua_pushinteger(L, result.value);
    return 2;
}

static int lmemorystorage_term(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_LDBSTORAGE);
    auto result = ((LDBStorage *)s->get())->Term(luaL_checkinteger(L, 2));
    lua_pushinteger(L, result.err);
    lua_pushinteger(L, result.value);
    return 2;
}

static int lmemorystorage_entries(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    auto lo = luaL_checkinteger(L, 2);
    auto hi = luaL_checkinteger(L, 3);
    auto max_size = lua_isnil(L, 4) ? (lua_Integer)noLimit : luaL_checkinteger(L, 4);
    auto result = (*s)->Entries(lo, hi, max_size);
    lua_pushinteger(L, result.err);
    auto slice = (IEntrySlicePtr *)lua_newuserdata(L, sizeof(IEntrySlicePtr));
    new (slice) IEntrySlicePtr(std::move(result.value));
    luaL_getmetatable(L, MT_SLICE_PTR);
    lua_setmetatable(L, -2);
    return 2;
}

int lmemorystorage_set_hardstate_(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_MEMORYSTORAGE);
    auto h = (HardState *)luaL_checkudata(L, 2, MT_HARDSTATE);
    ((MemoryStorage *)s->get())->hard_state = *h;
    return 0;
}

int lmemorystorage_set_hardstate(lua_State *L, void *v) {
    StoragePtr *s = (StoragePtr *)v;
    auto h = (HardState *)luaL_checkudata(L, 3, MT_HARDSTATE);
    ((MemoryStorage *)s->get())->hard_state = *h;
    return 0;
}

int lmemorystorage_hardstate(lua_State *L, void *v) {
    StoragePtr *s = (StoragePtr *)v;
    lua_pushlightuserdata(L, &((MemoryStorage *)s->get())->hard_state);
    luaL_getmetatable(L, MT_HARDSTATE);
    lua_setmetatable(L, -2);
    return 1;
}

//////////////////////////////////////////////////////////////////////////
// ldb storage
static int lldbstorage(lua_State *L) {
    if (!lua_istable(L, 1)) {
        luaL_error(L, "ldb extends class expected");
    }
    auto s = (StoragePtr *)lua_newuserdata(L, sizeof(StoragePtr));
    new (s) StoragePtr(new LDBStorage(L));
    luaL_getmetatable(L, MT_LDBSTORAGE);
    lua_setmetatable(L, -2);
    ((LDBStorage *)s->get())->init(1);
    return 1;
}

static int lldbstorage_delete(lua_State *L) {
    StoragePtr *s = (StoragePtr *)luaL_checkudata(L, 1, MT_LDBSTORAGE);
    s->~StoragePtr();
    return 0;
}

static int lislocalmsg(lua_State *L) {
    lua_pushboolean(L, IsLocalMsg((MessageType)luaL_checkinteger(L, 1)));
    return 1;
}

static int lIsEmptyHardState(lua_State *L) {
    lua_pushboolean(L, lua_isnil(L, 1) || IsEmptyHardState(*(HardState *)luaL_checkudata(L, 1, MT_HARDSTATE)));
    return 1;
}

static const luaL_Reg rawnode_m[] = {
    {"__gc", lrawnode_delete},
    {"init", lrawnode_init},
    {"ready", lrawnode_ready},
    {"step", lrawnode_step},
    {"advance", lrawnode_advance},
    {"campaign", lrawnode_campaign},
    {"propose", lrawnode_propose},
    {"propose_confchange", lrawnode_propose_confchange},
    {"apply_confchange", lrawnode_apply_confchange},
    {"readindex", lrawnode_readindex},
    {"has_ready", lrawnode_has_ready},
    {NULL, NULL}
};

static const Xet_reg_pre rawnode_getsets[] = {
    {"id", lrawnode_id, nullptr, 0},
    {"uncommitted_size", lrawnode_uncommittedSize, nullptr, 0},
    {"read_states", lrawnode_read_states, lrawnode_set_read_states, 0},
    {"step_func", nullptr, lrawnode_set_step, 0},
    {NULL}
};

static const luaL_Reg config_m[] = {
    {"__gc", lconfig_delete},
    {NULL, NULL}
};

int get_storage(lua_State *L, void *v) {
    auto vv = (StoragePtr*)v;
    const char *mt = nullptr;
    if (dynamic_cast<MemoryStorage*>(vv->get())) {
        mt = MT_MEMORYSTORAGE;
    } else if (dynamic_cast<LDBStorage*>(vv->get())) {
        mt = MT_LDBSTORAGE;
    } else {
        luaL_error(L, "invalid storage type");
    }
    StoragePtr *s = (StoragePtr *)lua_newuserdata(L, sizeof(StoragePtr));
    new (s) StoragePtr(*vv);
    luaL_getmetatable(L, mt);
    lua_setmetatable(L, -2);
    return 1;
}

int set_storage(lua_State *L, void *v) {
    if (!luaL_testudata(L, 3, MT_MEMORYSTORAGE) && !luaL_testudata(L, 3, MT_LDBSTORAGE)) {
        luaL_error(L, "invalid storage type");
    }
    auto s = (StoragePtr *)lua_touserdata(L, 3);
    *(StoragePtr *)v = *s;
    return 0;
}

static const Xet_reg_pre config_getsets[] = {
    {"ID", get_int<uint64_t>, set_int<uint64_t>, offsetof(Config, ID)},
    {"peers", get_vec_uint64, set_vec_uint64, offsetof(Config, peers)},
    {"learners", get_vec_uint64, set_vec_uint64, offsetof(Config, learners)},
    {"ElectionTick", get_int<int>, set_int<int>, offsetof(Config, ElectionTick)},
    {"HeartbeatTick", get_int<int>, set_int<int>, offsetof(Config, HeartbeatTick)},
    {"Applied", get_int<uint64_t>, set_int<uint64_t>, offsetof(Config, Applied)},
    {"MaxSizePerMsg", get_int<uint64_t>, set_int<uint64_t>, offsetof(Config, MaxSizePerMsg)},
    {"MaxCommittedSizePerReady", get_int<uint64_t>, set_int<uint64_t>, offsetof(Config, MaxCommittedSizePerReady)},
    {"MaxUncommittedEntriesSize", get_int<uint64_t>, set_int<uint64_t>, offsetof(Config, MaxUncommittedEntriesSize)},
    {"MaxInflightMsgs", get_int<int>, set_int<int>, offsetof(Config, MaxInflightMsgs)},
    {"CheckQuorum", get_bool, set_bool, offsetof(Config, CheckQuorum)},
    {"PreVote", get_bool, set_bool, offsetof(Config, PreVote)},
    {"Storage", get_storage, set_storage, offsetof(Config, Storage)},
    {"ReadOnlyOption", get_int<ReadOnlyOption>, set_int<ReadOnlyOption>, offsetof(Config, ReadOnlyOption)},
    {"DisableProposalForwarding", get_bool, set_bool, offsetof(Config, DisableProposalForwarding)},
    {NULL}
};

static const luaL_Reg peer_m[] = {
    {NULL, NULL}
};

static const Xet_reg_pre peer_getsets[] = {
    {"ID", get_int<uint64_t>, set_int<uint64_t>, offsetof(Peer, ID)},
    {"Context", get_string, set_string, offsetof(Peer, Context)},
    {NULL}
};

static const luaL_Reg memorystorage_m[] = {
    {"__gc", lmemorystorage_delete},
    {"append", lmemorystorage_append},
    {"apply_snapshot", lmemorystorage_apply_snapshot},
    {"LastIndex", lmemorystorage_lastindex},
    {"FirstIndex", lmemorystorage_firstindex},
    {"Term", lmemorystorage_term},
    {"Entries", lmemorystorage_entries},
    {"set_hardstate", lmemorystorage_set_hardstate_},
    {NULL, NULL}
};

static const Xet_reg_pre memorystorage_getsets[] = {
    {"hardstate", lmemorystorage_hardstate, lmemorystorage_set_hardstate, 0},
    {NULL}
};

static const luaL_Reg ldbstorage_m[] = {
    {"__gc", lldbstorage_delete},
    {NULL, NULL}
};

int lslice_size(lua_State *L) {
    IEntrySlice *slice = (IEntrySlice *)luaL_checkudata(L, 1, MT_SLICE);
    lua_pushinteger(L, slice->size());
    return 1;
}

int lslice_empty(lua_State *L) {
    IEntrySlice *slice = (IEntrySlice *)luaL_checkudata(L, 1, MT_SLICE);
    lua_pushboolean(L, slice->size() == 0);
    return 1;
}

int lslice_at(lua_State *L) {
    IEntrySlice *slice = (IEntrySlice *)luaL_checkudata(L, 1, MT_SLICE);
    lua_pushlightuserdata(L, &slice->at(luaL_checkinteger(L, 2) - 1));
    luaL_getmetatable(L, MT_ENTRY);
    lua_setmetatable(L, -2);
    return 1;
}

int lslice_ipairsaux(lua_State *L) {
    auto slice = (IEntrySlice *)lua_touserdata(L, 1);
    lua_Integer i = luaL_checkinteger(L, 2) + 1;
    lua_pushinteger(L, i);
    if (i <= (lua_Integer)slice->size()) {
        lua_pushlightuserdata(L, &slice->at(i - 1));
        luaL_getmetatable(L, MT_ENTRY);
        lua_setmetatable(L, -2);
        return 2;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

int lslice_ipairs(lua_State *L) {
    luaL_checkudata(L, 1, MT_SLICE);
    lua_pushcfunction(L, lslice_ipairsaux);  /* iteration function */
    lua_pushvalue(L, 1);  /* state */
    lua_pushinteger(L, 0);  /* initial value */
    return 3;
}

static const luaL_Reg slice_m[] = {
    {"size", lslice_size},
    {"__len", lslice_size},
    {"__array", lslice_at},
    {"empty", lslice_empty},
    {"at", lslice_at},
    {"ipairs", lslice_ipairs},
    {NULL, NULL}
};

static const Xet_reg_pre slice_getsets[] = {
    {NULL}
};

int lslice_ptr_size(lua_State *L) {
    IEntrySlicePtr *slice = (IEntrySlicePtr *)luaL_checkudata(L, 1, MT_SLICE_PTR);
    lua_pushinteger(L, (*slice)->size());
    return 1;
}

int lslice_ptr_empty(lua_State *L) {
    IEntrySlicePtr *slice = (IEntrySlicePtr *)luaL_checkudata(L, 1, MT_SLICE_PTR);
    lua_pushboolean(L, (*slice)->size() == 0);
    return 1;
}

int lslice_ptr_at(lua_State *L) {
    IEntrySlicePtr *slice = (IEntrySlicePtr *)luaL_checkudata(L, 1, MT_SLICE_PTR);
    lua_pushlightuserdata(L, &(*slice)->at(luaL_checkinteger(L, 2) - 1));
    luaL_getmetatable(L, MT_ENTRY);
    lua_setmetatable(L, -2);
    return 1;
}

int lslice_ptr_ipairsaux(lua_State *L) {
    auto slice = ((IEntrySlicePtr *)lua_touserdata(L, 1))->get();
    lua_Integer i = luaL_checkinteger(L, 2) + 1;
    lua_pushinteger(L, i);
    if (i <= (lua_Integer)slice->size()) {
        lua_pushlightuserdata(L, &slice->at(i - 1));
        luaL_getmetatable(L, MT_ENTRY);
        lua_setmetatable(L, -2);
        return 2;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

int lslice_ptr_ipairs(lua_State *L) {
    luaL_checkudata(L, 1, MT_SLICE_PTR);
    lua_pushcfunction(L, lslice_ptr_ipairsaux);  /* iteration function */
    lua_pushvalue(L, 1);  /* state */
    lua_pushinteger(L, 0);  /* initial value */
    return 3;
}

int lslice_ptr_delete(lua_State *L) {
    IEntrySlicePtr *slice = (IEntrySlicePtr *)luaL_checkudata(L, 1, MT_SLICE_PTR);
    slice->~IEntrySlicePtr();
    return 0;
}

static const luaL_Reg slice_ptr_m[] = {
    {"__gc", lslice_ptr_delete},
    {"__len", lslice_ptr_size},
    {"__array", lslice_ptr_at},
    {"size", lslice_ptr_size},
    {"empty", lslice_ptr_empty},
    {"at", lslice_ptr_at},
    {"ipairs", lslice_ptr_ipairs},
    {NULL, NULL}
};

static const Xet_reg_pre slice_ptr_getsets[] = {
    {NULL}
};

static int ldefault_log(lua_State *L) {
    lua_pushlightuserdata(L, &DefaultLogger::instance());
    luaL_getmetatable(L, MT_LOG);
    lua_setmetatable(L, -2);
    return 1;
}

static int llog_set_level(lua_State *L, void *v) {
    Logger *l = (Logger *)v;
    l->setLogLevel((raft::LogLevel)(int)luaL_checkinteger(L, 3));
    return 0;
}

static int llog_level(lua_State *L, void *v) {
    Logger *l = (Logger *)v;
    lua_pushinteger(L, l->getLogLevel());
    return 1;
}

static const luaL_Reg log_m[] = {
    {NULL, NULL}
};

static const Xet_reg_pre log_getsets[] = {
    {"level", llog_level, llog_set_level, 0},
    {NULL}
};

void regist_uint64(lua_State *L);
extern "C" LUA_API int luaopen_lraft(lua_State *L) {
    luaL_checkversion(L);
    luaL_Reg l[] = {
        { "rawnode", lrawnode },
        { "config", lconfig },
        { "memorystorage", lmemorystorage },
        { "ldbstorage", lldbstorage },
        { "IsLocalMsg", lislocalmsg },
        { "IsEmptyHardState", lIsEmptyHardState },
        { "readstate", lreadstate },
        { "default_logger", ldefault_log },
        { NULL, NULL },
    };
    luaL_newlib(L, l);
    init_metatable(L, MT_RAWNODE, rawnode_m, rawnode_getsets);
    init_metatable(L, MT_CONFIG, config_m, config_getsets);
    init_metatable(L, MT_PEER, peer_m, peer_getsets);
    init_metatable(L, MT_MEMORYSTORAGE, memorystorage_m, memorystorage_getsets);
    init_metatable(L, MT_LDBSTORAGE, ldbstorage_m);
    init_metatable(L, MT_SLICE, slice_m, slice_getsets);
    init_metatable(L, MT_SLICE_PTR, slice_ptr_m, slice_ptr_getsets);
    init_metatable(L, MT_LOG, log_m, log_getsets);
    regist_pb_class(L);
    regist_ready_class(L);
    regist_uint64(L);

    REG_ENUM(L, StateFollower);
    REG_ENUM(L, StateCandidate);
    REG_ENUM(L, StateLeader);
    REG_ENUM(L, StatePreCandidate);

    REG_ENUM(L, OK);
    REG_ENUM(L, ErrCompacted);
    REG_ENUM(L, ErrSnapOutOfDate);
    REG_ENUM(L, ErrUnavailable);
    REG_ENUM(L, ErrSnapshotTemporarilyUnavailable);
    REG_ENUM(L, ErrSeriaFail);
    REG_ENUM(L, ErrAppendOutOfData);
    REG_ENUM(L, ErrProposalDropped);
    REG_ENUM(L, ErrStepLocalMsg);
    REG_ENUM(L, ErrStepPeerNotFound);
    REG_ENUM(L, ErrFalse);

    REG_ENUM1(L, LogLevel::all, "LOG_ALL");
    REG_ENUM1(L, LogLevel::debug, "LOG_DEBUG");
    REG_ENUM1(L, LogLevel::info, "LOG_INFO");
    REG_ENUM1(L, LogLevel::warn, "LOG_WARN");
    REG_ENUM1(L, LogLevel::error, "LOG_ERROR");
    REG_ENUM1(L, LogLevel::fatal, "LOG_FATAL");
    REG_ENUM1(L, LogLevel::off, "LOG_OFF");

    lua_pushinteger(L, noLimit);
    lua_setfield(L, -2, "noLimit");
    lua_pushinteger(L, None);
    lua_setfield(L, -2, "None");
    return 1;
}
