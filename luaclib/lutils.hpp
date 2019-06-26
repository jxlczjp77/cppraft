#pragma once
#include <lua.hpp>

#define MT_RAWNODE             "raft_rawnode"
#define MT_CONFIG              "raft_config"
#define MT_ENTRY               "raft_entry"
#define MT_SNAPSHOT            "raft_Snapshot"
#define MT_SNAPSHOT_METADATA   "raft_SnapshotMetadata"
#define MT_SOFTSTATE           "raft_SoftState"
#define MT_HARDSTATE           "raft_HardState"
#define MT_SLICE               "raft_slice"
#define MT_SLICE_PTR           "raft_slice_ptr"
#define MT_CONFSTATE           "raft_ConfState"
#define MT_MESSAGE             "raft_Message"
#define MT_MESSAGEVEC          "raft_Message_vec"
#define MT_MSG_ENTRIES         "raft_MessageEntries"
#define MT_PEER                "raft_peer"
#define MT_MEMORYSTORAGE       "raft_memorystorage"
#define MT_LDBSTORAGE          "raft_ldbstorage"
#define MT_READY               "raft_ready"
#define MT_READSTATE           "raft_readstate"
#define MT_READSTATEVEC        "raft_readstate_vec"
#define MT_CONFCHANGE          "raft_ConfChange"
#define MT_LOG                 "raft_Log"
#define MT_UINT64              "raft_uint64"

#define REG_ENUM(L, V) \
    lua_pushinteger(L, V);\
    lua_setfield(L, -2, #V)

#define REG_ENUM1(L, V, N) \
    lua_pushinteger(L, V);\
    lua_setfield(L, -2, N) 

void regist_pb_class(lua_State *L);
void regist_ready_class(lua_State *L);

typedef int(*Xet_func)(lua_State *L, void *v);

struct Xet_reg_pre {
    const char *name;
    Xet_func get_func;
    Xet_func set_func;
    size_t offset;
};

typedef const Xet_reg_pre *Xet_reg;

template<typename T> int get_int(lua_State *L, void *v) {
    lua_pushinteger(L, *(T *)v);
    return 1;
}

template<typename T> int set_int(lua_State *L, void *v) {
    *(T *)v = (T)luaL_checkinteger(L, 3);
    return 0;
}

int get_vec_uint64(lua_State *L, void *v);
int set_vec_uint64(lua_State *L, void *v);
int get_bool(lua_State *L, void *v);
int set_bool(lua_State *L, void *v);
int get_string(lua_State *L, void *v);
int set_string(lua_State *L, void *v);

void init_metatable(lua_State *L, const char *metatable_name, const luaL_Reg methods[], const Xet_reg_pre getsets[] = NULL);
