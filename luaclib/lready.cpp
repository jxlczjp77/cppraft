#include "lutils.hpp"
#include <raft/node.hpp>
using namespace raft;

int lready_softstate(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    if (r->SoftState) {
        lua_createtable(L, 0, 2);
        lua_pushinteger(L, r->SoftState->Lead);
        lua_setfield(L, -2, "Lead");
        lua_pushinteger(L, r->SoftState->RaftState);
        lua_setfield(L, -2, "RaftState");
    } else {
        lua_pushnil(L);
    }
    return 1;
}

int lready_hardstate(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    if (r->HardState) {
        lua_pushlightuserdata(L, &r->HardState.value());
        luaL_getmetatable(L, MT_HARDSTATE);
        lua_setmetatable(L, -2);
    } else {
        lua_pushnil(L);
    }
    return 1;
}

int lready_entries(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    lua_pushlightuserdata(L, &r->Entries);
    luaL_getmetatable(L, MT_SLICE);
    lua_setmetatable(L, -2);
    return 1;
}

int lready_committedEntries(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    lua_pushlightuserdata(L, &r->CommittedEntries);
    luaL_getmetatable(L, MT_SLICE);
    lua_setmetatable(L, -2);
    return 1;
}

int lready_readstates(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    lua_pushlightuserdata(L, &r->ReadStates);
    luaL_getmetatable(L, MT_READSTATEVEC);
    lua_setmetatable(L, -2);
    return 1;
}

int lreadstate_vec_ipairsaux(lua_State *L) {
    auto state = (vector<ReadState> *)lua_touserdata(L, 1);
    lua_Integer i = luaL_checkinteger(L, 2) + 1;
    lua_pushinteger(L, i);
    if (i <= (lua_Integer)state->size()) {
        lua_pushlightuserdata(L, &state->at(i - 1));
        luaL_getmetatable(L, MT_READSTATE);
        lua_setmetatable(L, -2);
        return 2;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

int lreadstate_vec_ipairs(lua_State *L) {
    luaL_checkudata(L, 1, MT_READSTATEVEC);
    lua_pushcfunction(L, lreadstate_vec_ipairsaux);  /* iteration function */
    lua_pushvalue(L, 1);  /* state */
    lua_pushinteger(L, 0);  /* initial value */
    return 3;
}

int lreadstate_vec_size(lua_State *L) {
    auto state = (vector<ReadState> *)luaL_checkudata(L, 1, MT_READSTATEVEC);
    lua_pushinteger(L, state->size());
    return 1;
}

int lreadstate_vec_empty(lua_State *L) {
    auto state = (vector<ReadState> *)luaL_checkudata(L, 1, MT_READSTATEVEC);
    lua_pushboolean(L, state->empty());
    return 1;
}

int lready_delete(lua_State *L) {
    Ready *r = (Ready *)luaL_checkudata(L, 1, MT_READY);
    r->~Ready();
    return 0;
}

int lready_snapshot(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    lua_pushlightuserdata(L, &r->Snapshot);
    luaL_getmetatable(L, MT_SNAPSHOT);
    lua_setmetatable(L, -2);
    return 1;
}

int lready_messages(lua_State *L, void *v) {
    Ready *r = (Ready *)v;
    lua_pushlightuserdata(L, &r->Messages);
    luaL_getmetatable(L, MT_MESSAGEVEC);
    lua_setmetatable(L, -2);
    return 1;
}

static const Xet_reg_pre ready_getsets[] = {
    {"softstate", lready_softstate , nullptr, 0},
    {"hardstate", lready_hardstate, nullptr, 0},
    {"entries", lready_entries, nullptr, 0},
    {"committed_entries", lready_committedEntries, nullptr, 0},
    {"readstates", lready_readstates, nullptr, 0},
    {"snapshot", lready_snapshot, nullptr, 0},
    {"messages", lready_messages, nullptr, 0},
    {"mustsync", get_bool, nullptr, offsetof(Ready, MustSync)},
    {NULL, NULL}
};

static const luaL_Reg ready_m[] = {
    {"__gc", lready_delete},
    {NULL, NULL}
};

static const luaL_Reg readstate_vec_m[] = {
    {"size", lreadstate_vec_size},
    {"empty", lreadstate_vec_empty},
    {"ipairs", lreadstate_vec_ipairs},
    {NULL, NULL}
};


int lmessages_vec_ipairsaux(lua_State *L) {
    auto msgs = (vector<MessagePtr> *)lua_touserdata(L, 1);
    lua_Integer i = luaL_checkinteger(L, 2) + 1;
    lua_pushinteger(L, i);
    if (i <= (lua_Integer)msgs->size()) {
        lua_pushlightuserdata(L, msgs->at(i - 1).get());
        luaL_getmetatable(L, MT_MESSAGE);
        lua_setmetatable(L, -2);
        return 2;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

int lmessages_vec_ipairs(lua_State *L) {
    luaL_checkudata(L, 1, MT_MESSAGEVEC);
    lua_pushcfunction(L, lmessages_vec_ipairsaux);  /* iteration function */
    lua_pushvalue(L, 1);  /* state */
    lua_pushinteger(L, 0);  /* initial value */
    return 3;
}

int lmessages_vec_size(lua_State *L) {
    auto msgs = (vector<MessagePtr> *)luaL_checkudata(L, 1, MT_MESSAGEVEC);
    lua_pushinteger(L, msgs->size());
    return 1;
}

static const luaL_Reg messages_vec_m[] = {
    {"size", lmessages_vec_size},
    {"ipairs", lmessages_vec_ipairs},
    {NULL, NULL}
};

int lreadstate_index(lua_State *L, void *v) {
    auto state = (ReadState *)v;
    lua_pushinteger(L, state->Index);
    return 1;
}

int lreadstate_set_index(lua_State *L, void *v) {
    auto state = (ReadState *)v;
    state->Index = luaL_checkinteger(L, 3);
    return 0;
}

int lreadstate_request_ctx(lua_State *L, void *v) {
    auto state = (ReadState *)v;
    lua_pushlstring(L, state->RequestCtx.c_str(), state->RequestCtx.size());
    return 1;
}

int lreadstate_set_request_ctx(lua_State *L, void *v) {
    auto state = (ReadState *)v;
    state->RequestCtx = luaL_checkstring(L, 3);
    return 0;
}

int lreadstate_delete(lua_State *L) {
    ReadState *r = (ReadState *)luaL_checkudata(L, 1, MT_READSTATE);
    r->~ReadState();
    return 0;
}

static const luaL_Reg readstate_m[] = {
    {"__gc", lreadstate_delete},
    {NULL, NULL}
};

static const Xet_reg_pre readstate_getsets[] = {
    {"index", lreadstate_index , lreadstate_set_index, 0},
    {"ctx", lreadstate_request_ctx , lreadstate_set_request_ctx, 0},
    {NULL, NULL}
};

void regist_ready_class(lua_State *L) {
    init_metatable(L, MT_READY, ready_m, ready_getsets);
    init_metatable(L, MT_READSTATEVEC, readstate_vec_m);
    init_metatable(L, MT_MESSAGEVEC, messages_vec_m);
    init_metatable(L, MT_READSTATE, readstate_m, readstate_getsets);
}
