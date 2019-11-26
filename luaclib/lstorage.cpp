﻿#include "lstorage.hpp"
#include "lutils.hpp"
#include <raft/logger.hpp>

LDBStorage::LDBStorage(lua_State *L) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, LUA_RIDX_MAINTHREAD);
    m_l = lua_tothread(L, -1);
    lua_pop(L, 1);
    memset(m_funcs, 0, sizeof(m_funcs));
}

LDBStorage::~LDBStorage() {
    luaL_unref(m_l, LUA_REGISTRYINDEX, m_ref);
    for (int i = 0; i < FUNC_COUNT; i++) {
        if (m_funcs[i]) {
            luaL_unref(m_l, LUA_REGISTRYINDEX, m_funcs[i]);
        }
    }
}

void LDBStorage::init(lua_State *L, int idx) {
    auto top = lua_gettop(L);
    lua_pushvalue(L, idx);
    m_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    if (lua_getfield(L, idx, "InitialState") != LUA_TNIL) {
        m_funcs[FUNC_InitialState] = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    if (lua_getfield(L, idx, "Entries") != LUA_TNIL) {
        m_funcs[FUNC_Entries] = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    if (lua_getfield(L, idx, "Term") != LUA_TNIL) {
        m_funcs[FUNC_Term] = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    if (lua_getfield(L, idx, "LastIndex") != LUA_TNIL) {
        m_funcs[FUNC_LastIndex] = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    if (lua_getfield(L, idx, "FirstIndex") != LUA_TNIL) {
        m_funcs[FUNC_FirstIndex] = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    if (lua_getfield(L, idx, "Snapshot") != LUA_TNIL) {
        m_funcs[FUNC_Snapshot] = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    lua_settop(L, top);
}

ErrorCode LDBStorage::InitialState(HardState &hs, ConfState &cs) {
    if (!m_funcs[FUNC_InitialState]) {
        fLog(&DefaultLogger::instance(), "InitialState not impl");
    }
    auto top = lua_gettop(m_l);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_funcs[FUNC_InitialState]);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_ref);
    if (lua_pcall(m_l, 1, 2, 0)) {
        std::string err = lua_tostring(m_l, -1);
        lua_settop(m_l, top);
        fLog(&DefaultLogger::instance(), err);
    }
    auto hs_ = (HardState *)luaL_checkudata(m_l, -2, MT_HARDSTATE);
    auto snap_ = (raftpb::Snapshot *)luaL_checkudata(m_l, -1, MT_SNAPSHOT);
    hs = *hs_;
    cs = snap_->metadata().conf_state();
    lua_settop(m_l, top);
    return OK;
}

Result<IEntrySlicePtr> LDBStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size) {
    if (!m_funcs[FUNC_Entries]) {
        fLog(&DefaultLogger::instance(), "Entries not impl");
    }
    auto top = lua_gettop(m_l);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_funcs[FUNC_Entries]);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_ref);
    lua_pushinteger(m_l, lo);
    lua_pushinteger(m_l, hi);
    lua_pushinteger(m_l, max_size);
    if (lua_pcall(m_l, 4, 2, 0)) {
        std::string err = lua_tostring(m_l, -1);
        lua_settop(m_l, top);
        fLog(&DefaultLogger::instance(), err);
    }
    m_ents.clear();
    auto err = (ErrorCode)luaL_checkinteger(m_l, -2);
    if (lua_istable(m_l, -1)) {
        lua_pushnil(m_l);
        while (lua_next(m_l, -2) != 0) {
            auto ent = (Entry *)luaL_checkudata(m_l, -1, MT_ENTRY);
            m_ents.emplace_back(*ent);
            lua_pop(m_l, 1);
        }
    }
    lua_settop(m_l, top);
    return { std::make_unique<EntrySlice<EntryVec>>(m_ents), err };
}

Result<uint64_t> LDBStorage::Term(uint64_t i) {
    if (!m_funcs[FUNC_Term]) {
        fLog(&DefaultLogger::instance(), "Term not impl");
    }
    auto top = lua_gettop(m_l);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_funcs[FUNC_Term]);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_ref);
    lua_pushinteger(m_l, i);
    if (lua_pcall(m_l, 2, 2, 0)) {
        std::string err = lua_tostring(m_l, -1);
        lua_settop(m_l, top);
        fLog(&DefaultLogger::instance(), err);
    }
    ErrorCode err = (ErrorCode)luaL_checkinteger(m_l, -2);
    uint64_t val = lua_tointeger(m_l, -1);
    lua_settop(m_l, top);
    return { std::move(val), err };
}

Result<uint64_t> LDBStorage::LastIndex() {
    if (!m_funcs[FUNC_LastIndex]) {
        fLog(&DefaultLogger::instance(), "LastIndex not impl");
    }
    auto top = lua_gettop(m_l);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_funcs[FUNC_LastIndex]);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_ref);
    if (lua_pcall(m_l, 1, 2, 0)) {
        std::string err = lua_tostring(m_l, -1);
        lua_settop(m_l, top);
        fLog(&DefaultLogger::instance(), err);
    }
    ErrorCode err = (ErrorCode)luaL_checkinteger(m_l, -2);
    uint64_t val = luaL_checkinteger(m_l, -1);
    lua_settop(m_l, top);
    return { std::move(val), err };
}

Result<uint64_t> LDBStorage::FirstIndex() {
    if (!m_funcs[FUNC_FirstIndex]) {
        fLog(&DefaultLogger::instance(), "FirstIndex not impl");
    }
    auto top = lua_gettop(m_l);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_funcs[FUNC_FirstIndex]);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_ref);
    if (lua_pcall(m_l, 1, 2, 0)) {
        std::string err = lua_tostring(m_l, -1);
        lua_settop(m_l, top);
        fLog(&DefaultLogger::instance(), err);
    }
    ErrorCode err = (ErrorCode)luaL_checkinteger(m_l, -2);
    uint64_t val = luaL_checkinteger(m_l, -1);
    lua_settop(m_l, top);
    return { std::move(val), err };
}

Result<raftpb::Snapshot*> LDBStorage::Snapshot() {
    if (!m_funcs[FUNC_Snapshot]) {
        fLog(&DefaultLogger::instance(), "Snapshot not impl");
    }
    auto top = lua_gettop(m_l);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_funcs[FUNC_Snapshot]);
    lua_rawgeti(m_l, LUA_REGISTRYINDEX, m_ref);
    if (lua_pcall(m_l, 1, 1, 0)) {
        std::string err = lua_tostring(m_l, -1);
        lua_settop(m_l, top);
        fLog(&DefaultLogger::instance(), err);
    }
    auto val = (raftpb::Snapshot*)luaL_checkudata(m_l, -1, MT_SNAPSHOT);
    lua_settop(m_l, top);
    return { std::move(val) };
}

