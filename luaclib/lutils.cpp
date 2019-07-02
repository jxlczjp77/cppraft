#include "lutils.hpp"
#include <stdint.h>
#include <vector>
#include <string>
using namespace std;

void Xet_add(lua_State *L, Xet_reg l) {
    for (; l->name; l++) {
        lua_pushstring(L, l->name);
        lua_pushlightuserdata(L, (void*)l);
        lua_settable(L, -3);
    }
}

int index_handler(lua_State *L) {
    // stack has userdata, index
    if (lua_isnumber(L, 2)) {
        if (luaL_getmetafield(L, 1, "__array") == LUA_TNIL) {
            luaL_error(L, "cannot index '%s'", lua_tostring(L, 2));
        }
        lua_pushvalue(L, 1);
        lua_pushvalue(L, 2);
        lua_call(L, 2, 1);
        return 1;
    }
    lua_pushvalue(L, 2);                // dup index
    lua_rawget(L, lua_upvalueindex(1)); // lookup member by name

    if (!lua_islightuserdata(L, -1)) {
        if (lua_isnil(L, -1))                 // invalid member
            luaL_error(L, "cannot get member '%s'", lua_tostring(L, 2));

        return 1;
    }

    Xet_reg m = (Xet_reg)lua_touserdata(L, -1);
    if (!m->get_func) {
        luaL_error(L, "cannot get member '%s'", lua_tostring(L, 2));
    }
    lua_pop(L, 1);
    if (!lua_islightuserdata(L, 1)) {
        luaL_checktype(L, 1, LUA_TUSERDATA);
    }
    return m->get_func(L, (void*)((char*)lua_touserdata(L, 1) + m->offset));
}

int newindex_handler(lua_State *L) {
    // stack has userdata, index, value
    lua_pushvalue(L, 2);                // dup index
    lua_rawget(L, lua_upvalueindex(1)); // lookup member by name

    Xet_reg m = nullptr;
    if (!lua_islightuserdata(L, -1) || (m = (Xet_reg)lua_touserdata(L, -1), !m->set_func))    // invalid member
        luaL_error(L, "cannot set member '%s'", lua_tostring(L, 2));

    lua_pop(L, 1);
    if (!lua_islightuserdata(L, 1)) {
        luaL_checktype(L, 1, LUA_TUSERDATA);
    }
    return m->set_func(L, (void*)((char*)lua_touserdata(L, 1) + m->offset));
}

int get_vec_uint64(lua_State *L, void *v) {
    vector<uint64_t> &ss = *(vector<uint64_t> *)v;
    lua_createtable(L, (int)ss.size(), 0);
    for (size_t i = 0; i < ss.size(); i++) {
        lua_pushinteger(L, ss[i]);
        lua_seti(L, -2, i);
    }
    return 1;
}

int set_vec_uint64(lua_State *L, void *v) {
    luaL_checktype(L, 3, LUA_TTABLE);
    vector<uint64_t> &ss = *(vector<uint64_t> *)v;
    ss.clear();
    lua_pushnil(L);
    while (lua_next(L, 3) != 0) {
        auto val = luaL_checkinteger(L, -1);
        ss.push_back(val);
        lua_pop(L, 1);
    }
    lua_pop(L, 1);
    return 0;
}

int get_bool(lua_State *L, void *v) {
    lua_pushboolean(L, *(bool*)v);
    return 1;
}

int set_bool(lua_State *L, void *v) {
    *(bool*)v = lua_toboolean(L, 3);
    return 0;
}

int get_string(lua_State *L, void *v) {
    const string &s = *(string *)v;
    lua_pushlstring(L, s.c_str(), s.length());
    return 1;
}

int set_string(lua_State *L, void *v) {
    size_t l = 0;
    auto s = lua_tolstring(L, 3, &l);
    ((string*)v)->assign(s, l);
    return 0;
}

static bool is_meta_methods(const char *name) {
    return name[0] == '_' && name[1] == '_';
}

static void set_funcs(lua_State *L, const luaL_Reg *l, bool is_meta_or_methord) {
    for (; l->name; l++) {
        if (is_meta_methods(l->name) == is_meta_or_methord) {
            lua_pushcfunction(L, l->func);
            lua_setfield(L, -2, l->name);
        }
    }
}

void init_metatable(lua_State *L, const char *metatable_name, const luaL_Reg methods[], const Xet_reg_pre getsets[]) {
    luaL_newmetatable(L, metatable_name);

    if (getsets) {
        set_funcs(L, methods, true);
        lua_newtable(L);
        Xet_add(L, getsets);
        set_funcs(L, methods, false);
        lua_pushliteral(L, "__index");
        lua_pushvalue(L, -2);
        lua_pushcclosure(L, index_handler, 1);
        lua_rawset(L, -4);

        lua_pushliteral(L, "__newindex");
        lua_pushvalue(L, -2);
        lua_pushcclosure(L, newindex_handler, 1);
        lua_rawset(L, -4);

        lua_pop(L, 1);
    } else {
        luaL_setfuncs(L, methods, 0);

        lua_pushliteral(L, "__index");
        lua_pushvalue(L, -2);
        lua_settable(L, -3);
    }

    lua_pop(L, 1);
}
