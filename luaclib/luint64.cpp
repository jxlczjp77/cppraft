#include <lua.hpp>
#include <stdint.h>
#include <stdlib.h>
#include "lutils.hpp"

static uint64_t _uint64(lua_State *L, int index) {
    int type = lua_type(L, index);
    uint64_t n = 0;
    switch (type) {
    case LUA_TNUMBER:
    {
        n = (uint64_t)lua_tointeger(L, index);
        break;
    }
    case LUA_TSTRING:
    {
        size_t len = 0;
        const uint8_t * str = (const uint8_t *)lua_tolstring(L, index, &len);
        if (len > 8) {
            return luaL_error(L, "The string (length = %d) is not an int64 string", len);
        }
        int i = 0;
        uint64_t n64 = 0;
        for (i = 0; i < (int)len; i++) {
            n64 |= (uint64_t)str[i] << (i * 8);
        }
        n = (uint64_t)n64;
        break;
    }
    case LUA_TLIGHTUSERDATA:
    {
        void * p = lua_touserdata(L, index);
        n = (intptr_t)p;
        break;
    }
    default:
        return luaL_error(L, "argument %d error type %s", index, lua_typename(L, type));
    }
    return n;
}

static inline void
_pushint64(lua_State *L, uint64_t n) {
    void * p = (void *)(intptr_t)n;
    lua_pushlightuserdata(L, p);
}

static int
uint64_add(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    _pushint64(L, a + b);

    return 1;
}

static int
uint64_sub(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    _pushint64(L, a - b);

    return 1;
}

static int
uint64_mul(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    _pushint64(L, a * b);

    return 1;
}

static int
uint64_div(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    if (b == 0) {
        return luaL_error(L, "div by zero");
    }
    _pushint64(L, a / b);

    return 1;
}

static int
uint64_mod(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    if (b == 0) {
        return luaL_error(L, "mod by zero");
    }
    _pushint64(L, a % b);

    return 1;
}

static uint64_t
_pow64(uint64_t a, uint64_t b) {
    if (b == 1) {
        return a;
    }
    uint64_t a2 = a * a;
    if (b % 2 == 1) {
        return _pow64(a2, b / 2) * a;
    } else {
        return _pow64(a2, b / 2);
    }
}

static int
uint64_pow(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    uint64_t p;
    if (b > 0) {
        p = _pow64(a, b);
    } else if (b == 0) {
        p = 1;
    } else {
        return luaL_error(L, "pow by nagtive number %d", (int)b);
    }
    _pushint64(L, p);

    return 1;
}

static int
uint64_new(lua_State *L) {
    int top = lua_gettop(L);
    uint64_t n;
    switch (top) {
    case 0:
        lua_pushlightuserdata(L, NULL);
        break;
    case 1:
        n = _uint64(L, 1);
        _pushint64(L, n);
        break;
    default:
    {
        int base = (int)luaL_checkinteger(L, 2);
        if (base < 2) {
            luaL_error(L, "base must be >= 2");
        }
        const char * str = luaL_checkstring(L, 1);
        n = strtoll(str, NULL, base);
        _pushint64(L, n);
        break;
    }
    }
    lua_pushvalue(L, lua_upvalueindex(1));
    lua_setmetatable(L, -2);
    return 1;
}

static int
uint64_eq(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    lua_pushboolean(L, a == b);
    return 1;
}

static int
uint64_lt(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    lua_pushboolean(L, a < b);
    return 1;
}

static int
uint64_le(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    uint64_t b = _uint64(L, 2);
    lua_pushboolean(L, a <= b);
    return 1;
}

static int
uint64_len(lua_State *L) {
    uint64_t a = _uint64(L, 1);
    lua_pushinteger(L, (lua_Integer)a);
    return 1;
}

static int
tostring(lua_State *L) {
    static const char *hex = "0123456789ABCDEF";
    uintptr_t n = (uintptr_t)lua_touserdata(L, 1);
    if (lua_gettop(L) == 1) {
        luaL_Buffer b;
        luaL_buffinitsize(L, &b, 28);
        luaL_addstring(&b, "uint64: 0x");
        int i;
        bool strip = true;
        for (i = 15; i >= 0; i--) {
            int c = (n >> (i * 4)) & 0xf;
            if (strip && c == 0) {
                continue;
            }
            strip = false;
            luaL_addchar(&b, hex[c]);
        }
        if (strip) {
            luaL_addchar(&b, '0');
        }
        luaL_pushresult(&b);
    } else {
        int base = (int)luaL_checkinteger(L, 2);
        int shift, mask;
        switch (base) {
        case 0:
        {
            unsigned char buffer[8];
            int i;
            for (i = 0; i < 8; i++) {
                buffer[i] = (n >> (i * 8)) & 0xff;
            }
            lua_pushlstring(L, (const char *)buffer, 8);
            return 1;
        }
        case 10:
        {
            uint64_t dec = (uint64_t)n;
            luaL_Buffer b;
            luaL_buffinitsize(L, &b, 28);
            int buffer[32];
            int i;
            for (i = 0; i < 32; i++) {
                buffer[i] = dec % 10;
                dec /= 10;
                if (dec == 0)
                    break;
            }
            while (i >= 0) {
                luaL_addchar(&b, hex[buffer[i]]);
                --i;
            }
            luaL_pushresult(&b);
            return 1;
        }
        case 2:
            shift = 1;
            mask = 1;
            break;
        case 8:
            shift = 3;
            mask = 7;
            break;
        case 16:
            shift = 4;
            mask = 0xf;
            break;
        default:
            luaL_error(L, "Unsupport base %d", base);
            break;
        }
        int i;
        char buffer[64];
        for (i = 0; i < 64; i += shift) {
            buffer[i / shift] = hex[(n >> (64 - shift - i)) & mask];
        }
        lua_pushlstring(L, buffer, 64 / shift);
    }
    return 1;
}

luaL_Reg uint64_mt[] = {
        { "__add", uint64_add },
        { "__sub", uint64_sub },
        { "__mul", uint64_mul },
        { "__div", uint64_div },
        { "__mod", uint64_mod },
        { "__pow", uint64_pow },
        { "__eq", uint64_eq },
        { "__lt", uint64_lt },
        { "__le", uint64_le },
        { "__len", uint64_len },
        { "__tostring", tostring },
        { NULL, NULL },
};

void regist_uint64(lua_State *L) {
    if (sizeof(intptr_t) != sizeof(uint64_t)) {
        luaL_error(L, "Only support 64bit architecture");
    }
    init_metatable(L, MT_UINT64, uint64_mt);

    luaL_getmetatable(L, MT_UINT64);
    lua_pushcclosure(L, uint64_new, 1);
    lua_setfield(L, -2, "uint64");
}
