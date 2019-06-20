#include "lutils.hpp"
#include <raft/raft.pb.h>
#include <raft/entrys.hpp>
using namespace raftpb;

static int lnew_entry(lua_State *L) {
    void *p = lua_newuserdata(L, sizeof(Entry));
    new (p) Entry();
    luaL_getmetatable(L, MT_ENTRY);
    lua_setmetatable(L, -2);
    return 1;
}

static int lnew_message(lua_State *L) {
    Message *m = nullptr;
    if (lua_gettop(L) >= 1) {
        m = (Message *)luaL_checkudata(L, 1, MT_MESSAGE);
    }

    auto p = (Message *)lua_newuserdata(L, sizeof(Message));
    if (m) {
        new (p) Message(*m);
    } else {
        new (p) Message();
    }
    luaL_getmetatable(L, MT_MESSAGE);
    lua_setmetatable(L, -2);
    return 1;
}

static int lnew_confchange(lua_State *L) {
    void *p = lua_newuserdata(L, sizeof(ConfChange));
    new (p) ConfChange();
    luaL_getmetatable(L, MT_CONFCHANGE);
    lua_setmetatable(L, -2);
    return 1;
}

static int lentry_delete(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    e->~Entry();
    return 0;
}

static int lmessage_delete(lua_State *L) {
    Message *node = (Message *)luaL_checkudata(L, 1, MT_MESSAGE);
    node->~Message();
    return 0;
}

int lentry_set_index(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    e->set_index(luaL_checkinteger(L, 2));
    return 0;
}

int lentry_index(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    lua_pushinteger(L, e->index());
    return 1;
}

int lentry_set_term(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    e->set_term(luaL_checkinteger(L, 2));
    return 0;
}

int lentry_term(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    lua_pushinteger(L, e->term());
    return 1;
}

int lentry_set_type(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    e->set_type((EntryType)luaL_checkinteger(L, 2));
    return 0;
}

int lentry_type(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    lua_pushinteger(L, e->type());
    return 1;
}

int lentry_set_data(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    size_t len = 0;
    const char *p = luaL_checklstring(L, 2, &len);
    e->set_data(p, len);
    return 0;
}

int lentry_data(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    auto &data = e->data();
    lua_pushlstring(L, data.c_str(), data.length());
    return 1;
}

int lentry_parser(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    size_t len = 0;
    const char *p = luaL_checklstring(L, 2, &len);
    lua_pushboolean(L, e->ParseFromArray(p, (int)len));
    return 1;
}

int lentry_serialize(lua_State *L) {
    Entry *e = (Entry *)luaL_checkudata(L, 1, MT_ENTRY);
    auto d = e->SerializeAsString();
    lua_pushlstring(L, d.c_str(), d.length());
    return 1;
}

static const luaL_Reg entry_m[] = {
    {"__gc", lentry_delete},
    {"set_index", lentry_set_index},
    {"index", lentry_index},
    {"set_term", lentry_set_term},
    {"term", lentry_term},
    {"set_type", lentry_set_type},
    {"type", lentry_type},
    {"set_data", lentry_set_data},
    {"data", lentry_data},
    {"serialize", lentry_serialize},
    {"parser", lentry_parser},
    {NULL, NULL}
};

int lsnapshot_data(lua_State *L) {
    Snapshot *s = (Snapshot *)luaL_checkudata(L, 1, MT_SNAPSHOT);
    auto &d = s->data();
    lua_pushlstring(L, d.c_str(), d.length());
    return 1;
}

int lsnapshot_metadata(lua_State *L) {
    Snapshot *s = (Snapshot *)luaL_checkudata(L, 1, MT_SNAPSHOT);
    lua_pushlightuserdata(L, s->mutable_metadata());
    luaL_getmetatable(L, MT_SNAPSHOT_METADATA);
    lua_setmetatable(L, -2);
    return 1;
}

static const luaL_Reg snatshop_m[] = {
    {"data", lsnapshot_data},
    {"metadata", lsnapshot_metadata},
    {NULL, NULL}
};

int lsnapshot_metadata_index(lua_State *L) {
    SnapshotMetadata *s = (SnapshotMetadata *)luaL_checkudata(L, 1, MT_SNAPSHOT_METADATA);
    lua_pushinteger(L, s->index());
    return 1;
}

int lsnapshot_metadata_term(lua_State *L) {
    SnapshotMetadata *s = (SnapshotMetadata *)luaL_checkudata(L, 1, MT_SNAPSHOT_METADATA);
    lua_pushinteger(L, s->term());
    return 1;
}

int lsnapshot_metadata_conf_state(lua_State *L) {
    SnapshotMetadata *s = (SnapshotMetadata *)luaL_checkudata(L, 1, MT_SNAPSHOT_METADATA);
    lua_pushlightuserdata(L, s->mutable_conf_state());
    luaL_getmetatable(L, MT_CONFSTATE);
    lua_setmetatable(L, -2);
    return 1;
}

static const luaL_Reg snatshop_metadata_m[] = {
    {"index", lsnapshot_metadata_index},
    {"term", lsnapshot_metadata_term},
    {"conf_state", lsnapshot_metadata_conf_state},
    {NULL, NULL}
};

int lconf_state_nodes(lua_State *L) {
    ConfState *s = (ConfState *)luaL_checkudata(L, 1, MT_CONFSTATE);
    lua_createtable(L, s->nodes().size(), 0);
    size_t i = 1;
    for (auto id : s->nodes()) {
        lua_pushinteger(L, id);
        lua_seti(L, -2, i++);
    }
    return 1;
}

int lconf_state_learners(lua_State *L) {
    ConfState *s = (ConfState *)luaL_checkudata(L, 1, MT_CONFSTATE);
    lua_createtable(L, s->learners().size(), 0);
    size_t i = 1;
    for (auto id : s->learners()) {
        lua_pushinteger(L, id);
        lua_seti(L, -2, i++);
    }
    return 1;
}

static const luaL_Reg conf_state_m[] = {
    {"nodes", lconf_state_nodes},
    {"learners", lconf_state_learners},
    {NULL, NULL}
};

int lmessage_type(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_type()) lua_pushinteger(L, s->type());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_type(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_type((MessageType)luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_to(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_to()) lua_pushinteger(L, s->to());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_to(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_to(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_from(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_from()) lua_pushinteger(L, s->from());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_from(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_from(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_term(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_term()) lua_pushinteger(L, s->term());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_term(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_term(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_logterm(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_logterm()) lua_pushinteger(L, s->logterm());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_logterm(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_logterm(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_index(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_index()) lua_pushinteger(L, s->index());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_index(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_index(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_commit(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_commit()) lua_pushinteger(L, s->commit());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_commit(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_commit(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_snapshot(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_snapshot()) {
        lua_pushlightuserdata(L, s->mutable_snapshot());
        luaL_getmetatable(L, MT_SNAPSHOT);
        lua_setmetatable(L, -2);
    }
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_snapshot(lua_State *L, void *v) {
    Message *s = (Message *)v;
    auto snapshot = (Snapshot *)luaL_checkudata(L, 3, MT_SNAPSHOT);
    *s->mutable_snapshot() = *snapshot;
    return 0;
}

int lmessage_reject(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_reject()) lua_pushinteger(L, s->reject());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_reject(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_reject(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_rejecthint(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_rejecthint()) lua_pushinteger(L, s->rejecthint());
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_rejecthint(lua_State *L, void *v) {
    Message *s = (Message *)v;
    s->set_rejecthint(luaL_checkinteger(L, 3));
    return 0;
}

int lmessage_context(lua_State *L, void *v) {
    Message *s = (Message *)v;
    if (s->has_context()) {
        auto &content = s->context();
        lua_pushlstring(L, content.c_str(), content.size());
    }
    else lua_pushnil(L);
    return 1;
}

int lmessage_set_context(lua_State *L, void *v) {
    Message *s = (Message *)v;
    size_t l = 0;
    auto p = luaL_checklstring(L, 3, &l);
    s->set_context(p, l);
    return 0;
}

int lmessage_entries(lua_State *L, void *v) {
    Message *s = (Message *)v;
        lua_pushlightuserdata(L, (void *)&s->entries());
        luaL_getmetatable(L, MT_MSG_ENTRIES);
        lua_setmetatable(L, -2);
    return 1;
}

int lmessage_set_entries(lua_State *L, void *v) {
    auto entries = (google::protobuf::RepeatedPtrField<Entry> *)v;
    if (lua_istable(L, 3)) {
        entries->Clear();
        lua_pushnil(L);
        while (lua_next(L, 3) != 0) {
            auto entry = (Entry *)luaL_checkudata(L, -1, MT_ENTRY);
            *entries->Add() = std::move(*entry);
            lua_pop(L, 1);
        }
    } else if (lua_isuserdata(L, 3)) {
        entries->Clear();
        auto slice = (raft::IEntrySlice *)luaL_checkudata(L, 3, MT_SLICE);
        for (auto &s: *slice) {
            *entries->Add() = std::move(s);
        }
    } else {
        luaL_error(L, "invalid entries");
    }
    return 0;
}

static const Xet_reg_pre message_getsets[] = {
    {"type",lmessage_type, lmessage_set_type, 0},
    {"to",lmessage_to, lmessage_set_to, 0},
    {"from",lmessage_from, lmessage_set_from, 0},
    {"term",lmessage_term, lmessage_set_term, 0},
    {"logterm",lmessage_logterm, lmessage_set_logterm, 0},
    {"index",lmessage_index, lmessage_set_index, 0},
    {"commit",lmessage_commit, lmessage_set_commit, 0},
    {"snapshot",lmessage_snapshot, lmessage_set_snapshot, 0},
    {"reject",lmessage_reject, lmessage_set_reject, 0},
    {"rejecthint",lmessage_rejecthint, lmessage_set_rejecthint, 0},
    {"context",lmessage_context, lmessage_set_context, 0},
    {"entries",lmessage_entries, lmessage_set_entries, 0},
    {NULL}
};

int lmessage_entry(lua_State *L) {
    auto msg = (Message *)luaL_checkudata(L, 1, MT_MESSAGE);
    auto idx = luaL_checkinteger(L, 2);
    lua_pushlightuserdata(L, (void *)&msg->entries((int)idx));
    luaL_getmetatable(L, MT_ENTRY);
    lua_setmetatable(L, -2);
    return 1;
}

static const luaL_Reg message_m[] = {
    {"__gc", lmessage_delete},
    {"entry", lmessage_entry},
    {NULL, NULL}
};

int lmessage_entries_ipairsaux(lua_State *L) {
    auto entries = (google::protobuf::RepeatedPtrField<Entry> *)lua_touserdata(L, 1);
    lua_Integer i = luaL_checkinteger(L, 2) + 1;
    lua_pushinteger(L, i);
    if (i <= (lua_Integer)entries->size()) {
        lua_pushlightuserdata(L, (void *)&entries->Get((int)i - 1));
        luaL_getmetatable(L, MT_ENTRY);
        lua_setmetatable(L, -2);
        return 2;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

int lmessage_entries_ipairs(lua_State *L) {
    luaL_checkudata(L, 1, MT_MSG_ENTRIES);
    lua_pushcfunction(L, lmessage_entries_ipairsaux);  /* iteration function */
    lua_pushvalue(L, 1);  /* state */
    lua_pushinteger(L, 0);  /* initial value */
    return 3;
}

int lmessage_entries_size(lua_State *L) {
    auto entries = (google::protobuf::RepeatedPtrField<Entry> *)luaL_checkudata(L, 1, MT_MSG_ENTRIES);
    lua_pushinteger(L, entries->size());
    return 1;
}

int lmessage_entries_at(lua_State *L) {
    auto entries = (google::protobuf::RepeatedPtrField<Entry> *)luaL_checkudata(L, 1, MT_MSG_ENTRIES);
    lua_pushlightuserdata(L, (void *)&entries->Get((int)luaL_checkinteger(L, 2)));
    luaL_getmetatable(L, MT_ENTRY);
    lua_setmetatable(L, -2);
    return 1;
}

static const luaL_Reg message_entries_m[] = {
    {"ipairs", lmessage_entries_ipairs},
    {"size", lmessage_entries_size},
    {"at", lmessage_entries_at},
    {NULL, NULL}
};

int lhardstate_commit(lua_State *L, void *v) {
    HardState *s = (HardState *)v;
    if (s->has_commit()) lua_pushinteger(L, s->commit());
    else lua_pushnil(L);
    return 1;
}

int lhardstate_set_commit(lua_State *L, void *v) {
    HardState *s = (HardState *)v;
    s->set_commit(luaL_checkinteger(L, 3));
    return 0;
}

int lhardstate_term(lua_State *L, void *v) {
    HardState *s = (HardState *)v;
    if (s->has_term()) lua_pushinteger(L, s->term());
    else lua_pushnil(L);
    return 1;
}

int lhardstate_set_term(lua_State *L, void *v) {
    HardState *s = (HardState *)v;
    s->set_term(luaL_checkinteger(L, 3));
    return 0;
}

int lhardstate_vote(lua_State *L, void *v) {
    HardState *s = (HardState *)v;
    if (s->has_vote()) lua_pushinteger(L, s->vote());
    else lua_pushnil(L);
    return 1;
}

int lhardstate_set_vote(lua_State *L, void *v) {
    HardState *s = (HardState *)v;
    s->set_vote(luaL_checkinteger(L, 3));
    return 0;
}

static int lhardstate_delete(lua_State *L) {
    HardState *node = (HardState *)luaL_checkudata(L, 1, MT_HARDSTATE);
    node->~HardState();
    return 0;
}

static const luaL_Reg hardstate_m[] = {
    {"__gc", lhardstate_delete},
    {NULL, NULL}
};

static const Xet_reg_pre hardstate_getsets[] = {
    {"commit",lhardstate_commit, lhardstate_set_commit, 0},
    {"term",lhardstate_term, lhardstate_set_term, 0},
    {"vote",lhardstate_vote, lhardstate_set_vote, 0},
    {NULL, NULL}
};

static int lconfchange_delete(lua_State *L) {
    ConfChange *cc = (ConfChange *)luaL_checkudata(L, 1, MT_CONFCHANGE);
    cc->~ConfChange();
    return 0;
}

static int lconfchange_parser(lua_State *L) {
    ConfChange *cc = (ConfChange *)luaL_checkudata(L, 1, MT_CONFCHANGE);
    size_t l = 0;
    auto p = luaL_checkstring(L, 2);
    lua_pushboolean(L, cc->ParsePartialFromArray(p, (int)l));
    return 1;
}

static int lconfchange_serialize(lua_State *L) {
    ConfChange *cc = (ConfChange *)luaL_checkudata(L, 1, MT_CONFCHANGE);
    auto s = cc->SerializeAsString();
    lua_pushlstring(L, s.c_str(), s.size());
    return 1;
}

int lconfchange_id(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    if (s->has_id()) lua_pushinteger(L, s->id());
    else lua_pushnil(L);
    return 1;
}

int lconfchange_set_id(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    s->set_id(luaL_checkinteger(L, 3));
    return 0;
}

int lconfchange_type(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    if (s->has_type()) lua_pushinteger(L, s->type());
    else lua_pushnil(L);
    return 1;
}

int lconfchange_set_type(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    s->set_type((ConfChangeType)luaL_checkinteger(L, 3));
    return 0;
}

int lconfchange_nodeid(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    if (s->has_nodeid()) lua_pushinteger(L, s->nodeid());
    else lua_pushnil(L);
    return 1;
}

int lconfchange_set_nodeid(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    s->set_nodeid((ConfChangeType)luaL_checkinteger(L, 3));
    return 0;
}

int lconfchange_context(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    if (s->has_context()) {
        auto &context = s->context();
        lua_pushlstring(L, context.c_str(), context.size());
    }
    else lua_pushnil(L);
    return 1;
}

int lconfchange_set_context(lua_State *L, void *v) {
    ConfChange *s = (ConfChange *)v;
    size_t l = 0;
    auto p = luaL_checklstring(L, 3, &l);
    s->set_context(p, l);
    return 0;
}

static const luaL_Reg confchange_m[] = {
    {"__gc", lconfchange_delete},
    {"serialize", lconfchange_serialize},
    {"parser", lconfchange_parser},
    {NULL, NULL}
};

static const Xet_reg_pre confchange_getsets[] = {
    {"id", lconfchange_id, lconfchange_set_id, 0},
    {"type", lconfchange_type, lconfchange_set_type, 0},
    {"nodeid", lconfchange_nodeid, lconfchange_set_nodeid, 0},
    {"context", lconfchange_context, lconfchange_set_context, 0},
    {NULL, NULL}
};

int lMessageType_Name(lua_State *L) {
    auto &n = MessageType_Name((MessageType)luaL_checkinteger(L, 1));
    lua_pushlstring(L, n.c_str(), n.size());
    return 1;
}

static const luaL_Reg pb_m[] = {
    {"entry", lnew_entry},
    {"message", lnew_message},
    {"confchange", lnew_confchange},
    {"MessageType_Name", lMessageType_Name},
    {NULL, NULL}
};

void regist_pb_class(lua_State *L) {
    luaL_newlib(L, pb_m);

    REG_ENUM(L, EntryNormal);
    REG_ENUM(L, EntryConfChange);

    REG_ENUM(L, MsgHup);
    REG_ENUM(L, MsgBeat);
    REG_ENUM(L, MsgProp);
    REG_ENUM(L, MsgApp);
    REG_ENUM(L, MsgAppResp);
    REG_ENUM(L, MsgVote);
    REG_ENUM(L, MsgVoteResp);
    REG_ENUM(L, MsgSnap);
    REG_ENUM(L, MsgHeartbeat);
    REG_ENUM(L, MsgHeartbeatResp);
    REG_ENUM(L, MsgUnreachable);
    REG_ENUM(L, MsgSnapStatus);
    REG_ENUM(L, MsgCheckQuorum);
    REG_ENUM(L, MsgTransferLeader);
    REG_ENUM(L, MsgTimeoutNow);
    REG_ENUM(L, MsgReadIndex);
    REG_ENUM(L, MsgReadIndexResp);
    REG_ENUM(L, MsgPreVote);
    REG_ENUM(L, MsgPreVoteResp);

    REG_ENUM(L, ConfChangeAddNode);
    REG_ENUM(L, ConfChangeRemoveNode);
    REG_ENUM(L, ConfChangeUpdateNode);
    REG_ENUM(L, ConfChangeAddLearnerNode);

    REG_ENUM(L, MessageType_MIN);
    REG_ENUM(L, MessageType_MAX);

    lua_setfield(L, -2, "pb");

    init_metatable(L, MT_ENTRY, entry_m);
    init_metatable(L, MT_SNAPSHOT, snatshop_m);
    init_metatable(L, MT_SNAPSHOT_METADATA, snatshop_metadata_m);
    init_metatable(L, MT_CONFSTATE, conf_state_m);
    init_metatable(L, MT_MESSAGE, message_m, message_getsets);
    init_metatable(L, MT_HARDSTATE, hardstate_m, hardstate_getsets);
    init_metatable(L, MT_MSG_ENTRIES, message_entries_m);
    init_metatable(L, MT_CONFCHANGE, confchange_m, confchange_getsets);
}
