CXX=g++
CXXFLAGS=-g -O2 -Wall -std=c++14 -fpic -Wno-switch
RM=rm -f
LRAFT_TARGET=lraft.so
RAFT_TARGET=libraft.a

# Add a custom version below (5.1/5.2/5.3)
LUA_VERSION=5.3
LUA_DIR=/usr/local
LUA_INCDIR=$(LUA_DIR)/include/lua$(LUA_VERSION)

RAFT_SRCS=$(filter-out %test.cpp, $(wildcard src/*.cpp))
RAFT_TEST_SRCS=$(filter %test.cpp, $(wildcard src/*.cpp))

all: $(RAFT_TARGET) $(LRAFT_TARGET) test lua_leveldb
	
test: $(RAFT_TEST_SRCS) $(RAFT_TARGET)
	$(CXX) $(CXXFLAGS) -DBOOST_TEST_DYN_LINK $(RAFT_TEST_SRCS) -I/usr/local/include -Iinclude -Iinclude/raft -L. -lraft -lprotobuf -lboost_unit_test_framework -o $@

$(LRAFT_TARGET): $(RAFT_TARGET) luaclib/*.cpp luaclib/*.hpp
	$(CXX) $(CXXFLAGS) -shared -Wno-invalid-offsetof -Iinclude -Iinclude/raft -Iluaclib -I/usr/local/include luaclib/*.cpp -o $@ -L. -lraft -lprotobuf -lpthread

$(RAFT_TARGET): src/raft.pb.o $(subst .cpp,.o,$(RAFT_SRCS))
	ar rcs $@ $^

src/%.o: src/%.cpp include/raft/*.hpp
	$(CXX) -c $(CXXFLAGS) -Iinclude -Iinclude/raft -o $@ -I/usr/local/include $<

src/raft.pb.o: src/raftpb/raft.proto
	cd src/raftpb && protoc --cpp_out=.. raft.proto
	mv src/raft.pb.h include/raft
	$(CXX) -c $(CXXFLAGS) -Iinclude -Iinclude/raft -o $@ -I/usr/local/include src/raft.pb.cc
	
lua_leveldb:
	cd 3rd/lua_leveldb && make
	mv 3rd/lua_leveldb/lualeveldb.so .

.PHONY: clean
clean:
	$(RM) *.so src/*.o *.a test
