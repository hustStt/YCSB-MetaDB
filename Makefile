
CC=g++
CXXFLAGS= -std=c++11 -g -Wall -I./ 
LDFLAGS= -lpthread 

LIB_SOURCES= \
		core/core_workload.cc  \
		db/db_factory.cc   \
		db/hashtable_db.cc  \

##METADB
METADB_SOURCES= db/metadb_db.cc
METADB_LIBRARY= -lmetadb -lpmem
METADB_DEFS= -DYCSB_METADB
METADB_OBJECTS=$(METADB_SOURCES:.cc=.o)
##

##leveldb
LEVELDB_SOURCES= db/leveldb_db.cc
LEVELDB_LIBRARY= -lleveldb    
LEVELDB_DEFS= -DYCSB_LEVELDB
LEVELDB_OBJECTS=$(LEVELDB_SOURCES:.cc=.o)
##


OBJECTS=$(LIB_SOURCES:.cc=.o)
EXEC=ycsbc

ONLY_METADB_SOURCES=$(LIB_SOURCES) $(METADB_SOURCES)
ONLY_LEVELDB_SOURCES=$(LIB_SOURCES) $(LEVELDB_SOURCES)
ALL_SOURCES=$(LIB_SOURCES) $(METADB_SOURCES) $(LEVELDB_SOURCES)

all: clean
	$(CC) $(CXXFLAGS) $(METADB_DEFS) $(LEVELDB_DEFS) ycsbc.cc $(ALL_SOURCES) -o $(EXEC) $(LDFLAGS) $(METADB_LIBRARY) $(LEVELDB_LIBRARY)

metadb: clean
	$(CC) $(CXXFLAGS) $(METADB_DEFS) ycsbc.cc $(ONLY_METADB_SOURCES) -o $(EXEC) $(LDFLAGS) $(METADB_LIBRARY)

leveldb: clean
	$(CC) $(CXXFLAGS) $(LEVELDB_DEFS) ycsbc.cc $(ONLY_LEVELDB_SOURCES) -o $(EXEC) $(LDFLAGS) $(LEVELDB_LIBRARY)


clean:
	rm -f $(EXEC) 

.PHONY: clean leveldb metadb

