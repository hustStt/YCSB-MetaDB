//
// 
//

#ifndef YCSB_C_METADB_DB_H
#define YCSB_C_METADB_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include <stdint.h>
#include "core/properties.h"
#include "core/core_workload.h"

#include <metadb/db.h>


using std::cout;
using std::endl;

namespace ycsbc {
    class MetaDB : public DB{
    public :
        MetaDB(const char *dbfilename, utils::Properties &props);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void PrintStats();

        ~MetaDB();

    private:
        metadb::DB *db_;
        unsigned noResult;

        void SetOptions(metadb::Option *options, utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);
        metadb::inode_id_t StringToInodeId(const string &str);

    };
}


#endif //YCSB_C_LEVELDB_DB_H
