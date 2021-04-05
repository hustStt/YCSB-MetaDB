//
// 
//

#ifndef YCSB_C_HBKV_DB_H
#define YCSB_C_HBKV_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include "core/core_workload.h"

#include <nvmbtree/nvm_btree.h>


using std::cout;
using std::endl;

namespace ycsbc {
    class HBKV : public DB{
    public :
        HBKV(const char *dbfilename, utils::Properties &props);
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

        ~HBKV();

    private:
        NVMBtree *db_;
        unsigned noResult;
        bool write_sync_;

        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);
        uint64_t StringToUint64(const string &str);
    };
}


#endif //YCSB_C_HBKV_DB_H
