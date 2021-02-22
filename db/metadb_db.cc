//
// 
//
#include <iostream>


#include "metadb_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {
    MetaDB::MetaDB(const char *dbfilename, utils::Properties &props) :noResult(0){
        
        //set option
        metadb::Option options;
        SetOptions(&options, props);
        

        int s = metadb::DB::Open(options, dbfilename, &db_);
        if(s != 0){
            cerr<<"Can't open metadb "<<dbfilename<<" "<<s<<endl;
            exit(0);
        }
    }

    void MetaDB::SetOptions(metadb::Option *options, utils::Properties &props) {
        options->DIR_FIRST_HASH_MAX_CAPACITY = 1;
        options->INODE_MAX_ZONE_NUM = 1;
        options->node_allocator_path = "/pmem0/test/node.pool";
        options->node_allocator_size = 1ULL * 1024 * 1024 * 1024;   //GB
        options->file_allocator_path = "/pmem0/test/file.pool";
        options->file_allocator_size = 1ULL * 1024 * 1024 * 1024;   //GB
    }


    int MetaDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        metadb::inode_id_t ikey = StringToInodeId(key);
        int s = db_->InodeGet(ikey, value);
        if(s == 0) {
            DeSerializeValues(value, result);
            return DB::kOK;
        }
        if(s == 2){
            noResult++;
            //cerr<<"read not found:"<<noResult<<endl;
            return DB::kOK;
        }else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int MetaDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        //无
        return DB::kOK;
    }

    int MetaDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        string value;
        SerializeValues(values,value);
        metadb::inode_id_t ikey = StringToInodeId(key);
        //printf("key:%s ikey:%llx\n", key, ikey);

        int s = db_->InodePut(ikey, value);
        if(s != 0 && s != 2){
            cerr<<"insert error\n"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    int MetaDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int MetaDB::Delete(const std::string &table, const std::string &key) {
        metadb::inode_id_t ikey = StringToInodeId(key);
        int s = db_->InodeDelete(ikey);
        if(s != 0 && s != 2){
            cerr<<"Delete error\n"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    void MetaDB::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        string stats;
        db_->PrintAllStats(stats);
        cout<<stats<<endl;
    }

    MetaDB::~MetaDB() {
        delete db_;
    }

    void MetaDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void MetaDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
    metadb::inode_id_t MetaDB::StringToInodeId(const string &str){
        uint64_t num = 0;
        sscanf(str.c_str(), "%lx", &num);
        return num;
    }
}
