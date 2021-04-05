#include <iostream>


#include "hbkv_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {
    HBKV::HBKV(const char *dbfilename, utils::Properties &props) :noResult(0), write_sync_(false){
        string persistent_path(dbfilename);
        db_ = new NVMBtree();
        db_->Init(persistent_path, true);
        if(!s.ok()){
            cerr<<"Can't open hbkv "<<dbfilename<<" "<<s.ToString()<<endl;
            exit(0);
        }
    }



    int HBKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        uint64_t ikey = StringToUint64(key);
        int s = db_->Get(ikey, value);
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


    int HBKV::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        std::vector<std::string> values;
        db_->GetRange(StringToUint64(key),UINT64_MAX,values,len);
        return DB::kOK;
    }

    int HBKV::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        string value;
        SerializeValues(values,value);
        uint64_t ikey = StringToUint64(key);
        //printf("key:%s ikey:%llx\n", key, ikey);

        db_->Insert(ikey, value);
        return DB::kOK;
    }

    int HBKV::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        string value;
        SerializeValues(values,value);
        uint64_t ikey = StringToUint64(key);
        //printf("key:%s ikey:%llx\n", key, ikey);

        db_->Update(ikey, value);
        return DB::kOK;
    }

    int HBKV::Delete(const std::string &table, const std::string &key) {
        uint64_t ikey = StringToUint64(key);
        db_->Delete(ikey);
        return DB::kOK;
    }

    void HBKV::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        db_->PrintInfo();
    }

    HBKV::~HBKV() {
        delete db_;
    }

    void HBKV::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void HBKV::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
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

    uint64_t HBKV::StringToUint64(const string &str){
        uint64_t num = 0;
        sscanf(str.c_str(), "%lx", &num);
        return num;
    }
}
