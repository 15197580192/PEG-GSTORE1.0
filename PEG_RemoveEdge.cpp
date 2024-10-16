/*
 * @Filename: PEG_AddEdge.cpp
 * @Author: Hu Zheyuan
 * @Date: 2024-10-12 08:46:08
 * @LastEditTime: 2024-10-12 15:42:57
 * @LastEditors: Hu Zheyuan
 * @Description: a test to remove edge
 */
#include <iostream>
#include <string>
#include <vector>

#include "handler/Handler.h"
#include "util/util.h"
#include "crow/json.h"
#include "gStoreAPI/client.h"

using namespace std;

int main(int argc, char const *argv[])
{
    if(argc < 5)
    {
        cout << "============================================================" << endl;
        cout << "Add edge. The way to use this program: " << endl;
        cout << "./build/PEG_RemoveEdge database_name fromVertex perdicate toVertex" << endl;
        cout << "./build/PEG_RemoveEdge watdiv \<http://db.uwaterloo.ca/~galuc/wsdbm/User0\> \<test\> \<http://db.uwaterloo.ca/~galuc/wsdbm/User1\>" << endl;
        cout << "please note that fromVertex and toVertex must have existed already" << endl;
        cout << "============================================================" << endl;

        return 0;
    }
    long begintime = Util::get_cur_time();
    string serversListStr = readFile("conf/servers.json");
    crow::json::rvalue confJson = crow::json::load(serversListStr);

    string dbname(argv[1]);
    string fromVertex(argv[2]);
    string predicate(argv[3]);
    string toVertex(argv[4]);

    string dividefile = "entity/"+dbname+"InternalPoints.txt";
    // 读取分片文件 
    std::ifstream ifs_dividefile;
    ifs_dividefile.open(dividefile);
    int num = 0; // 分片数
    std::string entity;
    int partID;
    std::unordered_map<std::string, int> entity2id; // 存储顶点分片信息
    // 获取所有顶点分片信息
    while (ifs_dividefile >> entity && ifs_dividefile >> partID)
    {
        entity2id.insert(std::pair<std::string, int>(entity, partID));
        num = partID > num ? partID : num;
    }
    num++;

    // 在同一个分片，插入当前分片
    // 在不同分片，分别插入两个分片
    long long resNum;
    int subPartID1 = entity2id[fromVertex];
    cout<<"fromVertex is in part:"<<subPartID1<<endl;
    int subPartID2 = entity2id[toVertex];
    cout<<"toVertex is in part:"<<subPartID2<<endl;

    
    vector<GstoreConnector*> servers;

    if(subPartID1==subPartID2) {
        servers.push_back(new GstoreConnector(
            confJson["sites"][subPartID1]["ip"].s(),
            (int)confJson["sites"][subPartID1]["port"],
            confJson["sites"][subPartID1]["http_type"].s(),
            confJson["sites"][subPartID1]["dbuser"].s(),
            confJson["sites"][subPartID1]["dbpasswd"].s()
        ));
    } else {
        servers.push_back(new GstoreConnector(
            confJson["sites"][subPartID1]["ip"].s(),
            (int)confJson["sites"][subPartID1]["port"],
            confJson["sites"][subPartID1]["http_type"].s(),
            confJson["sites"][subPartID1]["dbuser"].s(),
            confJson["sites"][subPartID1]["dbpasswd"].s()
        ));
        servers.push_back(new GstoreConnector(
            confJson["sites"][subPartID2]["ip"].s(),
            (int)confJson["sites"][subPartID2]["port"],
            confJson["sites"][subPartID2]["http_type"].s(),
            confJson["sites"][subPartID2]["dbuser"].s(),
            confJson["sites"][subPartID2]["dbpasswd"].s()
        ));
    }
    
    std::string sparql = "delete data { " + fromVertex + "  " + predicate + "   " + toVertex + " . }";

    update(servers, dbname, sparql);
    
    long endtime = Util::get_cur_time();
    cout << "cost time: " << endtime - begintime << " ms" << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    return 0;
}

