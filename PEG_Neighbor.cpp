/*
 * @Filename: PEG_Neighbor.cpp
 * @Author: Hu Zheyuan
 * @Date: 2022-11-10 12:27:43
 * @LastEditTime: 2023-01-09 10:00:43
 * @LastEditors: Hu Zheyuan
 * @Description: a test to query a vertex's neighbor.
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
    if(argc < 4)
    {
        cout << "============================================================" << endl;
        cout << "Query a vertex's neighbor. The way to use this program: " << endl;
        cout << "./build/PEG_Neighbor database_name in_out(true/false) vertex_name [pred_name]" << endl;
        cout << "============================================================" << endl;

        return 0;
    }
    long begintime = Util::get_cur_time();
    string serversListStr = readFile("conf/servers.json");
    crow::json::rvalue confJson = crow::json::load(serversListStr);

    vector<GstoreConnector*> servers;
    for (int i = 0; i < confJson["sites"].size(); i++)
    {
        servers.push_back(new GstoreConnector(
            confJson["sites"][i]["ip"].s(),
            (int)confJson["sites"][i]["port"],
            confJson["sites"][i]["http_type"].s(),
            confJson["sites"][i]["dbuser"].s(),
            confJson["sites"][i]["dbpasswd"].s()
        ));
    }

    string dbname(argv[1]);
    string in_out(argv[2]);
    string vertex_name(argv[3]);
    string pred_name("");
    bool flag = true;
    if(argc > 4) pred_name = argv[4];
    if(in_out.compare("false")==0) flag = false;

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


    long begin = Util::get_cur_time();
    long long resNum;
    int subPartID = entity2id[vertex_name];
    cout<<subPartID<<endl;
    map<string,set<string>> resultStr = queryNeighbor2(confJson, servers, dbname, vertex_name, pred_name, flag, resNum, subPartID);
    
    long end = Util::get_cur_time();
    cout << "Query1 Time is : " << end - begin << " ms." << endl;
    
    begin = Util::get_cur_time();
    map<string,set<string>> resultStr1 = queryNeighbor(confJson, servers, dbname, vertex_name, pred_name, flag, resNum);
    
    end = Util::get_cur_time();
    
    cout << "Query Time is : " << end - begin << " ms." << endl;
    cout << "Total num is : " << resNum << endl;
    cout << "There Answer is :" << endl;
    // cout << resultStr << endl;
    for (auto it : resultStr1) {
	for(auto i: it.second)
        cout << it.first << " " << i << endl;
    }
    cout<<"=========优化=========="<<endl;
    for (auto it : resultStr) {
        for(auto i: it.second)
            cout << it.first << " " << i << endl;
    }
    
    long endtime = Util::get_cur_time();
    cout << "cost time: " << endtime - begintime << " ms" << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    return 0;
}
