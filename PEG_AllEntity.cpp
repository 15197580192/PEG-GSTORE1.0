/*
 * @Filename: PEG_AllEntity.cpp
 * @Author: Hu Zheyuan
 * @Date: 2022-11-22 08:46:08
 * @LastEditTime: 2022-12-05 15:42:57
 * @LastEditors: Hu Zheyuan
 * @Description: a test to query all vertexs neighbor
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
    if (argc < 2)
    {
        cout << "============================================================" << endl;
        cout << "Query all vertexs neighbor. The way to use this program: " << endl;
        cout << "./build/PEG_AllEntity database_name" << endl;
        cout << "============================================================" << endl;

        return 0;
    }
    long begintime = Util::get_cur_time();
    string serversListStr = readFile("conf/servers.json");
    crow::json::rvalue confJson = crow::json::load(serversListStr);

    vector<GstoreConnector *> servers;
    for (int i = 0; i < confJson["sites"].size(); i++)
    {
        servers.push_back(new GstoreConnector(
            confJson["sites"][i]["ip"].s(),
            (int)confJson["sites"][i]["port"],
            confJson["sites"][i]["http_type"].s(),
            confJson["sites"][i]["dbuser"].s(),
            confJson["sites"][i]["dbpasswd"].s()));
    }

    string dbname(argv[1]);
    long long resNum;

    long begin = Util::get_cur_time();
    
    // 查询所有实体
    set<string> resultStr = queryVertex(confJson, servers, dbname, resNum);
    
    long end = Util::get_cur_time();
    cout << "Query Time is : " << end - begin << " ms." << endl;
    cout << "Total num is : " << resNum << endl;
    // cout << "There Answer is :" << endl;
    
    ofstream outFile(dbname + "Entity.txt");
    for (auto it : resultStr)
    {
        cout << it << endl;
        outFile << it << endl;
    }
    outFile.close();

    long endtime = Util::get_cur_time();
    cout << "cost time: " << endtime - begintime << " ms" << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    return 0;
}
