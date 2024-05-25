/*
 * @Filename: PEG_Triangle2.cpp
 * @Author: Hu Zheyuan
 * @Date: 2023-01-23 10:10:10
 * @LastEditTime: 2023-01-23 10:59:40
 * @LastEditors: Hu Zheyuan
 * @Description: 查询重写计算三角形
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
    if(argc < 3)
    {
        cout << "============================================================" << endl;
        cout << "Compute the number of triangles in the graph, only considering the edges in the path with one label. The way to use this program: " << endl;
        cout << "./PEG_Triangle2 database_name /path/to/your/sparql/file" << endl;
        cout << "============================================================" << endl;

        return 0;
    }

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
    string spqlpath(argv[2]);

    string sparql = readFile(spqlpath);

    long begin = Util::get_cur_time();
    long long resNum;
    string resultStr = query(confJson, servers, dbname, sparql, resNum);

    // 处理含自旋点的三角形
    // 思路：去除列相同的结果
    string resultStr1;
    long long resNum1=0;

    vector<string> lines;
    joiner::split(resultStr, lines, "\n");
    resultStr1+=lines[0]+"\n";
    for(int i=1;i<lines.size();i++) {
        vector<string> line;
        string temp = lines[i];
        joiner::split(temp, line, "\t");
        set<string> line_set(line.begin(),line.end());
        if(line.size()==line_set.size()) {
            // set和vector大小相同时说明不是自旋点的结果
            resultStr1+=lines[i]+"\n";
            resNum1++;   
        }
    }
    long end = Util::get_cur_time();
    cout << "Query Time is : " << end - begin << " ms." << endl;
    cout << "Total num is : " << resNum1 << endl;
    cout << "There Answer is :" << endl;
    cout << resultStr1 << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    
    return 0;
}
