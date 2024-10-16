/*
 * @Filename: PEG_Triangle.cpp
 * @Author: Hu Jian
 * @Date: 2022-12-05 14:51:22
 * @LastEditTime: 2023-01-09 21:09:11
 * @LastEditors: Hu Zheyuan
 * @Description: Compute the number of triangles in the graph, only considering the edges in the path with one label.
 */
#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <sstream>
#include <set>
#include <map>
#include <fstream>

#include "handler/Handler.h"
#include "util/util.h"
#include "crow/json.h"
#include "gStoreAPI/client.h"

using namespace std;

int num = 0;
int queryID;
string predID; // 属性
unordered_set<string> tri_set;
string dbname;
crow::json::rvalue confJson;
vector<GstoreConnector *> servers;

// 从实体文件获取
set<string> getEntityFromFile1(string file)
{
    set<string> entity;
    string line;
    ifstream in(file);
    while (getline(in, line))
    {
        entity.insert(line);
    }
    in.close();
    return entity;
}

// 从划分文件获取实体
set<string> getEntityFromFile(string file)
{
    set<string> entity;
    string line, node;
    ifstream in;
    in.open(file);
    while (in >> line && in >> node)
        entity.insert(line);
    in.close();
    return entity;
}

void triFind(string beginID)
{
    // cout << beginID << endl;
    unordered_set<string> outSet;
    unordered_set<string> inSet;

    // 出边邻居集合
    long long resNum;
    map<string, set<string>> num_of_out_neighbor = queryNeighbor(confJson, servers, dbname, beginID, predID, true, resNum);
    if (num_of_out_neighbor.size() == 0)
        return;
    set<string> nextNode_out = num_of_out_neighbor.begin()->second;
    for (auto it = nextNode_out.cbegin(); it != nextNode_out.cend(); it++)
    {
        string node = *it;
        outSet.insert(node);
    }

    // 入边邻居集合
    long long resNum1;
    map<string, set<string>> num_of_in_neighbor = queryNeighbor(confJson, servers, dbname, beginID, predID, false, resNum1);
    if (num_of_in_neighbor.size() == 0)
        return;
    set<string> nextNode_in = num_of_in_neighbor.begin()->second;
    for (auto it = nextNode_in.cbegin(); it != nextNode_in.cend(); it++)
    {
        string node = *it;
        inSet.insert(node);
    }

    // 三角形计数
    for (const string secID : outSet)
    {
        // 剔除自旋点
        if(secID==beginID) {
            continue;
        }
        
        // 获取secID的出边 判断是否在begin的入边集合中有重复的点 如果有 则+1
        long long resNum2;
        map<string, set<string>> num_of_out_neighbor_1 = queryNeighbor(confJson, servers, dbname, secID, predID, true, resNum2);

        set<string> nextNode_out1 = num_of_out_neighbor_1.begin()->second;
        for (auto it = nextNode_out1.cbegin(); it != nextNode_out1.cend(); it++)
        {
            string t = *it;
            if (inSet.find(t) != inSet.end())
            {
                vector<string> newPath{beginID, secID, t};
                sort(newPath.begin(), newPath.end());
                string path = "";
                // 去除三角形重复
                for (auto it = newPath.begin(); it != newPath.end(); it++)
                {
                    string str = *it;
                    path = path + str;
                }
                if (tri_set.find(path) == tri_set.end())
                {
                    cout<<path<<endl;
                    tri_set.insert(path);
                    num++;
                }
            }
        }
    }
}

int main(int argc, char const *argv[])
{

    if (argc < 3)
    {
        cout << "============================================================" << endl;
        cout << "Compute the number of triangles in the graph, only considering the edges in the path with one label. The way to use this program: " << endl;
        cout << "./build/PEG_Triangle database_name label_name" << endl;
        cout << "============================================================" << endl;

        return 0;
    }

    predID = "<dfjshfdjksh>";

    // 单一谓词输入，扩展多谓词？
    if (argc == 3)
    {
        dbname = argv[1];
        predID = argv[2];
    }

    // 节点信息输入
    string serversListStr = readFile("conf/servers.json");
    confJson = crow::json::load(serversListStr);
    for (int i = 0; i < confJson["sites"].size(); i++)
    {
        servers.push_back(new GstoreConnector(
            confJson["sites"][i]["ip"].s(),
            (int)confJson["sites"][i]["port"],
            confJson["sites"][i]["http_type"].s(),
            confJson["sites"][i]["dbuser"].s(),
            confJson["sites"][i]["dbpasswd"].s()));
    }

    // 读取文件顶点集
    set<string> nodes = getEntityFromFile("entity/" + dbname + "InternalPoints.txt");

    long begintime = Util::get_cur_time();
    for (auto i : nodes)
    {
        triFind(i);
    }
    long endtime = Util::get_cur_time();

    cout << "三角形个数为：" << num;
    cout << "cost time: " << endtime - begintime << " ms" << endl;
}