/*
 * @Filename: PEG_Triangle1.cpp
 * @Author: Hu Jian
 * @Date: 2022-12-05 14:51:22
 * @LastEditTime: 2023-01-12 09:54:23
 * @LastEditors: Hu Zheyuan
 * @Description: Compute the number of triangles in the graph, only considering the edges in the path with one label.
 *               PEG_Triangle.cpp优化版本（找邻居制定节点查询，增加入、出邻居缓存）
 */
#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
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
// 顶点索引
std::unordered_map<std::string, int> entity2id;
map<string, map<string, set<string>>> neighbor;  // 出边邻居结果
map<string, map<string, set<string>>> neighbor1; // 入边邻居结果
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

// 从划分文件获取顶点所在节点索引
std::unordered_map<std::string, int> init(string dividefile)
{
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
    return entity2id;
}

void triFind(string beginID)
{
    cout << beginID << endl;
    unordered_set<string> outSet;
    unordered_set<string> inSet;

    // 出边邻居集合
    long long resNum;
    // map<string, set<string>> num_of_out_neighbor = queryNeighbor(confJson, servers, dbname, beginID, predID, true, resNum);
    int subPartID = entity2id[beginID];
    cout << subPartID << endl;

    // map<string, set<string>> num_of_out_neighbor = queryNeighbor2(confJson, servers, dbname, beginID, predID, true, resNum, subPartID);

    // if (num_of_out_neighbor.size() == 0)
    //     return;
    // set<string> nextNode_out = num_of_out_neighbor.begin()->second;
    map<string, set<string>> num_of_out_neighbor;
    if (neighbor[beginID][predID].size() == 0)
    {
        // 没有查过邻居
        num_of_out_neighbor = queryNeighbor2(confJson, servers, dbname, beginID, predID, true, resNum, subPartID);
        neighbor[beginID][predID] = num_of_out_neighbor[predID];
    }
    else
    {
        // 已经查过邻居
        num_of_out_neighbor[predID] = neighbor[beginID][predID];
    }
    // 没有邻居直接结束
    if (num_of_out_neighbor[predID].size() == 0)
        return;
    // 得到出邻居
    set<string> nextNode_out = num_of_out_neighbor[predID];

    for (auto it = nextNode_out.cbegin(); it != nextNode_out.cend(); it++)
    {
        string node = *it;
        outSet.insert(node);
    }

    // 入边邻居集合
    long long resNum1;
    // map<string, set<string>> num_of_in_neighbor = queryNeighbor(confJson, servers, dbname, beginID, predID, false, resNum1);
    int subPartID1 = entity2id[beginID];

    // map<string, set<string>> num_of_in_neighbor = queryNeighbor2(confJson, servers, dbname, beginID, predID, false, resNum1, subPartID1);

    // if (num_of_in_neighbor.size() == 0)
    //     return;
    // set<string> nextNode_in = num_of_in_neighbor.begin()->second;
    map<string, set<string>> num_of_in_neighbor;
    if (neighbor1[beginID][predID].size() == 0)
    {
        // 没有查过邻居
        num_of_in_neighbor = queryNeighbor2(confJson, servers, dbname, beginID, predID, false, resNum1, subPartID1);
        neighbor1[beginID][predID] = num_of_in_neighbor[predID];
    }
    else
    {
        // 已经查过邻居
        num_of_in_neighbor[predID] = neighbor1[beginID][predID];
    }
    // 没有邻居直接结束
    if (num_of_in_neighbor[predID].size() == 0)
        return;
    // 得到入邻居
    set<string> nextNode_in = num_of_in_neighbor[predID];

    for (auto it = nextNode_in.cbegin(); it != nextNode_in.cend(); it++)
    {
        string node = *it;
        cout<<"in:"<<node<<endl;
        inSet.insert(node);
    }

    // 三角形计数
    for (const string secID : outSet)
    {
        // 剔除自旋点
        if (secID == beginID)
        {
            continue;
        }

        // 获取secID的出边 判断是否在begin的入边集合中有重复的点 如果有 则+1
        long long resNum2;
        // map<string, set<string>> num_of_out_neighbor_1 = queryNeighbor(confJson, servers, dbname, secID, predID, true, resNum2);
        int subPartID2 = entity2id[secID];

        // map<string, set<string>> num_of_out_neighbor_1 = queryNeighbor2(confJson, servers, dbname, secID, predID, true, resNum2, subPartID2);
        // set<string> nextNode_out1 = num_of_out_neighbor_1.begin()->second;

        map<string, set<string>> num_of_out_neighbor_1;
        if (neighbor[secID][predID].size() == 0)
        {
            // 没有查过邻居
            num_of_out_neighbor_1 = queryNeighbor2(confJson, servers, dbname, secID, predID, true, resNum2, subPartID2);
            neighbor[secID][predID] = num_of_out_neighbor_1[predID];
        }
        else
        {
            // 已经查过邻居
            num_of_out_neighbor_1[predID] = neighbor[secID][predID];
        }
        // 没有邻居直接结束
        if (num_of_out_neighbor_1[predID].size() == 0)
            return;
        // 得到出邻居
        set<string> nextNode_out1 = num_of_out_neighbor_1[predID];

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
    set<string> nodes; // = getEntityFromFile("entity/" + dbname + "InternalPoints.txt");
    entity2id = init("entity/" + dbname + "InternalPoints.txt");
    for (auto node : entity2id)
    {
        nodes.insert(node.first);
    }

    long begintime = Util::get_cur_time();
    for (auto i : nodes)
    {
        triFind(i);
    }
    long endtime = Util::get_cur_time();

    cout << "三角形个数为：" << num;
    cout << "cost time: " << endtime - begintime << " ms" << endl;
}