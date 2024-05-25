/*
 * @Filename: PEG_ClosenessCentrality.cpp
 * @Author: Hu Jian
 * @Date: 2022-11-14 10:57:48
 * @LastEditTime: 2023-01-09 20:56:01
 * @LastEditors: Hu Zheyuan
 * @Description: Compute the closeness centrality of a vertex.
 * 
 * 紧密中心算法
 * 输入：一个点
 * 输出：一个值
 * @return
 */
#include <iostream>
#include <bits/stdc++.h>
#include "handler/Handler.h"
#include "util/util.h"
#include "crow/json.h"
#include "gStoreAPI/client.h"

using namespace std;

// 顶点索引
std::unordered_map<std::string, int> entity2id;
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

int main(int argc, char const *argv[])
{

    if (argc < 3)
    {
        cout << "============================================================" << endl;
        cout << "Compute the closeness centrality of a vertex. The way to use this program: " << endl;
        cout << "./build/PEG_ClosenessCentrality database_name vertex_name" << endl;
        cout << "Here is an example: " << endl;
        cout << "./build/PEG_ClosenessCentrality lubm10m \<http://www.Departmento.Universitye.edu/Coursee\>" << endl;
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
    string dbname = "lubm10m";
    // map<string,set<string>>的结果数目
    long long resNum;
    // 节点、图中节点总数、答案
    // string begin = "<http://www.Departmento.Universitye.edu/Coursee>";
    string begin = "<http://www.Department4.University65.edu>";

    if (argc > 1)
    {
        dbname = string(argv[1]);
        begin = string(argv[2]);
    }
    entity2id = init("entity/" + dbname + "InternalPoints.txt");

    cout << "dbname:" << dbname << endl;
    cout << "begin:" << begin << endl;
    int number = 0;
    double ans = 0.0;
    // 标记点有没有被访问 排除0 1点
    int vertexNum = 0;
    set<string> mark;
    mark.insert("0");
    mark.insert("1");
    queue<string> q;

    q.push(begin);
    int distance = 1;

    // 层次遍历记录点和距离
    while (!q.empty())
    {
        int size = q.size();
        // 层次遍历
        while (size > 0)
        {
            string cur = q.back();
            q.pop();
            // 调用查询邻居 得到map<string,set<string>>
            map<string, set<string>> map_out = queryNeighbor(confJson, servers, dbname, cur, "", true, resNum);
            map<string, set<string>> map_in = queryNeighbor(confJson, servers, dbname, cur, "", false, resNum);
            // int subPartID = entity2id[cur];
            // map<string, set<string>> map_out = queryNeighbor2(confJson, servers, dbname, cur, "", true, resNum,subPartID);
            // map<string, set<string>> map_in = queryNeighbor2(confJson, servers, dbname, cur, "", false, resNum,subPartID);
            // 取出邻居点集合
            for (auto out : map_out)
            {
                set<string> nextNode = out.second;
                // set<string> nextNode = map_out.begin()->second;
                for (auto it = nextNode.cbegin(); it != nextNode.cend(); it++)
                {
                    
                    string node = *it;
                    // cout<<node<<endl;
                    // 判断是否被遍历过
                    if (mark.count(node) == 0)
                    { // 没有遍历过
                        q.push(node);
                        // cout << node << endl;
                        mark.insert(node);
                        // 计算距离之和
                        number += distance;
                        vertexNum++;
                    }
                }
            }

            for (auto in : map_in)
            {
                set<string> nextNode_in = in.second;
                // set<string> nextNode_in = map_in.begin()->second;
                for (auto it = nextNode_in.cbegin(); it != nextNode_in.cend(); it++)
                {
                    string node = *it;
                    // 判断是否被遍历过
                    if (mark.count(node) == 0)
                    { // 没有遍历过
                        q.push(node);
                        mark.insert(node);
                        // 计算距离之和
                        number += distance;
                        vertexNum++;
                    }
                }
            }

            size--;
        }
        distance++;
    }

    ans = 1.0 * vertexNum / distance;
    cout << ans << endl;

    long endtime = Util::get_cur_time();
    cout << "cost time: " << endtime - begintime << " ms" << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    return 0;
}
