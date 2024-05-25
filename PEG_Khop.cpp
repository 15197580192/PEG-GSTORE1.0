/*
 * @Filename: PEG_Khop
 * @Author: Hu Jian
 * @Date: 2022-11-10 22:53:14
 * @LastEditTime: 2023-01-09 11:12:04
 * @LastEditors: Hu Zheyuan
 * @Description: compute all paths from u to v within k hops via edges labeled by labels in propset.
 */
#include <iostream>
#include <bits/stdc++.h>
#include <unordered_map>

#include "handler/Handler.h"
#include "util/util.h"
#include "crow/json.h"
#include "gStoreAPI/client.h"

using namespace std;

class Node
{
public:
    Node *prev;
    string id;
    string prop;
    Node(string _id) : id(_id) { prev = nullptr; }
};

void generateAns(Node *p, string pred_id, vector<string> &path_set)
{
    if (p == nullptr)
        return;

    generateAns(p->prev, pred_id, path_set);
    path_set.push_back(p->prop);
    path_set.push_back(p->id);
    // path_set.push_back(pred_id);
}

// 使用字符串进行分割
void Stringsplit(string str, const char split, vector<string> &res)
{
    if (str == "")
        return;
    // 在字符串末尾也加入分隔符，方便截取最后一段
    string strs = str + split;
    size_t pos = strs.find(split);

    // 若找不到内容则字符串搜索函数返回 npos
    while (pos != std::string::npos)
    {
        string temp = strs.substr(0, pos);
        res.push_back(temp);
        // 去掉已分割的字符串,在剩下的字符串中进行分割
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(split);
    }
}

// ./build/PEG_Khop

int main(int argc, char const *argv[])
{

    if (argc < 3)
    {
        cout << "============================================================" << endl;
        cout << "all paths from u to v within k hops via edges labeled by labels in propset. The way to use this program: " << endl;
        cout << "./build/PEG_Khop database_name <begin, end, propset, k>" << endl;
        cout << "Here is an example: " << endl;
        cout << "./build/PEG_Khop lubm10m \<\<http://www.Department4.University65.edu/GraduateStudent39\>,\<http://www.University65.edu\>\>,\<http://www.w3.org/1999/02/22-rdf-syntax-ns\#type\>,\<http://www.lehigh.edu/\~zhp2/2004/0401/univ-bench.owl\#memberOf\>,\<http://www.lehigh.edu/\~zhp2/2004/0401/univ-bench.owl\#subOrganizationOf\>,3" << endl;
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

    vector<string> strList;
    // 输入格式 begin， end， propset， k
    string str("<<http://www.Department4.University65.edu/GraduateStudent39>,<http://www.University65.edu>>,<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>,3");

    if (argc > 1)
    {
        dbname = string(argv[1]);
        str = string(argv[2]);
    }

    Stringsplit(str, ',', strList);
    // 获取k跳的值
    string num = strList.back();
    int number;
    stringstream ss;
    ss << num;
    ss >> number;
    /*
     * begin end props 起点 终点 谓词集合
     * 处理数据
     */
    string begin, end;
    vector<string> props;
    begin = strList[0];
    end = strList[1];
    for (int i = 2; i < strList.size() - 1; i++)
    {
        props.push_back(strList[i]);
    }
    begin = begin.replace(begin.find('<'), 1, "");
    end = end.replace(end.find('>'), 1, "");

    //    cout << begin << " " << end << " " << number << endl;
    // k跳
    queue<string> q;
    int count = 0;
    q.push(begin);

    unordered_map<string, Node *> i2node;
    i2node[begin] = new Node(begin);

    set<string> mark;
    // 起始节点先标记
    mark.insert(begin);
    while (!q.empty() && count < number)
    {
        int size = q.size();
        // 层次遍历

        while (size > 0)
        {
            // string cur = q.back();
            // 取队头?
            string cur = q.front();
            q.pop();
            for (auto iter : props)
            {
                string property = iter;
                // 调用分布式遍历 得到map<string,set<string>>
                map<string, set<string>> map = queryNeighbor(confJson, servers, dbname, cur, property, true, resNum);
                set<string> nextNode = map.begin()->second;
                for (auto it = nextNode.cbegin(); it != nextNode.cend(); it++)
                {
                    string node = *it;

                    // 判断是否被遍历过
                    if (mark.count(node) == 0)
                    { // 没有遍历过
                        q.push(node);

                        Node *tnode = new Node(node);
                        i2node[node] = tnode;
                        tnode->prev = i2node[cur];
                        tnode->prop = property;

                        // 终点标记无法继续找更长的路径
                        mark.insert(node);
                        // 找到目的点
                        if (node == end)
                        {
                            // 取消目的点的标记，保证可以继续找更长的路径
                            mark.erase(node);
                            vector<string> path_set;
                            generateAns(tnode, property, path_set);
                            // path_set.pop_back();

                            cout << "路径为：";
                            for (auto iter : path_set)
                            {
                                cout << iter << "";
                            }
                            cout << endl;
                        }
                    }
                }
            }

            size--;
        }
        count++;
    }

    //    cout << "答案不存在" << endl;

    long endtime = Util::get_cur_time();
    cout << "cost time: " << endtime - begintime << " ms" << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    return 0;
}
