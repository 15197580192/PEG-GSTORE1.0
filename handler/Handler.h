#ifndef HANDLER_H
#define HANDLER_H

#include <fstream>
#include <string>
#include <vector>
#include <set>
#include <queue>
#include <unordered_map>
#include <iostream>

#include "crow/json.h"
#include "../util/FileTransfer.h"
#include "../util/RemoteCmdInvoker.h"
#include "../util/util.h"
#include "../gStoreAPI/client.h"
#include "../queryDecompose/Query/QueryTree.h"
#include "../queryDecompose/Query/GeneralEvaluation.h"
#include "../joiner/joiner.h"

// 条件编译解决queryDecompose/Parser/SparqlParser.h和rapidjson定义冲突
#ifdef A
#undef A
#endif
#include "../rapidjson/document.h"

using namespace rapidjson;

string jsonToTxt(const string &jvStr);

int divide(std::string ntfile, std::string dividefile)
{
    std::ifstream ifs_dividefile;
    ifs_dividefile.open(dividefile);
    int num = 0;
    std::string entity;
    int partID;
    std::unordered_map<std::string, int> entity2id;

    while (ifs_dividefile >> entity && ifs_dividefile >> partID)
    {
        entity2id.insert(std::pair<std::string, int>(entity, partID));
        num = partID > num ? partID : num;
    }
    num++;
    
    std::ifstream ifs_ntfile;
    ifs_ntfile.open(ntfile);

    std::vector<std::ofstream> outfiles;
    outfiles.resize(num);
    std::string prefix = ntfile.substr(0, ntfile.find_last_of('.'));
    for (int i = 0; i < num; i++)
        outfiles[i].open(prefix + to_string(i) + ".nt");

    std::string line;
    std::vector<std::set<std::string>> s;
    s.resize(num);
    while (getline(ifs_ntfile, line))
    {
        std::string subject;
        std::string predict;
        std::string object;
        int lidx, ridx;
        
        lidx = line.find_first_of('<');
        ridx = line.find_first_of('>');
        subject = line.substr(lidx, ridx-lidx+1);

        lidx = line.find_first_of('<', ridx+1);
        ridx = line.find_first_of('>', ridx+1);
        predict = line.substr(lidx, ridx-lidx+1);

        lidx = line.find_first_of('<', ridx+1);
        ridx = line.find_first_of('>', ridx+1);

        int subPartID = entity2id[subject];
        outfiles[subPartID] << line << "\n";
        s[subPartID].insert(subject + "\t" + "<http://property/isInCurrPart>" + "\t" + "\"1\" .\n");
        
        if(lidx != std::string::npos)
        {
            object = line.substr(lidx, ridx-lidx+1);
            if(entity2id.find(object) != entity2id.end())
            {
                int objPartID = entity2id[object];
                if(subPartID == objPartID)
                {
                    s[subPartID].insert(object + "\t" + "<http://property/isInCurrPart>" + "\t" + "\"1\" .\n");
                }
                else
                {
                    s[subPartID].insert(object + "\t" + "<http://property/isInCurrPart>" + "\t" + "\"0\" .\n");

                    outfiles[objPartID] << line << "\n";
                    s[objPartID].insert(subject + "\t" + "<http://property/isInCurrPart>" + "\t" + "\"0\" .\n");
                    s[objPartID].insert(object + "\t" + "<http://property/isInCurrPart>" + "\t" + "\"1\" .\n");
                }
            }
        }
    }

    for (int i = 0; i < num; i++)
        for (auto iter = s[i].begin(); iter != s[i].end(); iter++)
            outfiles[i] << *iter << std::endl;

    for (int i = 0; i < num; i++)
        outfiles[i].close();
    ifs_ntfile.close();
    ifs_dividefile.close();

    return num;
}

int build(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, std::string &dbname, std::string &dbpath, std::string &divfilepath)
{
    // 数据图划分，得到一些nt文件的路径
    std::string txt_name = dbpath;                                     // 源数据图路径
    std::string sign = ("2" == "1") ? " " : "\t";                      // 分隔符
    std::string out_file = dbpath.substr(0, dbpath.find_last_of('.')); // 输出文件的前缀

    int part = divide(dbpath, divfilepath);

    // 分片分发
    TransferThreadArgs tft[part];
    pthread_t thread[part];
    for (int i = 0; i < part; i++)
    {
        crow::json::rvalue site = confJson["sites"][i];

        tft[i].ft = new FileTransfer(site["ip"].s(), site["user"].s());
        tft[i].localFilePath = out_file + to_string(i) + ".nt";
        tft[i].remoteFilePath = "/tmp/" + dbname + to_string(i) + ".nt";
        if (pthread_create(&thread[i], NULL, transferThread, &tft[i]) != 0)
        {
            throw runtime_error("creates transfer thread error!");
        }
    }
    for (int i = 0; i < part; i++)
    {
        pthread_join(thread[i], NULL);
    }

    // TODO 调用gStore http api 建库
    buildThreadArgs bta[part];
    pthread_t build_pthrd[part];
    for (int i = 0; i < part; i++)
    {
        crow::json::rvalue site = confJson["sites"][i];
        bta[i].gcp = servers[i];
        bta[i].dbname = dbname;
        bta[i].ntpath = "/tmp/" + dbname + to_string(i) + ".nt";
        if (pthread_create(&build_pthrd[i], NULL, buildThread, &bta[i]) != 0)
        {
            throw runtime_error("creates build thread error!");
        }
    }
    for (int i = 0; i < part; i++)
        pthread_join(build_pthrd[i], NULL);

    for (int i = 0; i < part; i++)
    {
        delete tft[i].ft;
    }

    crow::json::rvalue rdb = crow::json::load(readFile("conf/dblist.json"));
    crow::json::wvalue wdb(rdb);
    std::vector<crow::json::wvalue> remainDB;
    for (int i = 0; i < rdb["dbs"].size(); i++)
        remainDB.push_back(std::move(wdb["dbs"][i]));
    crow::json::wvalue newDB;
    newDB["dbName"] = dbname;
    remainDB.push_back(std::move(newDB));
    wdb["dbs"] = std::move(remainDB);
    writeFile("conf/dblist.json", wdb.dump());

    return 0;
}

std::string query(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, std::string &dbname, std::string query, long long &resNum)
{

    int part = confJson["sites"].size();

    // load
    buildThreadArgs bta[part];
    pthread_t load_pthrds[part];
    for (int i = 0; i < part; i++)
    {
        crow::json::rvalue site = confJson["sites"][i];
        bta[i].gcp = servers[i];
        bta[i].dbname = dbname;
        if (pthread_create(&load_pthrds[i], NULL, loadThread, &bta[i]) != 0)
        {
            throw runtime_error("creates load thread error!");
        }
    }
    for (int i = 0; i < part; i++)
        pthread_join(load_pthrds[i], NULL);
    std::cout << "all dbs invoked load." << std::endl;

    // query
    // 查询分解、组合
    long querytime = Util::get_cur_time();
    std::string _query_str = query;
    int PPQueryVertexCount = -1, vec_size = 0, star_tag = 0;
    QueryTree::QueryForm query_form = QueryTree::Ask_Query;
    GeneralEvaluation parser_evaluation;

    parser_evaluation.parseGetQuery(_query_str, PPQueryVertexCount, query_form, star_tag);
    printf("PPQueryVertexCount = %d\n", PPQueryVertexCount);
    std::vector<std::vector<std::string>> &total_queries = parser_evaluation.queriesDecomposed;
    long querydividetime = Util::get_cur_time();
    std::cout << "Divide Query cost " << querydividetime - querytime << " ms." << std::endl;

    // query on each site.
    std::vector<std::vector<std::string>> total_results;
    joiner joinobj;
    std::map<std::string, std::vector<std::string> *> query2res;
    int queryCnt = 0;
    for (int k = 0; k < total_queries.size(); k++)
        queryCnt += total_queries[k].size();
    std::cout << "The number of queries decomposed is " << queryCnt << std::endl;
    for (int k = 0; k < total_queries.size(); k++)
    {
        std::cout << "===================== Query partition method " << k << " =====================" << std::endl;
        long querybegin = Util::get_cur_time();
        std::vector<std::string> &queries = total_queries[k];
        // 执行查询
        std::queue<std::vector<std::string> *> results;
        // 并行查询
        pthread_t thread[queries.size()][servers.size()];
        QNV qnv[queries.size()][servers.size()];
        long queryTime[servers.size()];
        // vector<long> queryTime[servers.size()];
        memset(queryTime, 0, sizeof queryTime);
        std::set<int> skipQueryNum;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
                for (int j = 0; j < servers.size(); j++)
                {
                    qnv[i][j].queryId = i;
                    qnv[i][j].serverId = j;
                    qnv[i][j].server = servers[j];
                    qnv[i][j].dbname = dbname;
                    qnv[i][j].format = "json";
                    qnv[i][j].query = queries[i];
                    std::cout << "query " << i << " on server " << j << " ";
                    if (pthread_create(&thread[i][j], NULL, queryThread, &qnv[i][j]) != 0)
                    {
                        throw runtime_error("creates thread error!");
                    }
                    else
                        std::cout << "creates thread success!" << std::endl;
                }
            else
            {
                skipQueryNum.insert(i);
                std::cout << "query " << i << " has already executed." << std::endl;
            }
        }
        for (int i = 0; i < queries.size(); i++)
        {
            if (skipQueryNum.find(i) == skipQueryNum.end())
                for (int j = 0; j < servers.size(); j++)
                    pthread_join(thread[i][j], NULL);
        }
        // 合并并行查询后的结果
        std::cout << "Now merge the results..." << std::endl;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
            {
                bool firstRes = true;
                std::set<std::string> s; // 去掉重复的结果
                std::string head;
                for (int j = 0; j < servers.size(); j++)
                {
                    std::string temp_res1 = jsonToTxt(qnv[i][j].res);
                    // std::string &temp_res = qnv[i][j].res;
                    std::string &temp_res = temp_res1;
                    // std::cout << temp_res ;
                    if (temp_res.empty())
                    {
                        std::cout << "query " << i << " server " << j << " No response " << std::endl;
                        continue;
                    }
                    else if (temp_res.find("[empty result]") != std::string::npos) // 查出的结果是空集
                    {
                        std::cout << "query " << i << " server " << j << " Empty" << std::endl;
                        continue;
                    }
                    else
                    {
                        std::cout << "query " << i << " server " << j << " Get Answer ";
                    }

                    std::vector<std::string> lines;
                    joiner::split(temp_res, lines, "\n");
                    std::cout << lines.size() - 1 << std::endl;
                    std::vector<std::string>::iterator iter = lines.begin();
                    head = *iter;
                    // queryTime[j] += stol(*iter);
                    // queryTime[j].push_back(stol(*iter));
                    ++iter;
                    if (firstRes) // 第一次添加结果
                    {
                        firstRes = false;
                    }
                    for (; iter != lines.end(); iter++)
                        s.insert(*iter);
                }
                std::vector<std::string> *resOfQuery = new std::vector<std::string>; // 是否要new
                if (!firstRes)                                                       // 有结果
                {
                    resOfQuery->push_back(head);
                    resOfQuery->insert(resOfQuery->end(), s.begin(), s.end());
                }
                results.push(resOfQuery);
                query2res[queries[i]] = resOfQuery;
            }
            else
            {
                results.push(query2res[queries[i]]);
                std::cout << "Get result of executed query " << i << "." << std::endl;
            }
        }
        std::cout << "Before Join results size is " << results.size() << std::endl;

        // join
        long joinbegin = Util::get_cur_time();
        std::vector<std::string> finalRes = joinobj.join(results);
        std::cout << "joinobj.join " << finalRes.size() << std::endl;

        long queryend = Util::get_cur_time();
        std::cout << "Join cost " << queryend - joinbegin << " ms." << std::endl;
        std::cout << "Execute queries and join, cost " << queryend - querybegin << " ms." << std::endl;
        std::cout << "===================== Query partition method " << k << " =====================" << std::endl
                  << std::endl;
        // for(int i=0;i<finalRes.size();i++) cout<<finalRes[i];
        total_results.push_back(finalRes);
    }

    // unload
    // pthread_t unload_pthrds[part];
    // for (int i = 0; i < part; i++)
    //     if (pthread_create(&unload_pthrds[i], NULL, unloadThread, &bta[i]) != 0)
    //         throw runtime_error("creates unload thread error!");
    // for (int i = 0; i < part; i++)
    //     pthread_join(unload_pthrds[i], NULL);

    // union
    long unionbegin = Util::get_cur_time();
    std::vector<std::string> unionRes = total_results[0];
    for (int i = 1; i < total_results.size(); i++)
    {
        unionRes = joinobj.Union(unionRes, total_results[i]);
    }
    std::cout << "Begin to Union." << std::endl;
    long unionend = Util::get_cur_time();
    std::cout << "Union res of all method, cost " << unionend - unionbegin << " ms." << std::endl;
    for (std::map<std::string, std::vector<std::string> *>::iterator it = query2res.begin(); it != query2res.end(); it++) // 要改
        delete it->second;

    resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    std::string finalResult;
    for (int i = 0; i < unionRes.size(); i++)
    {
        finalResult += (unionRes.at(i) + "\n");
    }

    return finalResult;
}

int deleteDB(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, const std::string &dbname)
{
    int part = confJson["sites"].size();
    buildThreadArgs bta[part];
    pthread_t delete_pthrds[part];
    for (int i = 0; i < part; i++)
    {
        crow::json::rvalue site = confJson["sites"][i];
        bta[i].gcp = servers[i];
        bta[i].dbname = dbname;
        if (pthread_create(&delete_pthrds[i], NULL, deleteThread, &bta[i]) != 0)
            throw runtime_error("creates delete thread error!");
    }
    for (int i = 0; i < part; i++)
        pthread_join(delete_pthrds[i], NULL);

    crow::json::rvalue rdb = crow::json::load(readFile("conf/dblist.json"));
    crow::json::wvalue wdb(rdb);
    std::vector<crow::json::wvalue> remainDB;
    for (int i = 0; i < rdb["dbs"].size(); i++)
    {
        if (rdb["dbs"][i]["dbName"] == dbname)
            continue;
        remainDB.push_back(std::move(wdb["dbs"][i]));
    }
    wdb["dbs"] = std::move(remainDB);
    writeFile("conf/dblist.json", wdb.dump());

    return 0;
}

string jsonToTxt(const string &jvStr)
{
    string res = "";
    Document document;
    vector<string> vars;
    if (document.Parse(jvStr.c_str()).HasParseError())
    {
        std::cout << "JsonParseError" << endl;
        return "JsonParseError";
    }
    if (!document.HasMember("head") || !document.HasMember("results"))
    {
        return "[empty result]";
    }
    // if (document.Parse(jvStr.c_str()).HasParseError() || !document.HasMember("head") || !document.HasMember("results"))
    // {
    //     return "[empty result]";
    // }

    Value &jvObject = document["head"];
    Value &jvObject1 = document["results"];
    if (!jvObject.IsObject() || !jvObject.HasMember("vars") || !jvObject1.IsObject() || !jvObject1.HasMember("bindings"))
    {
        return "[empty result]";
    }

    Value &jv = jvObject["vars"];
    for (SizeType i = 0; i < jv.Size(); i++)
    {
        res += "?";
        res += jv[i].GetString();
        vars.push_back(jv[i].GetString());
        res += "\t";
    }
    // 末尾\t替换为\n
    res.pop_back();
    res += "\n";

    Value &jv1 = jvObject1["bindings"];
    if (jv1.Size() == 0)
    {
        return "[empty result]";
    }

    for (SizeType i = 0; i < jv1.Size(); i++)
    {
        Value &tmp = jv1[i];
        for (int t = 0; t < vars.size(); t++)
        {
            Value &tmp1 = tmp[vars[t].c_str()];
            string type = tmp1["type"].GetString();
            string value = tmp1["value"].GetString();
            if (type == "uri")
            {
                value = "<" + value + ">";
            }
            else if (type == "literal")
            {
                value = "\"" + value + "\"";
            }
            else if (type == "typed-literal")
            {
                // eg:“3”^^<http://www.w3.org/2001/XMLSchema#integer>
                // json返回type,value,datatype
                // datatype忽略?
                value = "\"" + value + "\"";
            }
            res += value;
            res += "\t";
        }
        // 末尾\t替换为\n
        res.pop_back();
        res += "\n";
    }
    // 去掉多余的\n
    res.pop_back();

    return res;
}

// 生成sql语句（BGP）
// predicate: 为空表示无谓词限制
// in_out: true 出边，false 入边
std::string neighborSql(std::string vertex, std::string predicate, bool in_out)
{
    if (vertex.empty())
    {
        return "";
    }
    // string sub = '<' + vertex + '>';
    // string pred = '<' + predicate + '>';
    string sub = vertex;
    string pred = predicate;

    string obj = "?o";
    string sql = "select * where {\n";

    if (predicate.empty())
    {
        pred = "?p";
    }

    if (in_out == false)
    {
        obj = sub;
        sub = "?s";
    }

    sql = sql + sub + '\t' + pred + '\t' + obj + " .\n}";
    return sql;
}

// 属性没有尖括号
std::string neighborSql1(std::string vertex, std::string predicate, bool in_out)
{
    if (vertex.empty())
    {
        return "";
    }
    string sub = '<' + vertex + '>';
    string pred = '<' + predicate + '>';
    // string sub = vertex ;
    // string pred = predicate ;

    string obj = "?o";
    string sql = "select * where {\n";

    if (predicate.empty())
    {
        pred = "?p";
    }

    if (in_out == false)
    {
        obj = sub;
        sub = "?s";
    }

    sql = sql + sub + '\t' + pred + '\t' + obj + " .\n}";
    return sql;
}

vector<string> split(const string &str, const string &pattern)
{
    vector<string> res;
    if (str == "")
        return res;
    // 在字符串末尾也加入分隔符，方便截取最后一段
    string strs = str + pattern;
    size_t pos = strs.find(pattern);

    while (pos != strs.npos)
    {
        string temp = strs.substr(0, pos);
        res.push_back(temp);
        // 去掉已分割的字符串,在剩下的字符串中进行分割
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(pattern);
    }

    return res;
}

std::map<string, set<string>> queryNeighbor(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, std::string &dbname, std::string vertex, std::string predicate, bool in_out, long long &resNum)
{

    int part = confJson["sites"].size();

    // // load
    // buildThreadArgs bta[part];
    // pthread_t load_pthrds[part];
    // for (int i = 0; i < part; i++)
    // {
    //     crow::json::rvalue site = confJson["sites"][i];
    //     bta[i].gcp = servers[i];
    //     bta[i].dbname = dbname;
    //     if (pthread_create(&load_pthrds[i], NULL, loadThread, &bta[i]) != 0)
    //     {
    //         throw runtime_error("creates load thread error!");
    //     }
    // }
    // for (int i = 0; i < part; i++)
    //     pthread_join(load_pthrds[i], NULL);
    // std::cout << "all dbs invoked load." << std::endl;

    // query
    // 查询生成
    std::vector<std::vector<std::string>> total_queries;
    std::vector<std::string> query;
    query.push_back(neighborSql(vertex, predicate, in_out));
    // 弃用，改用对sparql进行url编码
    // if((vertex.find("?username=") == string::npos)&&(vertex.find("?password=") == string::npos))
    //     query.push_back(neighborSql(vertex, predicate, in_out));
    total_queries.push_back(query);

    // query on each site.
    std::vector<std::vector<std::string>> total_results;
    joiner joinobj;
    std::map<std::string, std::vector<std::string> *> query2res;
    int queryCnt = 0;
    for (int k = 0; k < total_queries.size(); k++)
        queryCnt += total_queries[k].size();
    // std::cout << "The number of queries decomposed is " << queryCnt << std::endl;
    for (int k = 0; k < total_queries.size(); k++)
    {
        // std::cout << "===================== Query partition method " << k << " =====================" << std::endl;
        long querybegin = Util::get_cur_time();
        std::vector<std::string> &queries = total_queries[k];
        // 执行查询
        std::queue<std::vector<std::string> *> results;
        // 并行查询
        pthread_t thread[queries.size()][servers.size()];
        QNV qnv[queries.size()][servers.size()];
        long queryTime[servers.size()];
        // vector<long> queryTime[servers.size()];
        memset(queryTime, 0, sizeof queryTime);
        std::set<int> skipQueryNum;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
                for (int j = 0; j < servers.size(); j++)
                {
                    qnv[i][j].queryId = i;
                    qnv[i][j].serverId = j;
                    qnv[i][j].server = servers[j];
                    qnv[i][j].dbname = dbname;
                    qnv[i][j].format = "json";
                    qnv[i][j].query = queries[i];
                    // std::cout << "query " << i << " on server " << j << " ";
                    if (pthread_create(&thread[i][j], NULL, queryThread1, &qnv[i][j]) != 0)
                    {
                        throw runtime_error("creates thread error!");
                    }
                    // else
                    // std::cout << "creates thread success!" << std::endl;
                }
            else
            {
                skipQueryNum.insert(i);
                // std::cout << "query " << i <<" has already executed." << std::endl;
            }
        }
        for (int i = 0; i < queries.size(); i++)
        {
            if (skipQueryNum.find(i) == skipQueryNum.end())
                for (int j = 0; j < servers.size(); j++)
                    pthread_join(thread[i][j], NULL);
        }
        // 合并并行查询后的结果
        // std::cout << "Now merge the results..." << std::endl;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
            {
                bool firstRes = true;
                std::set<std::string> s; // 去掉重复的结果
                std::string head;
                for (int j = 0; j < servers.size(); j++)
                {
                    std::string temp_res1 = jsonToTxt(qnv[i][j].res);
                    while (temp_res1 == "JsonParseError")
                    {
                        // 解析失败查询重新执行
                        cout << "query replay" << endl;
                        pthread_t threadt;
                        if (pthread_create(&threadt, NULL, queryThread1, &qnv[i][j]) != 0)
                        {
                            throw runtime_error("creates thread error!");
                        }
                        pthread_join(threadt, NULL);
                        // 解析新的查询结果
                        temp_res1 = jsonToTxt(qnv[i][j].res);
                        cout << qnv[i][j].res << endl;
                    }
                    // std::string &temp_res = qnv[i][j].res;
                    std::string &temp_res = temp_res1;
                    // std::cout << temp_res ;
                    if (temp_res.empty())
                    {
                        // std::cout << "query " << i << " server " << j << " No response " << std::endl;
                        continue;
                    }
                    else if (temp_res.find("[empty result]") != std::string::npos) // 查出的结果是空集
                    {
                        // std::cout << "query " << i << " server " << j << " Empty" << std::endl;
                        continue;
                    }
                    else
                    {
                        // std::cout << "query " << i << " server " << j << " Get Answer ";
                    }

                    std::vector<std::string> lines;
                    joiner::split(temp_res, lines, "\n");
                    // std::cout << lines.size() - 1 << std::endl;
                    std::vector<std::string>::iterator iter = lines.begin();
                    head = *iter;
                    // queryTime[j] += stol(*iter);
                    // queryTime[j].push_back(stol(*iter));
                    ++iter;
                    if (firstRes) // 第一次添加结果
                    {
                        firstRes = false;
                    }
                    for (; iter != lines.end(); iter++)
                        s.insert(*iter);
                }
                std::vector<std::string> *resOfQuery = new std::vector<std::string>; // 是否要new
                if (!firstRes)                                                       // 有结果
                {
                    resOfQuery->push_back(head);
                    resOfQuery->insert(resOfQuery->end(), s.begin(), s.end());
                }
                results.push(resOfQuery);
                query2res[queries[i]] = resOfQuery;
            }
            else
            {
                results.push(query2res[queries[i]]);
                // std::cout << "Get result of executed query " << i << "." << std::endl;
            }
        }
        // std::cout << "Before Join results size is " << results.size() << std::endl;

        // join
        long joinbegin = Util::get_cur_time();
        std::vector<std::string> finalRes = joinobj.join(results);
        // std::cout<< "joinobj.join " << finalRes.size() << std::endl;

        long queryend = Util::get_cur_time();
        // std::cout << "Join cost " << queryend - joinbegin << " ms." << std::endl;
        // std::cout << "Execute queries and join, cost " << queryend - querybegin << " ms." << std::endl;
        // std::cout << "===================== Query partition method " << k << " =====================" << std::endl << std::endl;

        total_results.push_back(finalRes);
    }

    // // unload
    // pthread_t unload_pthrds[part];
    // for (int i = 0; i < part; i++)
    //     if (pthread_create(&unload_pthrds[i], NULL, unloadThread, &bta[i]) != 0)
    //         throw runtime_error("creates unload thread error!");
    // for (int i = 0; i < part; i++)
    //     pthread_join(unload_pthrds[i], NULL);

    // union
    long unionbegin = Util::get_cur_time();
    std::vector<std::string> unionRes = total_results[0];
    for (int i = 1; i < total_results.size(); i++)
    {
        unionRes = joinobj.Union(unionRes, total_results[i]);
    }
    // std::cout << "Begin to Union." << std::endl;
    long unionend = Util::get_cur_time();
    // std::cout << "Union res of all method, cost " << unionend - unionbegin << " ms." << std::endl;
    for (std::map<std::string, std::vector<std::string> *>::iterator it = query2res.begin(); it != query2res.end(); it++) // 要改
        delete it->second;

    /*resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    std::string finalResult;
    for (int i = 0; i < unionRes.size(); i++)
    {
          cout<<unionRes.at(i)<<endl;

        finalResult += (unionRes.at(i) + "\n");
    }

    return finalResult;*/
    map<string, set<string>> resMap;
    resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    if (resNum == 0)
        return resMap;
    // std::string finalResult;
    string head = unionRes.at(0);

    for (int i = 1; i < unionRes.size(); i++)
    {
        string temp = unionRes.at(i);
        if (head.compare("?s") == 0 || head.compare("?o") == 0)
        {
            // string p = '<' + predicate + '>';
            // resMap[p].insert(temp);
            resMap[predicate].insert(temp);
        }
        else if (head.compare("?s\t?p") == 0)
        {
            vector<string> temp1 = split(temp, "\t");
            resMap[temp1[1]].insert(temp1[0]);
        }
        else if (head.compare("?p\t?o") == 0)
        {
            vector<string> temp2 = split(temp, "\t");
            resMap[temp2[0]].insert(temp2[1]);
        }
    }

    return resMap;
}

// 属性没有尖括号
std::map<string, set<string>> queryNeighbor1(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, std::string &dbname, std::string vertex, std::string predicate, bool in_out, long long &resNum)
{

    int part = confJson["sites"].size();

    // // load
    // buildThreadArgs bta[part];
    // pthread_t load_pthrds[part];
    // for (int i = 0; i < part; i++)
    // {
    //     crow::json::rvalue site = confJson["sites"][i];
    //     bta[i].gcp = servers[i];
    //     bta[i].dbname = dbname;
    //     if (pthread_create(&load_pthrds[i], NULL, loadThread, &bta[i]) != 0)
    //     {
    //         throw runtime_error("creates load thread error!");
    //     }
    // }
    // for (int i = 0; i < part; i++)
    //     pthread_join(load_pthrds[i], NULL);
    // std::cout << "all dbs invoked load." << std::endl;

    // query
    // 查询生成
    std::vector<std::vector<std::string>> total_queries;
    std::vector<std::string> query;
    query.push_back(neighborSql1(vertex, predicate, in_out));
    total_queries.push_back(query);

    // query on each site.
    std::vector<std::vector<std::string>> total_results;
    joiner joinobj;
    std::map<std::string, std::vector<std::string> *> query2res;
    int queryCnt = 0;
    for (int k = 0; k < total_queries.size(); k++)
        queryCnt += total_queries[k].size();
    // std::cout << "The number of queries decomposed is " << queryCnt << std::endl;
    for (int k = 0; k < total_queries.size(); k++)
    {
        // std::cout << "===================== Query partition method " << k << " =====================" << std::endl;
        long querybegin = Util::get_cur_time();
        std::vector<std::string> &queries = total_queries[k];
        // 执行查询
        std::queue<std::vector<std::string> *> results;
        // 并行查询
        pthread_t thread[queries.size()][servers.size()];
        QNV qnv[queries.size()][servers.size()];
        long queryTime[servers.size()];
        // vector<long> queryTime[servers.size()];
        memset(queryTime, 0, sizeof queryTime);
        std::set<int> skipQueryNum;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
                for (int j = 0; j < servers.size(); j++)
                {
                    qnv[i][j].queryId = i;
                    qnv[i][j].serverId = j;
                    qnv[i][j].server = servers[j];
                    qnv[i][j].dbname = dbname;
                    qnv[i][j].format = "json";
                    qnv[i][j].query = queries[i];
                    // std::cout << "query " << i << " on server " << j << " ";
                    if (pthread_create(&thread[i][j], NULL, queryThread1, &qnv[i][j]) != 0)
                    {
                        throw runtime_error("creates thread error!");
                    }
                    // else
                    // std::cout << "creates thread success!" << std::endl;
                }
            else
            {
                skipQueryNum.insert(i);
                // std::cout << "query " << i <<" has already executed." << std::endl;
            }
        }
        for (int i = 0; i < queries.size(); i++)
        {
            if (skipQueryNum.find(i) == skipQueryNum.end())
                for (int j = 0; j < servers.size(); j++)
                    pthread_join(thread[i][j], NULL);
        }
        // 合并并行查询后的结果
        // std::cout << "Now merge the results..." << std::endl;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
            {
                bool firstRes = true;
                std::set<std::string> s; // 去掉重复的结果
                std::string head;
                for (int j = 0; j < servers.size(); j++)
                {
                    std::string temp_res1 = jsonToTxt(qnv[i][j].res);
                    while (temp_res1 == "JsonParseError")
                    {
                        // 解析失败查询重新执行
                        cout << "query replay" << endl;
                        pthread_t threadt;
                        if (pthread_create(&threadt, NULL, queryThread1, &qnv[i][j]) != 0)
                        {
                            throw runtime_error("creates thread error!");
                        }
                        pthread_join(threadt, NULL);

                        // 解析新的查询结果
                        temp_res1 = jsonToTxt(qnv[i][j].res);
                        cout << qnv[i][j].res << endl;
                    }
                    
                    // std::string &temp_res = qnv[i][j].res;
                    std::string &temp_res = temp_res1;
                    // std::cout << temp_res ;
                    if (temp_res.empty())
                    {
                        // std::cout << "query " << i << " server " << j << " No response " << std::endl;
                        continue;
                    }
                    else if (temp_res.find("[empty result]") != std::string::npos) // 查出的结果是空集
                    {
                        // std::cout << "query " << i << " server " << j << " Empty" << std::endl;
                        continue;
                    }
                    else
                    {
                        // std::cout << "query " << i << " server " << j << " Get Answer ";
                    }

                    std::vector<std::string> lines;
                    joiner::split(temp_res, lines, "\n");
                    // std::cout << lines.size() - 1 << std::endl;
                    std::vector<std::string>::iterator iter = lines.begin();
                    head = *iter;
                    // queryTime[j] += stol(*iter);
                    // queryTime[j].push_back(stol(*iter));
                    ++iter;
                    if (firstRes) // 第一次添加结果
                    {
                        firstRes = false;
                    }
                    for (; iter != lines.end(); iter++)
                        s.insert(*iter);
                }
                std::vector<std::string> *resOfQuery = new std::vector<std::string>; // 是否要new
                if (!firstRes)                                                       // 有结果
                {
                    resOfQuery->push_back(head);
                    resOfQuery->insert(resOfQuery->end(), s.begin(), s.end());
                }
                results.push(resOfQuery);
                query2res[queries[i]] = resOfQuery;
            }
            else
            {
                results.push(query2res[queries[i]]);
                // std::cout << "Get result of executed query " << i << "." << std::endl;
            }
        }
        // std::cout << "Before Join results size is " << results.size() << std::endl;

        // join
        long joinbegin = Util::get_cur_time();
        std::vector<std::string> finalRes = joinobj.join(results);
        // std::cout<< "joinobj.join " << finalRes.size() << std::endl;

        long queryend = Util::get_cur_time();
        // std::cout << "Join cost " << queryend - joinbegin << " ms." << std::endl;
        // std::cout << "Execute queries and join, cost " << queryend - querybegin << " ms." << std::endl;
        // std::cout << "===================== Query partition method " << k << " =====================" << std::endl << std::endl;

        total_results.push_back(finalRes);
    }

    // // unload
    // pthread_t unload_pthrds[part];
    // for (int i = 0; i < part; i++)
    //     if (pthread_create(&unload_pthrds[i], NULL, unloadThread, &bta[i]) != 0)
    //         throw runtime_error("creates unload thread error!");
    // for (int i = 0; i < part; i++)
    //     pthread_join(unload_pthrds[i], NULL);

    // union
    long unionbegin = Util::get_cur_time();
    std::vector<std::string> unionRes = total_results[0];
    for (int i = 1; i < total_results.size(); i++)
    {
        unionRes = joinobj.Union(unionRes, total_results[i]);
    }
    // std::cout << "Begin to Union." << std::endl;
    long unionend = Util::get_cur_time();
    // std::cout << "Union res of all method, cost " << unionend - unionbegin << " ms." << std::endl;
    for (std::map<std::string, std::vector<std::string> *>::iterator it = query2res.begin(); it != query2res.end(); it++) // 要改
        delete it->second;

    /*resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    std::string finalResult;
    for (int i = 0; i < unionRes.size(); i++)
    {
          cout<<unionRes.at(i)<<endl;

        finalResult += (unionRes.at(i) + "\n");
    }

    return finalResult;*/
    map<string, set<string>> resMap;
    resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    if (resNum == 0)
        return resMap;
    // std::string finalResult;
    string head = unionRes.at(0);

    for (int i = 1; i < unionRes.size(); i++)
    {
        string temp = unionRes.at(i);
        if (head.compare("?s") == 0 || head.compare("?o") == 0)
        {
            string p = '<' + predicate + '>';
            resMap[p].insert(temp);
        }
        else if (head.compare("?s\t?p") == 0)
        {
            vector<string> temp1 = split(temp, "\t");
            resMap[temp1[1]].insert(temp1[0]);
        }
        else if (head.compare("?p\t?o") == 0)
        {
            vector<string> temp2 = split(temp, "\t");
            resMap[temp2[0]].insert(temp2[1]);
        }
    }

    return resMap;
}

/**
 * @name: queryNeighbor2
 * @description: 获取顶点为内部顶点时所在分片，在分片内查询邻居，fast?
 * @return {map<string, set<string>>}
 * @param {rvalue} &confJson
 * @param {vector<GstoreConnector *>} &servers
 * @param {string} &dbname
 * @param {string} vertex
 * @param {string} predicate
 * @param {bool} in_out
 * @param {long long} &resNum
 * @param {string} dividefile
 */
std::map<string, set<string>> queryNeighbor2(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, std::string &dbname, std::string vertex, std::string predicate, bool in_out, long long &resNum, int subPartID)
{

    // // 读取分片文件
    // std::ifstream ifs_dividefile;
    // ifs_dividefile.open(dividefile);
    // int num = 0; // 分片数
    // std::string entity;
    // int partID;
    // std::unordered_map<std::string, int> entity2id; // 存储顶点分片信息
    // // 获取所有顶点分片信息
    // while (ifs_dividefile >> entity && ifs_dividefile >> partID)
    // {
    //     entity2id.insert(std::pair<std::string, int>(entity, partID));
    //     num = partID > num ? partID : num;
    // }
    // num++;

    // 返回的结果集
    map<string, set<string>> resMap;
    // 弃用，改用对sparql进行url编码
    // if((vertex.find("?username=") != string::npos)||(vertex.find("?password=") != string::npos))
    //      return resMap;

    // 查询顶点对应的分片
    // int subPartID = entity2id[vertex];
    // cout<<subPartID<<endl;
    // 查询参数准备
    pthread_t thread;
    QNV qnv;
    qnv.queryId = 0;
    qnv.serverId = subPartID;
    qnv.server = servers[subPartID];
    qnv.dbname = dbname;
    qnv.format = "json";
    qnv.query = neighborSql(vertex, predicate, in_out);

    // 查询
    if (pthread_create(&thread, NULL, queryThread1, &qnv) != 0)
    {
        throw runtime_error("creates thread error!");
    }
    pthread_join(thread, NULL);
    // GstoreConnector &server = *servers[subPartID];
    // qnv.res = server.query(qnv.dbname, qnv.format, qnv.query);
    // cout<<qnv.res<<endl;
    std::string temp_res = jsonToTxt(qnv.res);
    while (jsonToTxt(qnv.res) == "JsonParseError")
    {
        // 解析失败查询重新执行
        cout << "query replay" << endl;
        pthread_t threadt;
        if (pthread_create(&threadt, NULL, queryThread1, &qnv) != 0)
        {
            throw runtime_error("creates thread error!");
        }
        pthread_join(threadt, NULL);
        // 解析新的查询结果
        temp_res = jsonToTxt(qnv.res);
        cout << qnv.res << endl;
    }
    std::vector<std::string> lines;
    joiner::split(temp_res, lines, "\n");

    
    // 处理空查询结果[empty result]
    resNum = lines.size() > 1 ? lines.size() - 1 : 0;
    if (resNum == 0)
        return resMap;
    string head = lines.at(0);

    for (int i = 1; i < lines.size(); i++)
    {
        string temp = lines.at(i);
        if (head.compare("?s") == 0 || head.compare("?o") == 0)
        {
            string p = '<' + predicate + '>';
            resMap[p].insert(temp);
        }
        else if (head.compare("?s\t?p") == 0)
        {
            vector<string> temp1 = split(temp, "\t");
            resMap[temp1[1]].insert(temp1[0]);
        }
        else if (head.compare("?p\t?o") == 0)
        {
            vector<string> temp2 = split(temp, "\t");
            resMap[temp2[0]].insert(temp2[1]);
        }
    }

    return resMap;
}

// 使用字符串进行分割
void Stringsplit1(string str, const char split, vector<string> &res)
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

std::set<string> queryVertex(crow::json::rvalue &confJson, std::vector<GstoreConnector *> &servers, std::string &dbname, long long &resNum)
{
    string sql = "select ?s ?o where { ?s   ?p  ?o. }";
    int part = confJson["sites"].size();

    // // load
    buildThreadArgs bta[part];
    pthread_t load_pthrds[part];
    for (int i = 0; i < part; i++)
    {
        crow::json::rvalue site = confJson["sites"][i];
        bta[i].gcp = servers[i];
        bta[i].dbname = dbname;
        if (pthread_create(&load_pthrds[i], NULL, loadThread, &bta[i]) != 0)
        {
            throw runtime_error("creates load thread error!");
        }
    }
    for (int i = 0; i < part; i++)
        pthread_join(load_pthrds[i], NULL);
    std::cout << "all dbs invoked load." << std::endl;

    // query
    // 查询生成
    std::vector<std::vector<std::string>> total_queries;
    std::vector<std::string> query;
    query.push_back(sql);
    total_queries.push_back(query);

    // query on each site.
    std::vector<std::vector<std::string>> total_results;
    joiner joinobj;
    std::map<std::string, std::vector<std::string> *> query2res;
    int queryCnt = 0;
    for (int k = 0; k < total_queries.size(); k++)
        queryCnt += total_queries[k].size();
    // std::cout << "The number of queries decomposed is " << queryCnt << std::endl;
    for (int k = 0; k < total_queries.size(); k++)
    {
        // std::cout << "===================== Query partition method " << k << " =====================" << std::endl;
        long querybegin = Util::get_cur_time();
        std::vector<std::string> &queries = total_queries[k];
        // 执行查询
        std::queue<std::vector<std::string> *> results;
        // 并行查询
        pthread_t thread[queries.size()][servers.size()];
        QNV qnv[queries.size()][servers.size()];
        long queryTime[servers.size()];
        // vector<long> queryTime[servers.size()];
        memset(queryTime, 0, sizeof queryTime);
        std::set<int> skipQueryNum;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
                for (int j = 0; j < servers.size(); j++)
                {
                    qnv[i][j].queryId = i;
                    qnv[i][j].serverId = j;
                    qnv[i][j].server = servers[j];
                    qnv[i][j].dbname = dbname;
                    qnv[i][j].format = "json";
                    qnv[i][j].query = queries[i];
                    // std::cout << "query " << i << " on server " << j << " ";
                    if (pthread_create(&thread[i][j], NULL, queryThread, &qnv[i][j]) != 0)
                    {
                        throw runtime_error("creates thread error!");
                    }
                    // else
                    // std::cout << "creates thread success!" << std::endl;
                }
            else
            {
                skipQueryNum.insert(i);
                // std::cout << "query " << i <<" has already executed." << std::endl;
            }
        }
        for (int i = 0; i < queries.size(); i++)
        {
            if (skipQueryNum.find(i) == skipQueryNum.end())
                for (int j = 0; j < servers.size(); j++)
                    pthread_join(thread[i][j], NULL);
        }
        // 合并并行查询后的结果
        // std::cout << "Now merge the results..." << std::endl;
        for (int i = 0; i < queries.size(); i++)
        {
            if (query2res.find(queries[i]) == query2res.end())
            {
                bool firstRes = true;
                std::set<std::string> s; // 去掉重复的结果
                std::string head;
                for (int j = 0; j < servers.size(); j++)
                {
                    std::string temp_res1 = jsonToTxt(qnv[i][j].res);
                    // std::string &temp_res = qnv[i][j].res;
                    std::string &temp_res = temp_res1;
                    // std::cout << temp_res ;
                    if (temp_res.empty())
                    {
                        // std::cout << "query " << i << " server " << j << " No response " << std::endl;
                        continue;
                    }
                    else if (temp_res.find("[empty result]") != std::string::npos) // 查出的结果是空集
                    {
                        // std::cout << "query " << i << " server " << j << " Empty" << std::endl;
                        continue;
                    }
                    else
                    {
                        // std::cout << "query " << i << " server " << j << " Get Answer ";
                    }

                    std::vector<std::string> lines;
                    joiner::split(temp_res, lines, "\n");
                    // std::cout << lines.size() - 1 << std::endl;
                    std::vector<std::string>::iterator iter = lines.begin();
                    head = *iter;
                    // queryTime[j] += stol(*iter);
                    // queryTime[j].push_back(stol(*iter));
                    ++iter;
                    if (firstRes) // 第一次添加结果
                    {
                        firstRes = false;
                    }
                    for (; iter != lines.end(); iter++)
                        s.insert(*iter);
                }
                std::vector<std::string> *resOfQuery = new std::vector<std::string>; // 是否要new
                if (!firstRes)                                                       // 有结果
                {
                    resOfQuery->push_back(head);
                    resOfQuery->insert(resOfQuery->end(), s.begin(), s.end());
                }
                results.push(resOfQuery);
                query2res[queries[i]] = resOfQuery;
            }
            else
            {
                results.push(query2res[queries[i]]);
                // std::cout << "Get result of executed query " << i << "." << std::endl;
            }
        }
        // std::cout << "Before Join results size is " << results.size() << std::endl;

        // join
        long joinbegin = Util::get_cur_time();
        std::vector<std::string> finalRes = joinobj.join(results);
        // std::cout<< "joinobj.join " << finalRes.size() << std::endl;

        long queryend = Util::get_cur_time();
        // std::cout << "Join cost " << queryend - joinbegin << " ms." << std::endl;
        // std::cout << "Execute queries and join, cost " << queryend - querybegin << " ms." << std::endl;
        // std::cout << "===================== Query partition method " << k << " =====================" << std::endl << std::endl;

        total_results.push_back(finalRes);
    }

    // // unload
    // pthread_t unload_pthrds[part];
    // for (int i = 0; i < part; i++)
    //     if (pthread_create(&unload_pthrds[i], NULL, unloadThread, &bta[i]) != 0)
    //         throw runtime_error("creates unload thread error!");
    // for (int i = 0; i < part; i++)
    //     pthread_join(unload_pthrds[i], NULL);

    // union
    long unionbegin = Util::get_cur_time();
    std::vector<std::string> unionRes = total_results[0];
    for (int i = 1; i < total_results.size(); i++)
    {
        unionRes = joinobj.Union(unionRes, total_results[i]);
    }
    // std::cout << "Begin to Union." << std::endl;
    long unionend = Util::get_cur_time();
    // std::cout << "Union res of all method, cost " << unionend - unionbegin << " ms." << std::endl;
    for (std::map<std::string, std::vector<std::string> *>::iterator it = query2res.begin(); it != query2res.end(); it++) // 要改
        delete it->second;

    /*resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    std::string finalResult;
    for (int i = 0; i < unionRes.size(); i++)
    {
          cout<<unionRes.at(i)<<endl;

        finalResult += (unionRes.at(i) + "\n");
    }

    return finalResult;*/
    map<string, set<string>> resMap;
    set<string> entity;
    resNum = unionRes.size() > 1 ? unionRes.size() - 1 : 0;
    if (resNum == 0)
        return entity;
    // std::string finalResult;
    string head = unionRes.at(0);

    for (int i = 1; i < unionRes.size(); i++)
    {
        string temp = unionRes.at(i);
        std::cout << temp << endl;
        if (temp.size() > 1 && temp[0] != '#')
        {

            vector<string> strList;
            Stringsplit1(temp, '\t', strList);
            if (strList[0][0] != '\"')
                entity.insert(strList[0]);
            if (strList[1][0] != '\"')
                entity.insert(strList[1]);
        }
    }
    resNum = entity.size();
    return entity;
}

#endif
