#include <iostream>
#include <fstream>
#include <set>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <stack>

#include <pthread.h>

#include "util/FileTransfer.h"
#include "util/RemoteCmdInvoker.h"

#include "include/crow.h"

#include "gStoreAPI/client.h"
#include "queryDecompose/Query/QueryTree.h"
#include "queryDecompose/Query/GeneralEvaluation.h"
#include "joiner/joiner.h"
#include "util/util.h"
#include "handler/Handler.h"

using namespace std;

string conf;
crow::json::rvalue confJson;
vector<GstoreConnector*> servers;

int main()
{
    crow::SimpleApp app;
    app.loglevel(crow::LogLevel::DEBUG);

    conf = readFile("conf/servers.json");
    confJson = crow::json::load(conf);
    
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
    
    CROW_ROUTE(app, "/index")
    ([]{
        crow::mustache::context ctx;
        return crow::mustache::load("index.html").render();
    });

    CROW_ROUTE(app, "/api/user/login").methods(crow::HTTPMethod::POST)([](const crow::request &req){
        crow::json::wvalue retv;
        retv["status"] = "success";
        retv["message"] = "success";
        retv["data"] = NULL;
        crow::response res;
        res.set_header("Access-Control-Allow-Origin", res.get_header_value("Origin"));
        res.set_header("Access-Control-Allow-Credentials", "true");
        res.set_header("P3P", "CP=CAO PSA OUR");

        res.add_header("Access-Control-Allow-Methods", "POST,GET,TRACE,OPTIONS");
        res.add_header("Access-Control-Allow-Headers", "Content-Type,Origin,Accept");
        res.add_header("Access-Control-Max-Age", "120");
        
        res.write(retv.dump());
        return res;
    });

    
    CROW_ROUTE(app, "/api/version")([](const crow::request& req){
        
        crow::json::wvalue retv;
        retv["status"] = "success";
        retv["message"] = "success";
        retv["data"] = "PEG V1.0";
        return retv;
    });

    CROW_ROUTE(app, "/api/build")([](const crow::request& req){
        string dbname = req.url_params.get("dbname");
        string dbpath = req.url_params.get("dbpath");
        string divfilepath = req.url_params.get("divfilepath");

        int status = build(confJson, servers, dbname, dbpath, divfilepath);
        
        crow::json::wvalue retv;
        retv["status"] = "success";
        retv["message"] = "success";
        return retv;
    });

    CROW_ROUTE(app, "/api/query")([](const crow::request& req){
        string dbname = req.url_params.get("dbname");
        string sparql = req.url_params.get("sparql");

        CROW_LOG_DEBUG << "sparql :" << sparql;
        CROW_LOG_DEBUG << "dbname :" << dbname;

        long long resNum;
        string finalRes = query(confJson, servers, dbname, sparql, resNum);
        
        crow::json::wvalue retv;
        retv["status"] = "success";
        retv["message"] = "success";
        retv["data"] = std::move(finalRes);
        return retv;
    });

    CROW_ROUTE(app, "/api/nodeConfig")([](){
        return crow::json::wvalue(confJson);
    });

    CROW_ROUTE(app, "/api/getDBList")([](){
        string dbListStr = readFile("conf/dblist.json");
        return crow::json::wvalue(crow::json::load(dbListStr));
    });

    CROW_ROUTE(app, "/api/deleteDB")([](const crow::request& req){
        string dbname = req.url_params.get("dbname");

        int status = deleteDB(confJson, servers, dbname);
        
        crow::json::wvalue retv;
        retv["status"] = "success";
        retv["message"] = "success";
        return retv;
    });


    app.port(18081).multithreaded().run();

    CROW_LOG_INFO << "Deleting GstoreConnector...";
    for (int i = 0; i < confJson["sites"].size(); i++)
    {
        delete servers[i];
    }
    CROW_LOG_INFO << "Deleted GstoreConnector!";
    return 0;
}
