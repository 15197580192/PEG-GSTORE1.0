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
        cout << "Load Database from nt file. The way to use this program: " << endl;
        cout << "./build/PEG_Load database_name /nt/file/path /partition/file/path" << endl;
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
    string dbpath(argv[2]);
    string divpath(argv[3]);

    int status = build(confJson, servers, dbname, dbpath, divpath);

    if(status == 0)
        cout << "Build successfully. " << endl;
    
    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    
    return 0;
}
