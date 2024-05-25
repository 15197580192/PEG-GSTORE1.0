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
    if(argc < 2)
    {
        cout << "============================================================" << endl;
        cout << "Delete a Database. The way to use this program: " << endl;
        cout << "./PEG_Delete database_name" << endl;
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

    int status = deleteDB(confJson, servers ,dbname);

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];
    return 0;
}
