#ifndef REMOTECMDINVOKER
#define REMOTECMDINVOKER

#include <cstdlib>
#include <string>
#include <iostream>

#include "RemoteInfo.h"
#include "../gStoreAPI/client.h"

#include "../queryDecompose/Util/Util.h"

using namespace std;

class RemoteCmdInvoker : public RemoteInfo
{
private:

public:
    RemoteCmdInvoker(std::string _ip, std::string _user);
    ~RemoteCmdInvoker();

    int execute(std::string _cmd);
};

RemoteCmdInvoker::RemoteCmdInvoker(std::string _ip, std::string _user)
 : RemoteInfo(_ip, _user)
{
}

RemoteCmdInvoker::~RemoteCmdInvoker()
{
}

int RemoteCmdInvoker::execute(std::string _cmd)
{
    std::string cmd = "ssh " + user+"@"+ip+" \"" + _cmd + "\"";
    std::cout << cmd << std::endl;
    
    int ret = system(cmd.c_str());
    return ret;
}

struct buildThreadArgs
{
    GstoreConnector *gcp;
    std::string dbname;
    std::string ntpath;
};


void *buildThread(void *args)
{
    buildThreadArgs *p = (buildThreadArgs*) args;
    std::cout << "ip: " 
              << p->gcp->serverIP << " 255.255.255.0"
              << " port: " 
              << p->gcp->serverPort 
              << " dbuser: " 
              << p->gcp->username 
              << " dbpasswd: " 
              << p->gcp->password 
              << " dbname: "
              << p->dbname
              << " ntpath: "
              << p->ntpath
              << std::endl;
    std::cout << p->gcp->build(p->dbname, p->ntpath) << std::endl;
    return nullptr;
}

void *loadThread(void *args)
{
    buildThreadArgs *p = (buildThreadArgs*) args;
    std::cout << p->gcp->load(p->dbname) << std::endl;
    return nullptr;
}

void *unloadThread(void *args)
{
    buildThreadArgs *p = (buildThreadArgs*) args;
    std::cout << p->gcp->unload(p->dbname) << std::endl;
    return nullptr;
}

void *deleteThread(void *args)
{
    buildThreadArgs *p = (buildThreadArgs*) args;
    std::cout << p->gcp->drop(p->dbname, false) << std::endl;
    return nullptr;
}

typedef struct QueryNeedVars
{
    int queryId, serverId;
    GstoreConnector *server;
    string dbname;
    string format;
    string query;
    string res;
    long timeCost;
} QNV;

static const std::string UrlEncode(const std::string& s)
{
	std::string ret;
	unsigned char* ptr = (unsigned char*)s.c_str();
	ret.reserve(s.length());

	for (int i = 0; i < s.length(); ++i)
	{
		if ((int(ptr[i]) == 42) || (int(ptr[i]) == 45) || (int(ptr[i]) == 46) || (int(ptr[i]) == 47) || (int(ptr[i]) == 58) || (int(ptr[i]) == 95))
			ret += ptr[i];
		else if ((int(ptr[i]) >= 48) && (int(ptr[i]) <= 57))
			ret += ptr[i];
		else if ((int(ptr[i]) >= 65) && (int(ptr[i]) <= 90))
			ret += ptr[i];
		else if ((int(ptr[i]) >= 97) && (int(ptr[i]) <= 122))
			ret += ptr[i];
		else if (int(ptr[i]) == 32)
			ret += '+';
		else if ((int(ptr[i]) != 9) && (int(ptr[i]) != 10) && (int(ptr[i]) != 13))
		{
			char buf[5];
			memset(buf, 0, 5);
			snprintf(buf, 5, "%%%X", ptr[i]);
			ret.append(buf);
		}
	}
	return ret;
}

void *queryThread(void *args)
{
    long begin = Util::get_cur_time(), timeCost;
    QNV *vars = (QNV *)args;
    GstoreConnector &server = *vars->server;
    // vars->res = server.query(vars->dbname, vars->format, vars->query);
    // get方式执行查询对sparql进行url编码,防止特殊字符，如？，@,&等字符影响sparql读取(类似sql注入)
    vars->res = server.query(vars->dbname, vars->format, UrlEncode(vars->query));
    timeCost = Util::get_cur_time() - begin;
    cout << "query " << vars->queryId << " has finished on server " << vars->serverId << ". Take " << timeCost << "ms." << endl;
    cout << vars->query << endl;
    // cout << vars->res << endl;
    vars -> timeCost = timeCost;
    // cout << vars -> timeCost << endl;
    return NULL;
}


// 没有提示
void *queryThread1(void *args)
{
    long begin = Util::get_cur_time(), timeCost;
    QNV *vars = (QNV *)args;
    GstoreConnector &server = *vars->server;
    // vars->res = server.query(vars->dbname, vars->format, vars->query);
    // get方式执行查询对sparql进行url编码,防止特殊字符，如？，@,&等字符影响sparql读取(类似sql注入)
    vars->res = server.query(vars->dbname, vars->format, UrlEncode(vars->query));
    timeCost = Util::get_cur_time() - begin;
    // cout << "query " << vars->queryId << " has finished on server " << vars->serverId << ". Take " << timeCost << "ms." << endl;
    // cout << vars->query << endl;
    // cout << vars->res << endl;
    vars -> timeCost = timeCost;
    // cout << vars -> timeCost << endl;
    return NULL;
}

#endif
