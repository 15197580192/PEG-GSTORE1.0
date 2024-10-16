#ifndef REMOTEINFO
#define REMOTEINFO

#include <string>

class RemoteInfo
{
protected:
    std::string ip;
    std::string user;

public:
    RemoteInfo(std::string _ip, std::string _user);
    ~RemoteInfo();
};

RemoteInfo::RemoteInfo(std::string _ip, std::string _user): ip(_ip), user(_user)
{
}

RemoteInfo::~RemoteInfo()
{
}

#endif