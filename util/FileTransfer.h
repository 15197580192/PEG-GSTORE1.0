#ifndef FILETRANSFER
#define FILETRANSFER

#include <iostream>
#include <string>
#include <cstdlib>

#include "RemoteInfo.h"

/**
 * 先配置好免密登入
 */
class FileTransfer: public RemoteInfo
{
private:

public:
    FileTransfer(std::string _ip, std::string _user);
    ~FileTransfer();

    int transfer(std::string localFilePath, std::string remoteFilePath);
};

FileTransfer::FileTransfer(std::string _ip, std::string _user)
 : RemoteInfo(_ip, _user)
{
}

FileTransfer::~FileTransfer()
{
}

int FileTransfer::transfer(std::string localFilePath, std::string remoteFilePath)
{
    std::string cmd = "scp " + localFilePath + " " + user+"@"+ip+":"+remoteFilePath;
    // scp 传输成功返回0，不成功返回大于0的数
    std::cout << cmd << std::endl;
    
    int ret = system(cmd.c_str());
    return ret;
}

struct TransferThreadArgs
{
    FileTransfer *ft;
    std::string localFilePath;
    std::string remoteFilePath;
};

void *transferThread(void *args)
{
    TransferThreadArgs *p = (TransferThreadArgs*)args;
    int ret = p->ft->transfer(p->localFilePath, p->remoteFilePath);
    std::cout << "Transfer " << p->localFilePath << " to remote " << p->remoteFilePath;
    if(ret == 0)
        std::cout << " successfully." << std::endl;
    else
        std::cout << " unsuccessfully." << std::endl;
    return nullptr;
}

#endif