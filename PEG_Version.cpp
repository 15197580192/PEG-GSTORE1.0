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

    if(argc > 1)
    {
        cout << "============================================================" << endl;
        cout << "Get the version of PEG. The way to use this program: " << endl;
        cout << "./PEG_Version" << endl;
        cout << "============================================================" << endl;

        return 0;
    }

    std::cout<<"PEG version: V1.0"<<std::endl;
    std::cout<<"Copyright (c) 2022, 2024, hnu and/or its affiliates."<<std::endl;
    
    return 0;
}
