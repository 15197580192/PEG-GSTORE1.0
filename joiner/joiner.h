#ifndef JOINER_H
#define JOINER_H

#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <queue>
#include <sys/time.h>

using namespace std;

class joiner
{
private:
    map<string, vector<string>> hashTable;
    map<string, int> varMap;

    long long finalResNum = 0;
    long get_cur_time();
    vector<string> intersection(const vector<string> &s1, const vector<string> &s2);
    vector<string> unionSet(const vector<string> &s1, const vector<string> &s2);

public:
    void static split(const string &s, vector<string> &tokens, string delimiters = " ");
    
    void addItem(string item, vector<string> &elements);
    bool isContained(string item);
    long long getFinalResNum();

    vector<string> join(queue<vector<string>*> &results);
    vector<string> join(vector<string> &res1, vector<string> &res2);
    string join(string &res1, string &res2);

    string Union(string &res1, string &res2);
    vector<string> Union(vector<string> &res1, vector<string> &res2);
};

#endif