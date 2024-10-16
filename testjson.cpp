/*
 * @Filename: testjson.cpp
 * @Author: Hu Zheyuan
 * @Date: 2022-11-01 21:56:57
 * @LastEditTime: 2022-12-05 15:38:18
 * @LastEditors: Hu Zheyuan 1980452465@qq.com
 * @Description: a test of transforming gstore query results from json format to txt format
 */
#include <string>
#include <fstream>
#include <iostream>
#include <vector> 
#include "rapidjson/document.h"
using namespace std;
using namespace rapidjson;

string JsonToTxt(const string &jvStr);

int main () {
	ifstream t("./json.txt");
	string str((std::istreambuf_iterator<char>(t)),std::istreambuf_iterator<char>());
	string res=JsonToTxt(str);	
	cout<<res;
	return 0;
}

string JsonToTxt(const string &jvStr) {
	string res="";
	Document document;
	vector<string> vars;
	if (document.Parse(jvStr.c_str()).HasParseError()) {
		cout<<"JsonParseError"<<endl;
		return "[empty result]";
	}
  	if (!document.HasMember("head") || !document.HasMember("results")) 
	{
		return "[empty result]";
	}
	
	Value &jvObject = document["head"];
	Value &jvObject1 = document["results"];
	if(!jvObject.IsObject() || !jvObject.HasMember("vars")||!jvObject1.IsObject() || !jvObject1.HasMember("bindings"))
	{
		return "[empty result]";
	}
	
	Value &jv = jvObject["vars"];
	for(SizeType i = 0; i < jv.Size(); i++) {
		res+="?";
		res+=jv[i].GetString();
		vars.push_back(jv[i].GetString());
		res+="\t";
	}
	// ĩβ\t�滻Ϊ\n 
	res.pop_back();
	res+="\n";
	
	
	Value &jv1 = jvObject1["bindings"];
	if(jv1.Size()==0) {
		return "[empty result]";
	}
	
	for(SizeType i = 0; i < jv1.Size(); i++) {
		Value &tmp = jv1[i];
		for(int t=0;t<vars.size();t++) {
			Value &tmp1 = tmp[vars[t].c_str()]; 
			string type = tmp1["type"].GetString();
			string value = tmp1["value"].GetString();
			if(type == "uri") {
				value = "<" + value + ">";
			} else if(type == "literal") {
				value = "\""+ value + "\"";
			} else if(type == "typed-literal") {
				// eg:��3��^^<http://www.w3.org/2001/XMLSchema#integer>
				// json����type,value,datatype
				// datatype����?
				value = "\""+ value + "\"";
			}
			res+=value;
			res+="\t";
		}
		// ĩβ\t�滻Ϊ\n 
		res.pop_back();
		res+="\n"; 
	}
	// ȥ�������\n 
	res.pop_back();
	
	return res;
}

