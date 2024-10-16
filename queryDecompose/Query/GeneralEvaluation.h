/*=============================================================================
# Filename: GeneralEvaluation.h
# Author: Jiaqi, Chen
# Mail: chenjiaqi93@163.com
# Last Modified: 2016-09-12
# Description: 
=============================================================================*/

#ifndef _QUERY_GENERALEVALUATION_H
#define _QUERY_GENERALEVALUATION_H


#include "../Util/Util.h"
#include "../Parser/QueryParser.h"
#include "QueryTree.h"

using namespace std;

class GeneralEvaluation
{
	private:
		QueryParser query_parser;
		QueryTree query_tree;

	public:
		vector<vector<string>> queriesDecomposed;
        bool parseGetQuery(const string &_query, int& var_num, QueryTree::QueryForm& query_form, int& star_tag);
		
};

#endif // _QUERY_GENERALEVALUATION_H

