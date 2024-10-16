/*=============================================================================
# Filename: GeneralEvaluation.cpp
# Author: Jiaqi, Chen
# Mail: chenjiaqi93@163.com
# Last Modified: 2016-09-12
# Description: implement functions in GeneralEvaluation.h
=============================================================================*/

#include "GeneralEvaluation.h"



bool GeneralEvaluation::parseGetQuery(const string &_query, int& var_num, QueryTree::QueryForm& query_form, int& star_tag)
{
    try
    {
        this->query_parser.SPARQLParse(_query, this->query_tree);
    }
    catch(const char* e)
    {
        cerr << e << endl;
        return false;
    }

    // this->query_tree.getGroupPattern().getVarset();

    int patten_num = this->query_tree.getGroupPattern().patterns.size();

    var_num = this->query_tree.getGroupPattern().grouppattern_subject_object_maximal_varset.varset.size();
    if(this->query_tree.getQueryForm() == QueryTree::Ask_Query){
        query_form = QueryTree::Ask_Query;
    }else{
        query_form = QueryTree::Select_Query;
    }

    std::vector<std::vector<std::vector<QueryTree::Vertex>>> queryVertex=this->query_tree.getStarQuery();

    for(int i=0;i<queryVertex.size();i++){
        if(patten_num!=1)
            queriesDecomposed.push_back(query_tree.getSparql(queryVertex[i]));
        else 
            queriesDecomposed.push_back({_query});
    }
    
    return true;
}

