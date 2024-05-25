/*=============================================================================
# Filename: QueryTree.cpp
# Author: Jiaqi, Chen
# Mail: chenjiaqi93@163.com
# Last Modified: 2016-07-14
# Description: implement functions in QueryTree.h
=============================================================================*/

#include "QueryTree.h"

using namespace std;

void QueryTree::GroupPattern::FilterTreeNode::getVarset(Varset &varset)
{
	for (int i = 0; i < (int)this->child.size(); i++)
	{
		if (this->child[i].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::String_type && this->child[i].arg[0] == '?')
			varset.addVar(this->child[i].arg);
		if (this->child[i].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::Tree_type)
			this->child[i].node.getVarset(varset);
	}
}

void QueryTree::GroupPattern::FilterTreeNode::print(vector<GroupPattern> &exist_grouppatterns, int dep)
{
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Not_type)
		printf("!");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_regex_type)
		printf("regex");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_lang_type)
		printf("lang");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_langmatches_type)
		printf("langmatches");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_bound_type)
		printf("bound");

	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_in_type)
	{
		if (this->child[0].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::String_type)
			printf("%s", this->child[0].arg.c_str());
		printf(" in (");
		for (int i = 1; i < (int)this->child.size(); i++)
		{
			if (i != 1)
				printf(" , ");
			if (this->child[i].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::String_type)
				printf("%s", this->child[i].arg.c_str());
		}
		printf(")");

		return;
	}

	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_exists_type)
	{
		printf("exists");
		exist_grouppatterns[this->exists_grouppattern_id].print(dep);

		return;
	}

	printf("(");

	if ((int)this->child.size() >= 1)
	{
		if (this->child[0].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::String_type)
			printf("%s", this->child[0].arg.c_str());
		if (this->child[0].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::Tree_type)
			this->child[0].node.print(exist_grouppatterns, dep);
	}

	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Or_type)
		printf(" || ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::And_type)
		printf(" && ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Equal_type)
		printf(" = ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::NotEqual_type)
		printf(" != ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Less_type)
		printf(" < ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::LessOrEqual_type)
		printf(" <= ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Greater_type)
		printf(" > ");
	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::GreaterOrEqual_type)
		printf(" >= ");

	if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_regex_type || this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_langmatches_type)
		printf(", ");

	if ((int)this->child.size() >= 2)
	{
		if (this->child[1].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::String_type)
			printf("%s", this->child[1].arg.c_str());
		if (this->child[1].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::Tree_type)
			this->child[1].node.print(exist_grouppatterns, dep);
	}

	if ((int)this->child.size() >= 3)
	{
		if (this->oper_type == QueryTree::GroupPattern::FilterTreeNode::Builtin_regex_type && this->child[2].node_type == QueryTree::GroupPattern::FilterTreeNode::FilterTreeChild::String_type)
			printf(", %s", this->child[2].arg.c_str());
	}

	printf(")");
}

//----------------------------------------------------------------------------------------------------------------------------------------------------

void QueryTree::GroupPattern::addOnePattern(Pattern _pattern)
{
	this->patterns.push_back(_pattern);
}

void QueryTree::GroupPattern::addOneGroupUnion()
{
	this->unions.push_back(GroupPatternUnions((int)this->patterns.size() - 1));
}

void QueryTree::GroupPattern::addOneUnion()
{
	int n = (int)this->unions.size();
	this->unions[n - 1].grouppattern_vec.push_back(GroupPattern());
}

QueryTree::GroupPattern &QueryTree::GroupPattern::getLastUnion()
{
	int n = (int)this->unions.size();
	int m = (int)this->unions[n - 1].grouppattern_vec.size();
	return this->unions[n - 1].grouppattern_vec[m - 1];
}

void QueryTree::GroupPattern::addOneOptionalOrMinus(char _type)
{
	this->optionals.push_back(OptionalOrMinusGroupPattern((int)this->patterns.size() - 1, (int)this->unions.size() - 1, _type));
}

QueryTree::GroupPattern &QueryTree::GroupPattern::getLastOptionalOrMinus()
{
	int n = (int)this->optionals.size();
	return this->optionals[n - 1].grouppattern;
}

void QueryTree::GroupPattern::addOneFilterTree()
{
	this->filters.push_back(FilterTreeRoot());
	this->filter_exists_grouppatterns.push_back(vector<GroupPattern>());
}

QueryTree::GroupPattern::FilterTreeNode &QueryTree::GroupPattern::getLastFilterTree()
{
	return this->filters[(int)(this->filters.size()) - 1].root;
}

void QueryTree::GroupPattern::addOneExistsGroupPattern()
{
	int n = (int)this->filter_exists_grouppatterns.size();
	this->filter_exists_grouppatterns[n - 1].push_back(GroupPattern());
}

QueryTree::GroupPattern &QueryTree::GroupPattern::getLastExistsGroupPattern()
{
	int n = (int)this->filter_exists_grouppatterns.size();
	int m = (int)this->filter_exists_grouppatterns[n - 1].size();
	return this->filter_exists_grouppatterns[n - 1][m - 1];
}

void QueryTree::GroupPattern::getVarset()
{
	for (int i = 0; i < (int)this->patterns.size(); i++)
	{
		if (this->patterns[i].subject.value[0] == '?')
		{
			this->patterns[i].varset.addVar(this->patterns[i].subject.value);
			this->grouppattern_subject_object_maximal_varset.addVar(this->patterns[i].subject.value);
		}
		if (this->patterns[i].predicate.value[0] == '?')
		{
			this->patterns[i].varset.addVar(this->patterns[i].predicate.value);
			this->grouppattern_predicate_maximal_varset.addVar(this->patterns[i].predicate.value);
		}
		if (this->patterns[i].object.value[0] == '?')
		{
			this->patterns[i].varset.addVar(this->patterns[i].object.value);
			this->grouppattern_subject_object_maximal_varset.addVar(this->patterns[i].object.value);
		}
		this->grouppattern_resultset_minimal_varset = this->grouppattern_resultset_minimal_varset + this->patterns[i].varset;
		this->grouppattern_resultset_maximal_varset = this->grouppattern_resultset_maximal_varset + this->patterns[i].varset;
	}

	for (int i = 0; i < (int)this->unions.size(); i++)
	{
		Varset minimal_varset;
		for (int j = 0; j < (int)this->unions[i].grouppattern_vec.size(); j++)
		{
			this->unions[i].grouppattern_vec[j].getVarset();
			if (j == 0)
				minimal_varset = minimal_varset + this->unions[i].grouppattern_vec[j].grouppattern_resultset_minimal_varset;
			else
				minimal_varset = minimal_varset * this->unions[i].grouppattern_vec[j].grouppattern_resultset_minimal_varset;
			this->grouppattern_resultset_maximal_varset = this->grouppattern_resultset_maximal_varset + this->unions[i].grouppattern_vec[j].grouppattern_resultset_maximal_varset;
			this->grouppattern_subject_object_maximal_varset = this->grouppattern_subject_object_maximal_varset + this->unions[i].grouppattern_vec[j].grouppattern_subject_object_maximal_varset;
			this->grouppattern_predicate_maximal_varset = this->grouppattern_predicate_maximal_varset + this->unions[i].grouppattern_vec[j].grouppattern_predicate_maximal_varset;
		}
		this->grouppattern_resultset_minimal_varset = this->grouppattern_resultset_minimal_varset + minimal_varset;
	}

	for (int i = 0; i < (int)this->optionals.size(); i++)
	{
		this->optionals[i].grouppattern.getVarset();
		if (this->optionals[i].type == 'o')
		{
			this->grouppattern_resultset_maximal_varset = this->grouppattern_resultset_maximal_varset + this->optionals[i].grouppattern.grouppattern_resultset_maximal_varset;
			this->grouppattern_subject_object_maximal_varset = this->grouppattern_subject_object_maximal_varset + this->optionals[i].grouppattern.grouppattern_subject_object_maximal_varset;
			this->grouppattern_predicate_maximal_varset = this->grouppattern_predicate_maximal_varset + this->optionals[i].grouppattern.grouppattern_predicate_maximal_varset;
		}
	}

	for (int i = 0; i < (int)this->filters.size(); i++)
	{
		this->filters[i].root.getVarset(this->filters[i].varset);
	}

	for (int i = 0; i < (int)this->filter_exists_grouppatterns.size(); i++)
		for (int j = 0; j < (int)this->filter_exists_grouppatterns[i].size(); j++)
		{
			this->filter_exists_grouppatterns[i][j].getVarset();
		}
}

bool QueryTree::GroupPattern::checkOnlyUnionOptionalFilterNoExists()
{
	for (int i = 0; i < (int)this->unions.size(); i++)
	{
		for (int j = 0; j < (int)this->unions[i].grouppattern_vec.size(); j++)
			if (!this->unions[i].grouppattern_vec[j].checkOnlyUnionOptionalFilterNoExists())
				return false;
	}

	for (int i = 0; i < (int)this->optionals.size(); i++)
	{
		if (this->optionals[i].type != 'o')
			return false;
		if (!this->optionals[i].grouppattern.checkOnlyUnionOptionalFilterNoExists())
			return false;
	}

	for (int i = 0; i < (int)this->filter_exists_grouppatterns.size(); i++)
		if ((int)this->filter_exists_grouppatterns[i].size() != 0)
			return false;

	return true;
}

pair<Varset, Varset> QueryTree::GroupPattern::checkOptionalGroupPatternVarsAndSafeFilter(Varset occur, Varset ban, bool &check_condition)
// return occur varset and ban varset
{
	if (!check_condition)
		return make_pair(Varset(), Varset());

	Varset this_ban;

	int lastpattern = -1, lastunions = -1, lastoptional = -1;
	while (check_condition && (lastpattern + 1 < (int)this->patterns.size() || lastunions + 1 < (int)this->unions.size() || lastoptional + 1 < (int)this->optionals.size()))
	{
		if (lastoptional + 1 < (int)this->optionals.size() && this->optionals[lastoptional + 1].lastpattern == lastpattern && this->optionals[lastoptional + 1].lastunions == lastunions)
		// optional
		{
			pair<Varset, Varset> sub_grouppattern_return_varset = this->optionals[lastoptional + 1].grouppattern.checkOptionalGroupPatternVarsAndSafeFilter(Varset(), ban, check_condition);

			if (occur.hasCommonVar(sub_grouppattern_return_varset.second))
				check_condition = false;

			Varset out = this->optionals[lastoptional + 1].grouppattern.grouppattern_resultset_maximal_varset - occur;
			occur = occur + sub_grouppattern_return_varset.first;
			this_ban = this_ban + sub_grouppattern_return_varset.second;
			this_ban = this_ban + out;
			ban = ban + this_ban;

			lastoptional++;
		}
		else if (lastunions + 1 < (int)this->unions.size() && this->unions[lastunions + 1].lastpattern == lastpattern)
		// union
		{
			Varset sub_grouppattern_occur, sub_grouppattern_ban;

			for (int i = 0; i < (int)this->unions[lastunions + 1].grouppattern_vec.size(); i++)
			{
				pair<Varset, Varset> sub_grouppattern_result = this->unions[lastunions + 1].grouppattern_vec[i].checkOptionalGroupPatternVarsAndSafeFilter(occur, ban, check_condition);

				if (i == 0)
					sub_grouppattern_occur = sub_grouppattern_occur + sub_grouppattern_result.first;
				else
					sub_grouppattern_occur = sub_grouppattern_occur * sub_grouppattern_result.first;
				sub_grouppattern_ban = sub_grouppattern_ban + sub_grouppattern_result.second;
			}

			occur = occur + sub_grouppattern_occur;
			this_ban = this_ban + sub_grouppattern_ban;
			ban = ban + this_ban;

			lastunions++;
		}
		else
		// triple pattern
		{
			if (this->patterns[lastpattern + 1].varset.hasCommonVar(ban))
				check_condition = false;

			occur = occur + this->patterns[lastpattern + 1].varset;

			lastpattern++;
		}
	}
	// filter
	for (int i = 0; i < (int)this->filters.size(); i++)
		if (!this->filters[i].varset.belongTo(occur))
		{
			check_condition = false;
			break;
		}

	return make_pair(occur, this_ban);
}

void QueryTree::GroupPattern::initPatternBlockid()
{
	this->pattern_blockid.clear();
	for (int i = 0; i < (int)this->patterns.size(); i++)
		this->pattern_blockid.push_back(i);
}

int QueryTree::GroupPattern::getRootPatternBlockID(int x)
{
	if (this->pattern_blockid[x] == x)
		return x;
	this->pattern_blockid[x] = getRootPatternBlockID(this->pattern_blockid[x]);
	return this->pattern_blockid[x];
}

void QueryTree::GroupPattern::mergePatternBlockID(int x, int y)
{
	int px = getRootPatternBlockID(x);
	int py = getRootPatternBlockID(y);
	this->pattern_blockid[px] = py;
}

void QueryTree::GroupPattern::print(int dep)
{
	for (int t = 0; t < dep; t++)
		printf("\t");
	printf("{\n");

	int lastpattern = -1, lastunions = -1, lastoptional = -1;
	while (lastpattern + 1 < (int)this->patterns.size() || lastunions + 1 < (int)this->unions.size() || lastoptional + 1 < (int)this->optionals.size())
	{
		if (lastoptional + 1 < (int)this->optionals.size() && this->optionals[lastoptional + 1].lastpattern == lastpattern && this->optionals[lastoptional + 1].lastunions == lastunions)
		// optional
		{
			for (int t = 0; t <= dep; t++)
				printf("\t");
			if (this->optionals[lastoptional + 1].type == 'o')
				printf("OPTIONAL\n");
			if (this->optionals[lastoptional + 1].type == 'm')
				printf("MINUS\n");

			this->optionals[lastoptional + 1].grouppattern.print(dep + 1);
			lastoptional++;
		}
		else if (lastunions + 1 < (int)this->unions.size() && this->unions[lastunions + 1].lastpattern == lastpattern)
		// union
		{
			for (int i = 0; i < (int)this->unions[lastunions + 1].grouppattern_vec.size(); i++)
			{
				if (i != 0)
				{
					for (int t = 0; t <= dep; t++)
						printf("\t");
					printf("UNION\n");
				}
				this->unions[lastunions + 1].grouppattern_vec[i].print(dep + 1);
			}
			lastunions++;
		}
		else
		// triple pattern
		{
			for (int t = 0; t <= dep; t++)
				printf("\t");
			printf("%s\t%s\t%s.\n", this->patterns[lastpattern + 1].subject.value.c_str(), this->patterns[lastpattern + 1].predicate.value.c_str(), this->patterns[lastpattern + 1].object.value.c_str());
			lastpattern++;
		}
	}
	// filter
	for (int i = 0; i < (int)this->filters.size(); i++)
	{
		for (int t = 0; t <= dep; t++)
			printf("\t");
		printf("FILTER\t");
		this->filters[i].root.print(this->filter_exists_grouppatterns[i], dep + 1);
		printf("\n");
	}

	for (int t = 0; t < dep; t++)
		printf("\t");
	printf("}\n");
}

//----------------------------------------------------------------------------------------------------------------------------------------------------

void QueryTree::setQueryForm(QueryForm _queryform)
{
	this->query_form = _queryform;
}

QueryTree::QueryForm QueryTree::getQueryForm()
{
	return this->query_form;
}

void QueryTree::setProjectionModifier(ProjectionModifier _projection_modifier)
{
	projection_modifier = _projection_modifier;
}

QueryTree::ProjectionModifier QueryTree::getProjectionModifier()
{
	return this->projection_modifier;
}

void QueryTree::addProjectionVar(string _projection)
{
	this->projection.addVar(_projection);
}

int QueryTree::getProjectionNum()
{
	return (int)this->projection.varset.size();
}

Varset &QueryTree::getProjection()
{
	return this->projection;
}

void QueryTree::setProjectionAsterisk()
{
	this->projection_asterisk = true;
}

bool QueryTree::checkProjectionAsterisk()
{
	return this->projection_asterisk;
}

void QueryTree::addOrder(string &_var, bool _descending)
{
	this->order.push_back(Order(_var, _descending));
}

vector<QueryTree::Order> &QueryTree::getOrder()
{
	return this->order;
}

void QueryTree::setOffset(int _offset)
{
	this->offset = _offset;
}

int QueryTree::getOffset()
{
	return this->offset;
}

void QueryTree::setLimit(int _limit)
{
	this->limit = _limit;
}

int QueryTree::getLimit()
{
	return this->limit;
}

QueryTree::GroupPattern &QueryTree::getGroupPattern()
{
	return this->grouppattern;
}

void QueryTree::setUpdateType(UpdateType _updatetype)
{
	this->update_type = _updatetype;
}

QueryTree::UpdateType QueryTree::getUpdateType()
{
	return this->update_type;
}

QueryTree::GroupPattern &QueryTree::getInsertPatterns()
{
	return this->insert_patterns;
}

QueryTree::GroupPattern &QueryTree::getDeletePatterns()
{
	return this->delete_patterns;
}

bool QueryTree::checkWellDesigned()
{
	if (!this->getGroupPattern().checkOnlyUnionOptionalFilterNoExists())
		return false;

	bool check_condition = true;
	this->getGroupPattern().checkOptionalGroupPatternVarsAndSafeFilter(Varset(), Varset(), check_condition);
	return check_condition;
}

void QueryTree::print()
{
	for (int j = 0; j < 80; j++)
		printf("=");
	printf("\n");

	if (this->getUpdateType() == Not_Update)
	{
		if (this->getQueryForm() == Select_Query)
		{
			printf("SELECT");
			if (this->getProjectionModifier() == Modifier_Distinct)
				printf(" distinct");
			printf("\n");

			printf("var : \t");
			vector<string> &varvec = this->getProjection().varset;
			for (int i = 0; i < (int)varvec.size(); i++)
				printf("%s\t", varvec[i].c_str());
			if (this->checkProjectionAsterisk())
				printf("*");
			printf("\n");
		}
		else
			printf("ASK\n");

		printf("group pattern : \n");
		this->getGroupPattern().print(0);

		if (this->getQueryForm() == Select_Query)
		{
			if ((int)this->getOrder().size() > 0)
			{
				printf("order by : \t");
				vector<QueryTree::Order> &order = this->getOrder();
				for (int i = 0; i < (int)order.size(); i++)
				{
					if (!order[i].descending)
						printf("asc(");
					else
						printf("desc(");
					printf("%s)  ", order[i].var.c_str());
				}
				printf("\n");
			}
			if (this->getOffset() != 0)
				printf("offset : %d\n", this->getOffset());
			if (this->getLimit() != -1)
				printf("limit : %d\n", this->getLimit());
		}
	}
	else
	{
		printf("UPDATE\n");
		if (this->getUpdateType() == Delete_Data || this->getUpdateType() == Delete_Where ||
			this->getUpdateType() == Delete_Clause || this->getUpdateType() == Modify_Clause)
		{
			printf("delete : \n");
			this->getDeletePatterns().print(0);
		}
		if (this->getUpdateType() == Insert_Data || this->getUpdateType() == Insert_Clause || this->getUpdateType() == Modify_Clause)
		{
			printf("insert : \n");
			this->getInsertPatterns().print(0);
		}
		if (this->getUpdateType() == Delete_Where || this->getUpdateType() == Insert_Clause ||
			this->getUpdateType() == Delete_Clause || this->getUpdateType() == Modify_Clause)
		{
			printf("group pattern : \n");
			this->getGroupPattern().print(0);
		}
	}

	for (int j = 0; j < 80; j++)
		printf("=");
	printf("\n");
}

int QueryTree::checkStar()
{
	vector<QueryTree::GroupPattern::Pattern> p_vec = this->getGroupPattern().patterns;

	if (p_vec.size() <= 1)
		return 1;

	string center_var;
	if (p_vec[0].subject.value.compare(p_vec[1].subject.value) == 0 || p_vec[0].subject.value.compare(p_vec[1].object.value) == 0)
	{
		center_var = p_vec[0].subject.value;
	}
	else if (p_vec[0].object.value.compare(p_vec[1].subject.value) == 0 || p_vec[0].object.value.compare(p_vec[1].object.value) == 0)
	{
		center_var = p_vec[0].object.value;
	}
	else
	{
		return 0;
	}

	for (int i = 2; i < p_vec.size(); i++)
	{
		if (p_vec[i].subject.value.compare(center_var) != 0 && p_vec[i].object.value.compare(center_var) != 0)
		{
			return 0;
		}
	}

	return 1;
}

int QueryTree::checkStar(vector<vector<int>> &_query_adjacent_list)
{
	vector<QueryTree::GroupPattern::Pattern> p_vec = this->getGroupPattern().patterns;

	map<string, int> varIDMap;
	vector<string> var_vec = this->getProjection().varset;
	for (int i = 0; i < var_vec.size(); i++)
	{
		varIDMap.insert(make_pair(var_vec[i], i));
		_query_adjacent_list.push_back(vector<int>());
	}

	for (int i = 0; i < p_vec.size(); i++)
	{
		if (p_vec[i].subject.value.at(0) == '?' && p_vec[i].object.value.at(0) == '?')
		{
			_query_adjacent_list[varIDMap[p_vec[i].subject.value]].push_back(varIDMap[p_vec[i].object.value]);
			_query_adjacent_list[varIDMap[p_vec[i].object.value]].push_back(varIDMap[p_vec[i].subject.value]);
		}
	}

	if (p_vec.size() <= 1)
		return 1;

	string center_var;
	if (p_vec[0].subject.value.compare(p_vec[1].subject.value) == 0 || p_vec[0].subject.value.compare(p_vec[1].object.value) == 0)
	{
		center_var = p_vec[0].subject.value;
	}
	else if (p_vec[0].object.value.compare(p_vec[1].subject.value) == 0 || p_vec[0].object.value.compare(p_vec[1].object.value) == 0)
	{
		center_var = p_vec[0].object.value;
	}
	else
	{
		return 0;
	}

	for (int i = 2; i < p_vec.size(); i++)
	{
		if (p_vec[i].subject.value.compare(center_var) != 0 && p_vec[i].object.value.compare(center_var) != 0)
		{
			return 0;
		}
	}

	return 1;
}

bool cmp(pair<string, QueryTree::Vertex> a, pair<string, QueryTree::Vertex> b)
{
	return a.second.degree > b.second.degree;
}

void getCutting(vector<vector<vector<string>>> &res, vector<vector<string>> &subgraphs, vector<vector<string>> &sta, int target, int now)
{
	int sizeSta = 0;
	for (int i = 0; i < sta.size(); i++)
	{
		sizeSta += sta[i].size();
	}
	if (sizeSta == target)
	{
		cout << "结果：" << endl;
		for (int i = 0; i < sta.size(); i++)
		{
			for (int j = 0; j < sta[i].size(); j++)
			{
				cout << sta[i][j] << "  ";
			}
			cout << endl;
		}
		cout << "结果========" << endl;
		res.push_back(sta);
		return;
	}

	if (now >= subgraphs.size())
	{
		return;
	}

	bool cover = false;
	int staLen = sta.size();
	for (int i = 0; i < staLen; i++)
	{
		// cout<<i<<endl;
		for (int j = 0; j < sta[i].size(); j++)
		{
			for (int k = 0; k < subgraphs[now].size(); k++)
			{
				if (sta[i][j] == subgraphs[now][k])
				{
					cover = true;
					break;
				}
			}
			if (cover)
			{
				break;
			}
		}
		if (cover)
		{
			break;
		}
	}

	if (cover)
	{
		getCutting(res, subgraphs, sta, target, now + 1);
	}
	else
	{
		sta.push_back(subgraphs[now]);

		getCutting(res, subgraphs, sta, target, now + 1);
		sta.pop_back();
		getCutting(res, subgraphs, sta, target, now + 1);
	}
}

bool isConnected(map<string, QueryTree::Vertex> &map, vector<string> graph)
{
	if (graph.size() <= 1)
	{
		return true;
	}
	vector<string> component;
	component.push_back(graph[0]);
	// graph[0]="null";

	int comBegin = 0;
	int comLast = -1;
	while (comBegin > comLast)
	{
		for (int i = comBegin; i < component.size(); i++)
		{
			vector<QueryTree::Edge> edges = map[component[i]].in_edges;
			vector<QueryTree::Edge> out_edges = map[component[i]].out_edges;
			edges.insert(edges.end(), out_edges.begin(), out_edges.end());
			for (int j = 0; j < edges.size(); j++)
			{
				string ad = edges[j].ad_vertex_value;
				for (int k = 0; k < graph.size(); k++)
				{
					if (graph[k] == ad)
					{
						component.push_back(ad);
						graph[k] = "null";
					}
				}
			}
		}
		comLast = comBegin;
		comBegin = component.size() - 1;
	}
	if (comBegin == graph.size())
	{
		return true;
	}
	return false;
}

std::vector<std::vector<std::vector<QueryTree::Vertex>>> QueryTree::getStarQuery()
{

	vector<QueryTree::GroupPattern::Pattern> p_vec = this->getGroupPattern().patterns;

	// 建立map，存入图的信息
	map<string, QueryTree::Vertex> v_map;
	for (int i = 0; i < p_vec.size(); i++)
	{
		string subject = p_vec[i].subject.value;
		string predicate = p_vec[i].predicate.value;
		string object = p_vec[i].object.value;

		if (v_map.find(subject) == v_map.end())
		{
			QueryTree::Vertex vertex;
			vertex.vertex_value = subject;

			QueryTree::Edge edge;
			edge.ad_vertex_value = object;
			edge.edge_value = predicate;

			vertex.out_edges.push_back(edge);
			vertex.degree = 1;

			v_map.insert(make_pair(subject, vertex));
		}
		else
		{
			QueryTree::Edge edge;
			edge.ad_vertex_value = object;
			edge.edge_value = predicate;
			v_map[subject].out_edges.push_back(edge);
			v_map[subject].degree++;
		}

		if (v_map.find(object) == v_map.end())
		{
			QueryTree::Vertex vertex;
			vertex.vertex_value = object;

			QueryTree::Edge edge;
			edge.ad_vertex_value = subject;
			edge.edge_value = predicate;

			vertex.in_edges.push_back(edge);
			vertex.degree = 1;

			v_map.insert(make_pair(object, vertex));
		}
		else
		{
			QueryTree::Edge edge;
			edge.ad_vertex_value = subject;
			edge.edge_value = predicate;
			v_map[object].in_edges.push_back(edge);
			v_map[object].degree++;
		}
	}

	map<string, QueryTree::Vertex> graph_v_map = v_map;
	for (map<string, QueryTree::Vertex>::iterator itVer = graph_v_map.begin(); itVer != graph_v_map.end();)
	{
		if (itVer->second.degree <= 1)
		{
			// 对点的入边进行处理
			vector<Edge> in_edges = itVer->second.in_edges;
			for (int i = 0; i < in_edges.size(); i++)
			{
				Edge edge = in_edges[i];
				string ad_vertex_value = edge.ad_vertex_value;
				Vertex ad_vertex = graph_v_map[ad_vertex_value];

				vector<Edge> out_edges = ad_vertex.out_edges;
				vector<Edge>::iterator it;
				for (it = out_edges.begin(); it != out_edges.end();)
				{
					if ((*it).ad_vertex_value == itVer->first)
					{
						it = out_edges.erase(it);
					}
					else
					{
						++it;
					}
				}

				ad_vertex.out_edges = out_edges;
				// ad_vertex.degree--;

				graph_v_map[ad_vertex_value] = ad_vertex;

				for (int i = 0; i < ad_vertex.out_edges.size(); i++)
				{
					cout << ad_vertex.out_edges[i].ad_vertex_value << "++++" << ad_vertex.out_edges[i].edge_value << endl;
				}
			}

			// 对点的出边进行处理
			vector<Edge> out_edges = itVer->second.out_edges;
			for (int i = 0; i < out_edges.size(); i++)
			{
				Edge edge = out_edges[i];
				string ad_vertex_value = edge.ad_vertex_value;
				Vertex ad_vertex = graph_v_map[ad_vertex_value];

				vector<Edge> in_edges = ad_vertex.in_edges;
				vector<Edge>::iterator it;
				for (it = in_edges.begin(); it != in_edges.end();)
				{

					if ((*it).ad_vertex_value == itVer->first)
					{
						it = in_edges.erase(it);
					}
					else
					{
						++it;
					}
				}

				ad_vertex.in_edges = in_edges;
				// ad_vertex.degree--;

				graph_v_map[ad_vertex_value] = ad_vertex;

				for (int i = 0; i < ad_vertex.in_edges.size(); i++)
				{
					cout << ad_vertex.in_edges[i].ad_vertex_value << "++++" << ad_vertex.in_edges[i].edge_value << endl;
				}
			}

			graph_v_map.erase(itVer++);
		}
		else
		{
			itVer++;
		}
	}

	vector<string> neighbor;
	for (map<string, QueryTree::Vertex>::iterator itVer = graph_v_map.begin(); itVer != graph_v_map.end(); itVer++)
	{
		neighbor.push_back(itVer->first);
	}

	vector<vector<string>> combination;
	vector<string> none;
	combination.push_back(none);
	int cur = 0;
	while (cur < neighbor.size())
	{
		int comSize = combination.size();
		for (int i = 0; i < comSize; i++)
		{
			vector<string> newOne = combination[i];
			newOne.push_back(neighbor[cur]);
			combination.push_back(newOne);
		}

		cur++;
	}

	vector<vector<string>> subgraphs;
	for (int i = 0; i < combination.size(); i++)
	{
		if (combination[i].size() == 0)
		{
			continue;
		}
		if (isConnected(graph_v_map, combination[i]))
		{
			subgraphs.push_back(combination[i]);
		}
	}

	vector<vector<vector<string>>> res;
	vector<vector<string>> sta;

	getCutting(res, subgraphs, sta, neighbor.size(), 0);

	vector<vector<vector<Vertex>>> result;
	for (int k = 0; k < res.size(); k++)
	{
		vector<vector<string>> sta = res[k];
		cout << "result" << k << ":   " << endl;
		vector<vector<Vertex>> verss;
		for (int i = 0; i < sta.size(); i++)
		{
			vector<Vertex> vers;
			for (int j = 0; j < sta[i].size(); j++)
			{
				vers.push_back(v_map[sta[i][j]]);
				cout << sta[i][j] << "   ";
			}
			verss.push_back(vers);
			cout << endl;
		}
		result.push_back(verss);
	}

	return result;
}

std::vector<string> QueryTree::getSparql(std::vector<std::vector<QueryTree::Vertex>> vertexs)
{
	set<string> vars;

	// 添加变量
	Varset re = this->getProjection();
	cout << "vars" << endl;
	for (int i = 0; i < re.varset.size(); i++)
	{
		cout << re.varset[i] << endl;
		vars.insert(re.varset[i]);
	}

	// 添加交的点
	set<string> joinVar;

	for (int i = 0; i < vertexs.size() - 1; i++)
	{
		for (int j = i + 1; j < vertexs.size(); j++)
		{
			for (int m = 0; m < vertexs[i].size(); m++)
			{
				for (int n = 0; n < vertexs[j].size(); n++)
				{
					string vertex = vertexs[i][m].vertex_value;
					vector<Edge> inedges = vertexs[j][n].in_edges;
					for (int l = 0; l < inedges.size(); l++)
					{
						if (inedges[l].ad_vertex_value == vertex)
						{
							joinVar.insert(vertex);
							joinVar.insert(vertexs[j][n].vertex_value);
						}
					}
					vector<Edge> outedges = vertexs[j][n].out_edges;
					for (int l = 0; l < outedges.size(); l++)
					{
						if (outedges[l].ad_vertex_value == vertex)
						{
							joinVar.insert(vertex);
							joinVar.insert(vertexs[j][n].vertex_value);
						}
					}
				}
			}
		}
	}

	// 记录内外部点
	map<string, bool> joinVarMap;
	cout << "joinVar" << endl;
	for (set<string>::iterator it = joinVar.begin(); it != joinVar.end(); it++)
	{
		joinVarMap[(*it)] = false;
		cout << (*it) << endl;
	}

	vector<string> sparqls;

	for (int k = 0; k < vertexs.size(); k++)
	{
		map<string, bool> joinVarMapUsed = joinVarMap;

		string sparql = "select ";
		// 入边标签
		string inWhere = "";
		// 出边标签
		string outWhere = "";
		// 变量语句
		set<string> varString;
		// 条件语句
		set<string> conditionString;
		
		vector<Vertex> vertex = vertexs[k];

		// 内部点首先进行标记
		for (int i = 0; i < vertex.size(); i++)
		{
			Vertex ver = vertex[i];
			inWhere += ver.vertex_value + " <http://property/isInCurrPart> \"1\"  .\n";
			joinVarMapUsed[ver.vertex_value] = true;
		}

		// 拼语句
		for (int i = 0; i < vertex.size(); i++)
		{
			Vertex ver = vertex[i];

			// // 添加该节点
			// if (vars.find(ver.vertex_value) != vars.end())
			// {
			// 	varString.insert(ver.vertex_value);
			// }
			varString.insert(ver.vertex_value);

			// 搜索入边
			for (int j = 0; j < ver.in_edges.size(); j++)
			{
				string ad_vertex = ver.in_edges[j].ad_vertex_value;
				string edge = ver.in_edges[j].edge_value;
				// 添加边
				conditionString.insert(ad_vertex + " " + edge + " " + ver.vertex_value + ".\n");

				// // 添加临界变量
				// if (vars.find(ad_vertex) != vars.end())
				// {
				// 	varString.insert(ad_vertex);
				// }
				varString.insert(ad_vertex);
				// 添加临界标签
				if (joinVar.find(ad_vertex) != joinVar.end())
				{

					if (!joinVarMapUsed[ad_vertex])
					{
						varString.insert(ad_vertex);
						inWhere += ad_vertex + " <http://property/isInCurrPart> \"0\"  .\n";
						joinVarMapUsed[ad_vertex] = true;
					}
				}
			}

			// 搜索出边
			for (int j = 0; j < ver.out_edges.size(); j++)
			{
				string ad_vertex = ver.out_edges[j].ad_vertex_value;
				string edge = ver.out_edges[j].edge_value;
				conditionString.insert(ver.vertex_value + " " + edge + " " + ad_vertex + ".\n");

				// if (vars.find(ad_vertex) != vars.end())
				// {
				// 	varString.insert(ad_vertex);
				// }
				varString.insert(ad_vertex);
				if (joinVar.find(ad_vertex) != joinVar.end())
				{
					if (!joinVarMapUsed[ad_vertex])
					{
						varString.insert(ad_vertex);
						outWhere += ad_vertex + " <http://property/isInCurrPart> \"0\"  .\n";
						joinVarMapUsed[ad_vertex] = true;
					}
				}
			}
		}

		for (set<string>::iterator it = varString.begin(); it != varString.end(); it++)
		{
			if((*it)[0]=='?'){
				sparql += (*it)+" " ;
			}
		}

		sparql += string("where {") + "\n" + inWhere + outWhere ;

		for (set<string>::iterator it = conditionString.begin(); it != conditionString.end(); it++)
		{
			sparql += (*it) ;
		}

		sparql += string( "}");
		cout << sparql << endl;

		sparqls.push_back(sparql);
	}
	return sparqls;
}
