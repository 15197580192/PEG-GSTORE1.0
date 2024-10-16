/*
 * @Filename: PEG_SSPPR.cpp
 * @Author: Hu Zheyuan
 * @Date: 2023-02-22 16:16:10
 * @LastEditors: Hu Zheyuan
 * @Description: Single Source Personal Page Rank
 */
#include <iostream>
#include <string>
#include <vector>

#include "handler/Handler.h"
#include "util/util.h"
#include "crow/json.h"
#include "gStoreAPI/client.h"

using namespace std;

crow::json::rvalue confJson;
vector<GstoreConnector *> servers;
int n = -1; // dbpedia 所有顶点数
string dbname;
std::vector<string> entity2str;
std::vector<string> property2str;
// 顶点索引
std::unordered_map<std::string, std::pair<int,int>> entity2id;
std::unordered_map<std::string, int> property2id;

/**
 * A custom vector. For some algorithms in PPR.
 */
template <typename _T>
class iVector
{
public:
    unsigned int m_size;
    _T *m_data;
    unsigned int m_num;

    void free_mem()
    {
        delete[] m_data;
    }

    iVector()
    {
        m_size = 20;
        m_data = new _T[20];
        m_num = 0;
    }
    iVector(unsigned int n)
    {
        if (n == 0)
        {
            n = 20;
        }
        m_size = n;
        m_data = new _T[m_size];
        m_num = 0;
    }
    void push_back(_T d)
    {
        if (m_num == m_size)
        {
            re_allocate(m_size * 2);
        }
        m_data[m_num] = d;
        m_num++;
    }
    void push_back(const _T *p, unsigned int len)
    {
        while (m_num + len > m_size)
        {
            re_allocate(m_size * 2);
        }
        memcpy(m_data + m_num, p, sizeof(_T) * len);
        m_num += len;
    }

    void re_allocate(unsigned int size)
    {
        if (size < m_num)
        {
            return;
        }
        _T *tmp = new _T[size];
        memcpy(tmp, m_data, sizeof(_T) * m_num);
        m_size = size;
        delete[] m_data;
        m_data = tmp;
    }
    void Sort()
    {
        if (m_num < 20)
        {
            int k;
            _T tmp;
            for (int i = 0; i < m_num - 1; ++i)
            {
                k = i;
                for (int j = i + 1; j < m_num; ++j)
                    if (m_data[j] < m_data[k])
                        k = j;
                if (k != i)
                {
                    tmp = m_data[i];
                    m_data[i] = m_data[k];
                    m_data[k] = tmp;
                }
            }
        }
        else
            sort(m_data, m_data + m_num);
    }
    void unique()
    {
        if (m_num == 0)
            return;
        Sort();
        unsigned int j = 0;
        for (unsigned int i = 0; i < m_num; ++i)
            if (!(m_data[i] == m_data[j]))
            {
                ++j;
                if (j != i)
                    m_data[j] = m_data[i];
            }
        m_num = j + 1;
    }
    int BinarySearch(_T &data)
    {
        for (int x = 0, y = m_num - 1; x <= y;)
        {
            int p = (x + y) / 2;
            if (m_data[p] == data)
                return p;
            if (m_data[p] < data)
                x = p + 1;
            else
                y = p - 1;
        }
        return -1;
    }
    void clean()
    {
        m_num = 0;
    }
    void assign(iVector &t)
    {
        m_num = t.m_num;
        m_size = t.m_size;
        delete[] m_data;
        m_data = t.m_data;
    }

    bool remove(_T &x)
    {
        for (int l = 0, r = m_num; l < r;)
        {
            int m = (l + r) / 2;

            if (m_data[m] == x)
            {
                m_num--;
                if (m_num > m)
                    memmove(m_data + m, m_data + m + 1, sizeof(_T) * (m_num - m));
                return true;
            }
            else if (m_data[m] < x)
                l = m + 1;
            else
                r = m;
        }
        return false;
    }

    void sorted_insert(_T &x)
    {
        if (m_num == 0)
        {
            push_back(x);
            return;
        }

        if (m_num == m_size)
            re_allocate(m_size * 2);

        int l, r;

        for (l = 0, r = m_num; l < r;)
        {
            int m = (l + r) / 2;
            if (m_data[m] < x)
                l = m + 1;
            else
                r = m;
        }

        if (m_num > l)
        {
            memmove(m_data + l + 1, m_data + l, sizeof(_T) * (m_num - l));
        }
        m_num++;
        m_data[l] = x;
    }

    bool remove_unsorted(_T &x)
    {
        for (int m = 0; m < m_num; ++m)
        {
            if (m_data[m] == x)
            {
                m_num--;
                if (m_num > m)
                    memcpy(m_data + m, m_data + m + 1, sizeof(_T) * (m_num - m));
                return true;
            }
        }
        return false;
    }

    _T &operator[](unsigned int i)
    {
        return m_data[i];
    }
    // close range check for [] in iVector if release
};

/**
 * A custom map. For some algorithms in PPR.
 */
template <typename _T>
struct iMap
{
    _T *m_data;
    int m_num;
    int cur;
    iVector<int> occur;
    _T nil;
    iMap()
    {
        m_data = NULL;
        m_num = 0;
    }
    iMap(int size)
    {
        initialize(size);
    }
    void free_mem()
    {
        delete[] m_data;
        occur.free_mem();
    }

    void initialize(int n)
    {
        occur.re_allocate(n);
        occur.clean();
        m_num = n;
        if (m_data != NULL)
            delete[] m_data;
        m_data = new _T[m_num];
        for (int i = 0; i < m_num; ++i)
            m_data[i] = nil;
        cur = 0;
    }
    void clean()
    {
        for (int i = 0; i < occur.m_num; ++i)
        {
            m_data[occur[i]] = nil;
        }
        occur.clean();
        cur = 0;
    }

    // init keys 0-n, value as 0
    void init_keys(int n)
    {
        occur.re_allocate(n);
        occur.clean();
        m_num = n;
        if (m_data != NULL)
            delete[] m_data;
        m_data = new _T[m_num];
        for (int i = 0; i < m_num; ++i)
        {
            m_data[i] = 0;
            occur.push_back(i);
            cur++;
        }
    }
    // reset all values to be zero
    void reset_zero_values()
    {
        memset(m_data, 0.0, m_num * sizeof(_T));
    }

    void reset_one_values()
    {
        for (int i = 0; i < m_num; ++i)
            m_data[i] = 1.0;
    }

    _T get(int p)
    {
        return m_data[p];
    }
    _T &operator[](int p)
    {
        return m_data[p];
    }
    void erase(int p)
    {
        m_data[p] = nil;
        cur--;
    }
    bool notexist(int p)
    {
        return m_data[p] == nil;
    }
    bool exist(int p)
    {
        return !(m_data[p] == nil);
    }
    void insert(int p, _T d)
    {
        if (m_data[p] == nil)
        {
            occur.push_back(p);
            cur++;
        }
        m_data[p] = d;
    }
    void inc(int p)
    {
        m_data[p]++;
    }
    void inc(int p, int x)
    {
        m_data[p] += x;
    }
    void dec(int p)
    {
        m_data[p]--;
    }
    // close range check when release!!!!!!!!!!!!!!!!!!!!
};

void SSPPR(int uid, int retNum, int k, const std::vector<int> &pred_set, std::vector< std::pair<int ,double> > &topkV2ppr);
// Helper functions for SSPPR
void compute_ppr_with_reserve(std::pair<iMap<double>, iMap<double>> &fwd_idx, std::unordered_map<int, double> &v2ppr);
void forward_local_update_linear_topk(int s, double& rsum, double rmax, double lowest_rmax, \
    std::vector<std::pair<int, int>>& forward_from, std::pair<iMap<double>, iMap<double>> &fwd_idx, \
    const std::vector<int> &pred_set, double alpha, int k);
void compute_ppr_with_fwdidx_topk_with_bound(double check_rsum, std::pair<iMap<double>, iMap<double>> &fwd_idx, \
    std::unordered_map<int, double> &v2ppr, double delta, double alpha, double threshold, \
    const std::vector<int> &pred_set, double pfail, double &zero_ppr_upper_bound, double omega, \
    iMap<double> &upper_bounds, iMap<double> &lower_bounds);
void set_ppr_bounds(std::pair<iMap<double>, iMap<double>> &fwd_idx, double rsum, long total_rw_num, \
    std::unordered_map<int, double> &v2ppr, double pfail, double &zero_ppr_upper_bound, \
    iMap<double> &upper_bounds, iMap<double> &lower_bounds);
inline double calculate_lambda(double rsum, double pfail, double upper_bound, long total_rw_num);
inline int random_walk(int start, double alpha, const std::vector<int> &pred_set);
double kth_ppr(std::unordered_map<int, double> &v2ppr, int retNum);
bool if_stop(int retNum, double delta, double threshold, double epsilon, iMap<int> &topk_filter, \
    iMap<double> &upper_bounds, iMap<double> &lower_bounds, std::unordered_map<int, double> &v2ppr);



int init(string dbname) {
    string line, part;
    ifstream in;
    string filename="entity/"+dbname+"InternalPoints.txt";
    string filename1="entity/"+dbname+"Property.txt";
    in.open(filename);
    int i = 0;
    while (in >> line >> part) {
        entity2str.push_back(line);
        entity2id[line] = std::make_pair(i++, stoi(part));
    }
    in.close();
    n=entity2id.size();

    in.open(filename1);
    i = 0;
    while (in >> line) {
        property2str.push_back(line);
        property2id[line] = i++;
    }
    in.close();
    n=entity2id.size();
}

int getEntityId(string u)
{
    if (n == -1)
        return -1;
    if (entity2id.find(u) == entity2id.end())
        return -1;
    return entity2id[u].first;
}
string getEntityStr(int id)
{
    return entity2str[id];
}

int getPropertyId(string u)
{
    if (property2id.find(u) == property2id.end())
        return -1;
    return property2id[u];
}
string getPropertyStr(int id)
{
    return property2str[id];
}
vector<int> getPropertyId(vector<string> str)
{
    vector<int> ids;
    for(auto i: str) {
        ids.push_back(getPropertyId(i));
    }
    return ids;
}
vector<string> getPropertyStr(vector<int> id)
{
    vector<string> strs;
    for(auto i: id) {
        strs.push_back(getPropertyStr(i));
    }
    return strs;
}


int getVertNum()
{
    if (n != -1)
        return n;
    init(dbname);
    n = entity2id.size();
    return n;
}

int getSetEdgeNum(const std::vector<int> &pred_set) {
    return 96989809;
}

int getOutSize(int vid, int pred) {
    string u = getEntityStr(vid);
    string p = getPropertyStr(pred);
    long long resNum;
    queryNeighbor2(confJson, servers,dbname,u,p,true,resNum,entity2id[u].second);
    return (int)resNum;
}

int getSetOutSize(int vid, const std::vector<int> &pred_set) {
    string u = getEntityStr(vid);
    long long resNum;
    // cout<<pred_set.size()<<endl;
    if(pred_set.size()==0) {
        // cout<<resNum<<endl;
        queryNeighbor2(confJson, servers,dbname,u,"",true,resNum,entity2id[u].second);
        // cout<<resNum<<endl;
        return resNum;
    }
    set<string> res;
    for(auto i: pred_set) {
        string p=getPropertyStr(i);
        set<string> tmp = queryNeighbor2(confJson, servers,dbname,u,p,true,resNum,entity2id[u].second)[p];
        res.insert(tmp.begin(),tmp.end());
    }
    // cout<<res.size()<<endl;
    return res.size();
}

int getOutVertID(int vid, int pred, int pos) {
    string u = getEntityStr(vid);
    string p=getPropertyStr(pred);
    long long resNum;
    set<string> tmp = queryNeighbor2(confJson, servers,dbname,u,p,true,resNum,entity2id[u].second)[p];
    vector<string> vec;
    vec.assign(tmp.begin(),tmp.end());
    return getEntityId(vec[pos]);
}


// retNum is the number of top nodes to return; k is the hop constraint -- don't mix them up!
void SSPPR(int uid, int retNum, int k, const vector<int> &pred_set, vector<pair<int, double>> &topkV2ppr)
{
    topkV2ppr.clear();
    srand(time(NULL));

    unordered_map<int, double> v2ppr;
    pair<iMap<double>, iMap<double>> fwd_idx;
    iMap<double> ppr;
    iMap<int> topk_filter;
    iMap<double> upper_bounds;
    iMap<double> lower_bounds;

    // Data structures initialization
    fwd_idx.first.nil = -9;
    fwd_idx.first.initialize(getVertNum());
    fwd_idx.second.nil = -9;
    fwd_idx.second.initialize(getVertNum());
    upper_bounds.nil = -9;
    upper_bounds.init_keys(getVertNum());
    lower_bounds.nil = -9;
    lower_bounds.init_keys(getVertNum());
    ppr.nil = -9;
    ppr.initialize(getVertNum());
    topk_filter.nil = -9;
    topk_filter.initialize(getVertNum());

    // Params initialization
    int numPredEdges = getSetEdgeNum(pred_set);
    double ppr_decay_alpha = 0.77;
    double pfail = 1.0 / getVertNum() / getVertNum() / log(getVertNum()); // log(1/pfail) -> log(1*n/pfail)
    double delta = 1.0 / 4;
    double epsilon = 0.5;
    double rmax;
    double rmax_scale = 1.0;
    double omega;
    double alpha = 0.2;
    double min_delta = 1.0 / getVertNum();
    double threshold = (1.0 - ppr_decay_alpha) / pow(500, ppr_decay_alpha) / pow(getVertNum(), 1 - ppr_decay_alpha);
    double lowest_delta_rmax = epsilon * sqrt(min_delta / 3 / numPredEdges / log(2 / pfail));
    double rsum = 1.0;

    vector<pair<int, int>> forward_from;
    forward_from.clear();
    forward_from.reserve(getVertNum());
    forward_from.push_back(make_pair(uid, 0));

    fwd_idx.first.clean();  // reserve
    fwd_idx.second.clean(); // residual
    fwd_idx.second.insert(uid, rsum);

    double zero_ppr_upper_bound = 1.0;
    upper_bounds.reset_one_values();
    lower_bounds.reset_zero_values();

    cout<<"ppr begin"<<endl;


    // fora_query_topk_with_bound
    // for delta: try value from 1/4 to 1/n
    int iteration = 0;
    while (delta >= min_delta)
    {
        rmax = epsilon * sqrt(delta / 3 / numPredEdges / log(2 / pfail));
        rmax *= rmax_scale;
        omega = (2 + epsilon) * log(2 / pfail) / delta / epsilon / epsilon;
        if (getSetOutSize(uid, pred_set) == 0)
        {
            rsum = 0.0;
            fwd_idx.first.insert(uid, 1);
            compute_ppr_with_reserve(fwd_idx, v2ppr);
            return;
        }
        else {
            // cout<<"forwardpush"<<endl;
            forward_local_update_linear_topk(uid, rsum, rmax, lowest_delta_rmax, forward_from, fwd_idx, pred_set, alpha, k); // forward propagation, obtain reserve and residual
        
        }
        compute_ppr_with_fwdidx_topk_with_bound(rsum, fwd_idx, v2ppr, delta, alpha, threshold,
                                                pred_set, pfail, zero_ppr_upper_bound, omega, upper_bounds, lower_bounds);
        if (if_stop(retNum, delta, threshold, epsilon, topk_filter, upper_bounds, lower_bounds, v2ppr) || delta <= min_delta)
            break;
        else
            delta = max(min_delta, delta / 2.0); // otherwise, reduce delta to delta/2
    }

    cout<<"top-k results begin"<<endl;

    // Extract top-k results
    topkV2ppr.clear();
    topkV2ppr.resize(retNum);
    partial_sort_copy(v2ppr.begin(), v2ppr.end(), topkV2ppr.begin(), topkV2ppr.end(),
                      [](pair<int, double> const &l, pair<int, double> const &r)
                      { return l.second > r.second; });
    size_t i = topkV2ppr.size() - 1;
    while (topkV2ppr[i].second == 0)
        i--;
    topkV2ppr.erase(topkV2ppr.begin() + i + 1, topkV2ppr.end()); // Get rid of ppr = 0 entries
}

void compute_ppr_with_reserve(pair<iMap<double>, iMap<double>> &fwd_idx, unordered_map<int, double> &v2ppr)
{
    int node_id;
    double reserve;
    for (long i = 0; i < fwd_idx.first.occur.m_num; i++)
    {
        node_id = fwd_idx.first.occur[i];
        reserve = fwd_idx.first[node_id];
        if (reserve)
            v2ppr[node_id] = reserve;
    }
}

void forward_local_update_linear_topk(int s, double &rsum, double rmax, double lowest_rmax, vector<pair<int, int>> &forward_from,
                                      pair<iMap<double>, iMap<double>> &fwd_idx, const vector<int> &pred_set, double alpha, int k)
{
    double myeps = rmax;
    vector<bool> in_forward(getVertNum());
    vector<bool> in_next_forward(getVertNum());

    std::fill(in_forward.begin(), in_forward.end(), false);
    std::fill(in_next_forward.begin(), in_next_forward.end(), false);

    vector<pair<int, int>> next_forward_from;
    next_forward_from.reserve(getVertNum());
    for (auto &v : forward_from)
        in_forward[v.first] = true;

    unsigned long i = 0;
    while (i < forward_from.size())
    {
        int v = forward_from[i].first;
        int level = forward_from[i].second;
        i++;
        // if (k != -1 && level >= k)
        // 	continue;
        in_forward[v] = false;
        if (fwd_idx.second[v] / getSetOutSize(v, pred_set) >= myeps)
        {
            int out_neighbor = getSetOutSize(v, pred_set);
            double v_residue = fwd_idx.second[v];
            fwd_idx.second[v] = 0;
            if (!fwd_idx.first.exist(v))
                fwd_idx.first.insert(v, v_residue * alpha);
            else
                fwd_idx.first[v] += v_residue * alpha;

            rsum -= v_residue * alpha;
            if (out_neighbor == 0)
            {
                fwd_idx.second[s] += v_residue * (1 - alpha);
                if (getSetOutSize(s, pred_set) > 0 && in_forward[s] != true && fwd_idx.second[s] / getSetOutSize(s, pred_set) >= myeps)
                {
                    // forward_from.push_back(make_pair(s, level + 1));
                    forward_from.push_back(make_pair(s, 0));
                    in_forward[s] = true;
                }
                else if (getSetOutSize(s, pred_set) >= 0 && in_next_forward[s] != true && fwd_idx.second[s] / getSetOutSize(s, pred_set) >= lowest_rmax)
                {
                    // next_forward_from.push_back(make_pair(s, level + 1));
                    next_forward_from.push_back(make_pair(s, 0));
                    in_next_forward[s] = true;
                }
                continue;
            }
            double avg_push_residual = ((1 - alpha) * v_residue) / out_neighbor;
            // int out_neighbor_test = 0;
            // cout << "out_neighbor = " << out_neighbor << endl;
            for (int pred : pred_set)
            {
                int out_neighbor_pred = getOutSize(v, pred);
                // out_neighbor_test += out_neighbor_pred;
                for (int i = 0; i < out_neighbor_pred; i++)
                {
                    int next = getOutVertID(v, pred, i);
                    if (next == -1)
                    {
                        cout << "ERROR!!!!!!" << endl;
                        exit(0); // TODO: throw an exception
                    }

                    if (!fwd_idx.second.exist(next))
                        fwd_idx.second.insert(next, avg_push_residual);
                    else
                        fwd_idx.second[next] += avg_push_residual;

                    if (in_forward[next] != true && fwd_idx.second[next] / getSetOutSize(next, pred_set) >= myeps && (k == -1 || level < k))
                    {
                        forward_from.push_back(make_pair(next, level + 1));
                        in_forward[next] = true;
                    }
                    else
                    {
                        if (in_next_forward[next] != true && fwd_idx.second[next] / getSetOutSize(next, pred_set) >= lowest_rmax && (k == -1 || level < k))
                        {
                            next_forward_from.push_back(make_pair(next, level + 1));
                            in_next_forward[next] = true;
                        }
                    }
                }
            }
        }
        else
        {
            if (in_next_forward[v] != true && fwd_idx.second[v] / getSetOutSize(v, pred_set) >= lowest_rmax)
            {
                next_forward_from.push_back(make_pair(v, level));
                in_next_forward[v] = true;
            }
        }
    }

    forward_from = next_forward_from;
}

void compute_ppr_with_fwdidx_topk_with_bound(double check_rsum, pair<iMap<double>, iMap<double>> &fwd_idx,
                                             unordered_map<int, double> &v2ppr, double delta, double alpha, double threshold,
                                             const vector<int> &pred_set, double pfail, double &zero_ppr_upper_bound, double omega,
                                             iMap<double> &upper_bounds, iMap<double> &lower_bounds)
{
    compute_ppr_with_reserve(fwd_idx, v2ppr);

    if (check_rsum == 0.0)
        return;

    long num_random_walk = omega * check_rsum;
    long real_num_rand_walk = 0;

    for (long i = 0; i < fwd_idx.second.occur.m_num; i++)
    {
        int source = fwd_idx.second.occur[i];
        double residual = fwd_idx.second[source];
        long num_s_rw = ceil(residual / check_rsum * num_random_walk);
        double a_s = residual / check_rsum * num_random_walk / num_s_rw;

        real_num_rand_walk += num_s_rw;

        double ppr_incre = a_s * check_rsum / num_random_walk;
        for (long j = 0; j < num_s_rw; j++)
        {
            int des = random_walk(source, alpha, pred_set);
            if (v2ppr.find(des) == v2ppr.end())
                v2ppr[des] = ppr_incre;
            else
                v2ppr[des] += ppr_incre;
        }
    }

    if (delta < threshold)
        set_ppr_bounds(fwd_idx, check_rsum, real_num_rand_walk, v2ppr, pfail, zero_ppr_upper_bound,
                       upper_bounds, lower_bounds);
    else
        zero_ppr_upper_bound = calculate_lambda(check_rsum, pfail, zero_ppr_upper_bound, real_num_rand_walk);
}

void set_ppr_bounds(pair<iMap<double>, iMap<double>> &fwd_idx, double rsum,
                    long total_rw_num, unordered_map<int, double> &v2ppr, double pfail, double &zero_ppr_upper_bound,
                    iMap<double> &upper_bounds, iMap<double> &lower_bounds)
{
    double min_ppr = 1.0 / getVertNum();
    double sqrt_min_ppr = sqrt(1.0 / getVertNum());

    double epsilon_v_div = sqrt(2.67 * rsum * log(2.0 / pfail) / total_rw_num);
    double default_epsilon_v = epsilon_v_div / sqrt_min_ppr;

    int nodeid;
    double ub_eps_a;
    double lb_eps_a;
    double ub_eps_v;
    double lb_eps_v;
    double up_bound;
    double low_bound;
    zero_ppr_upper_bound = calculate_lambda(rsum, pfail, zero_ppr_upper_bound, total_rw_num);

    for (auto it = v2ppr.begin(); it != v2ppr.end(); ++it)
    {
        nodeid = it->first;
        if (v2ppr[nodeid] <= 0)
            continue;
        double reserve = 0.0;
        if (fwd_idx.first.exist(nodeid))
            reserve = fwd_idx.first[nodeid];
        double epsilon_a = 1.0;
        if (upper_bounds.exist(nodeid))
        {
            assert(upper_bounds[nodeid] > 0.0);
            if (upper_bounds[nodeid] > reserve)
                epsilon_a = calculate_lambda(rsum, pfail, upper_bounds[nodeid] - reserve, total_rw_num);
            else
                epsilon_a = calculate_lambda(rsum, pfail, 1 - reserve, total_rw_num);
        }
        else
            epsilon_a = calculate_lambda(rsum, pfail, 1.0 - reserve, total_rw_num);

        ub_eps_a = v2ppr[nodeid] + epsilon_a;
        lb_eps_a = v2ppr[nodeid] - epsilon_a;
        if (!(lb_eps_a > 0))
            lb_eps_a = 0;

        double epsilon_v = default_epsilon_v;
        if (fwd_idx.first.exist(nodeid) && fwd_idx.first[nodeid] > min_ppr)
        {
            if (lower_bounds.exist(nodeid))
                reserve = max(reserve, lower_bounds[nodeid]);
            epsilon_v = epsilon_v_div / sqrt(reserve);
        }
        else
        {
            if (lower_bounds[nodeid] > 0)
                epsilon_v = epsilon_v_div / sqrt(lower_bounds[nodeid]);
        }

        ub_eps_v = 1.0;
        lb_eps_v = 0.0;
        if (1.0 - epsilon_v > 0)
        {
            ub_eps_v = v2ppr[nodeid] / (1.0 - epsilon_v);
            lb_eps_v = v2ppr[nodeid] / (1.0 + epsilon_v);
        }

        up_bound = min(min(ub_eps_a, ub_eps_v), 1.0);
        low_bound = max(max(lb_eps_a, lb_eps_v), reserve);
        if (up_bound > 0)
        {
            if (!upper_bounds.exist(nodeid))
                upper_bounds.insert(nodeid, up_bound);
            else
                upper_bounds[nodeid] = up_bound;
        }

        if (low_bound >= 0)
        {
            if (!lower_bounds.exist(nodeid))
                lower_bounds.insert(nodeid, low_bound);
            else
                lower_bounds[nodeid] = low_bound;
        }
    }
}

inline double calculate_lambda(double rsum, double pfail, double upper_bound, long total_rw_num)
{
    return 1.0 / 3 * log(2 / pfail) * rsum / total_rw_num +
           sqrt(4.0 / 9.0 * log(2.0 / pfail) * log(2.0 / pfail) * rsum * rsum +
                8 * total_rw_num * log(2.0 / pfail) * rsum * upper_bound) /
               2.0 / total_rw_num;
}

inline int random_walk(int start, double alpha, const vector<int> &pred_set)
{
    int cur = start;
    unsigned long k;
    if (getSetOutSize(start, pred_set) == 0)
        return start;
    while (true)
    {
        if ((double)rand() / (double)RAND_MAX <= alpha) // drand, return bool, bernoulli by alpha
            return cur;
        if (getSetOutSize(cur, pred_set))
        {
            k = rand() % getSetOutSize(cur, pred_set); // lrand
            unsigned long curr_idx = k;
            for (int pred : pred_set)
            {
                int curr_out = getOutSize(cur, pred);
                if (curr_out <= curr_idx)
                    curr_idx -= curr_out;
                else
                {
                    cur = getOutVertID(cur, pred, curr_idx);
                    if (cur == -1)
                    {
                        cout << "ERROR1!!!!!!" << endl;
                        exit(0); // TODO: throw an exception
                    }
                    break;
                }
            }
        }
        else
            cur = start;
    }
}

double kth_ppr(unordered_map<int, double> &v2ppr, int retNum)
{
    static vector<double> temp_ppr;
    temp_ppr.clear();
    temp_ppr.resize(v2ppr.size());
    int i = 0;
    for (auto it = v2ppr.begin(); it != v2ppr.end(); ++it)
    {
        temp_ppr[i] = v2ppr[it->second];
        i++;
    }
    nth_element(temp_ppr.begin(), temp_ppr.begin() + retNum - 1, temp_ppr.end(),
                [](double x, double y)
                { return x > y; });
    return temp_ppr[retNum - 1];
}

bool if_stop(int retNum, double delta, double threshold, double epsilon, iMap<int> &topk_filter,
             iMap<double> &upper_bounds, iMap<double> &lower_bounds, unordered_map<int, double> &v2ppr)
{
    if (kth_ppr(v2ppr, retNum) >= 2.0 * delta)
        return true;

    if (delta >= threshold)
        return false;

    const static double error = 1.0 + epsilon;
    const static double error_2 = 1.0 + epsilon;

    vector<pair<int, double>> topk_pprs;
    topk_pprs.clear();
    topk_pprs.resize(retNum);
    topk_filter.clean();

    static vector<pair<int, double>> temp_bounds;
    temp_bounds.clear();
    temp_bounds.resize(lower_bounds.occur.m_num);
    int nodeid;
    for (int i = 0; i < lower_bounds.occur.m_num; i++)
    {
        nodeid = lower_bounds.occur[i];
        temp_bounds[i] = make_pair(nodeid, lower_bounds[nodeid]);
    }

    // sort topk nodes by lower bound
    partial_sort_copy(temp_bounds.begin(), temp_bounds.end(), topk_pprs.begin(), topk_pprs.end(),
                      [](pair<int, double> const &l, pair<int, double> const &r)
                      { return l.second > r.second; });

    // for topk nodes, upper-bound/low-bound <= 1+epsilon
    double ratio = 0.0;
    double largest_ratio = 0.0;
    for (auto &node : topk_pprs)
    {
        topk_filter.insert(node.first, 1);
        ratio = upper_bounds[node.first] / lower_bounds[node.first];
        if (ratio > largest_ratio)
            largest_ratio = ratio;
        if (ratio > error_2)
        {
            return false;
        }
    }

    // for remaining NO. retNum+1 to NO. n nodes, low-bound of retNum > the max upper-bound of remaining nodes
    double low_bound_k = topk_pprs[retNum - 1].second;
    if (low_bound_k <= delta)
        return false;

    for (int i = 0; i < upper_bounds.occur.m_num; i++)
    {
        nodeid = upper_bounds.occur[i];
        if (topk_filter.exist(nodeid) || v2ppr[nodeid] <= 0)
            continue;

        double upper_temp = upper_bounds[nodeid];
        double lower_temp = lower_bounds[nodeid];
        if (upper_temp > low_bound_k * error)
        {
            if (upper_temp > (1 + epsilon) / (1 - epsilon) * lower_temp)
                continue;
            else
                return false;
        }
        else
            continue;
    }

    return true;
}

int main(int argc, char const *argv[])
{
    if (argc < 4)
    {
        cout << "============================================================" << endl;
        cout << "Compute the top-k Single Source Personal Page Rank from u, the last param 'k' is the number of top nodes to return. The way to use this program: " << endl;
        cout << "./PEG_SSPPR database_name u hop [prop_set] k" << endl;
        cout << "============================================================" << endl;

        return 0;
    }

    string serversListStr = readFile("conf/servers.json");
    // crow::json::rvalue confJson = crow::json::load(serversListStr);
    confJson = crow::json::load(serversListStr);

    // vector<GstoreConnector *> servers;
    for (int i = 0; i < confJson["sites"].size(); i++)
    {
        servers.push_back(new GstoreConnector(
            confJson["sites"][i]["ip"].s(),
            (int)confJson["sites"][i]["port"],
            confJson["sites"][i]["http_type"].s(),
            confJson["sites"][i]["dbuser"].s(),
            confJson["sites"][i]["dbpasswd"].s()));
    }

    dbname=argv[1];
    string u(argv[2]);
    string hop(argv[3]);
    string k(argv[argc - 1]);
    vector<int> pred_set;
    vector<pair<int, double>> pprRes;

    // for (int i = 4; i < argc; i++)
    // {
    //     pred_set.push_back(getPropertyId(argv[i]));
    // }
    
    init(dbname);
    for(int i=0;i<property2id.size();i++) {
        pred_set.push_back(i);
    }
    cout<<"initial success"<<endl;
    long begin = Util::get_cur_time();
    // cout<<entity2id.size()<<property2id.size()<<endl;
    // cout<<getEntityId(u)
    //     <<getEntityStr(getEntityId(u))
    //     <<getPropertyId("<http://dbpedia.org/property/sisterStations>")
    //     <<getPropertyStr(getPropertyId("<http://dbpedia.org/property/sisterStations>"))
    //     <<getSetEdgeNum({})
    //     <<getVertNum()
    //     <<getOutSize(getEntityId(u),{})
    //     <<endl;

    SSPPR(getEntityId(u), stoi(k), stoi(hop), pred_set, pprRes);

    for(auto i: pprRes) {
        cout<<getEntityStr(i.first)<<' '<<i.second<<endl;
    }

    long end = Util::get_cur_time();
    cout << "Query Time is : " << end - begin << " ms." << endl;

    for (int i = 0; i < confJson["sites"].size(); i++)
        delete servers[i];

    return 0;
}

// nohup ./build/PEG_SSPPR dbpedia \<http://dbpedia.org/resource/WTMA\> 5 5 >dbpprres &