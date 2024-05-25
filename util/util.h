#ifndef UTIL_H
#define UTIL_H

#include <string>
#include <fstream>

inline std::string readFile(std::string filename)
{
    std::cout << "Opening " << filename << " to read." << std::endl;
    std::ifstream ifs;
    ifs.open(filename, ios::in);
    std::string sparql;
    std::string s;
    if (!ifs.is_open())
    {
        throw runtime_error("file opened failed");
    }
    while (!ifs.eof())
    {
        getline(ifs, s);
        sparql += s;
        sparql += "\n";
    }
    ifs.close();
    return sparql;
}

inline void writeFile(std::string filename, const std::string &&content)
{
    std::cout << "Opening " << filename << " to write." << std::endl;
    std::ofstream ofs;
    ofs.open(filename, ios::out);
    ofs << content;
    ofs.close();
}

#endif