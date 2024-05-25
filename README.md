## 环境

| 库  | 版本|
| --- | --- |
| g++ | 8.4.1, -std=c++17|
| boost | 1.68 |
| curl | 7.61.1 |
| metis | 5.1.0 |
| antlr | 3.4 |

> 项目中已安装metis可执行文件和antlr库，若无法使用，请自行安装

## 编译项目

新建文件夹`build`，然后进入此文件夹，使用cmake进行编译。

```bash
mkdir build
cd build
cmake ..
make
```

## 配置服务器

编辑`conf/servers.json`

```json
{
    "sites": [
        {
            "ip": "site's ipv4",
            "user": "Linux user name",
            "port": "gStore http port",
            "http_type": "ghttp",
            "dbuser": "root",
            "dbpasswd": "123456",
            "rootPath": "/opt/gStore"
        }
    ]
}
```

在`sites`中，添加节点对象：
- `ip`字段表示节点的ip
- `user`字段表示Linux系统用户
- `port`字段表示gStore http服务端口
- `http_type`字段表示http服务类型
- `dbuser`字段表示gStore的用户名
- `dbpasswd`字段表示gStore用户的密码
- `rootPath`字段表示gStore在节点中的部署路径

> 注意：PEG需要免密登入以上配置的节点，并且启动gStore http服务，才能保证程序正常运行

## 运行

在项目根目录

```bash
# 启动web服务
[root@localhost gStoreDemo]$ ./build/PEG_Server

# 命令行新建数据库
[root@localhost gStoreDemo]$ ./build/PEG_Load 数据库名 nt文件路径 分片文件路径
./build/PEG_Load watdiv /root/projects/PEG/watdiv100K.nt /root/projects/PEG/dataInternalPoints.txt
./build/PEG_Load watdiv /root/projects/PEG/watdiv100K.nt /root/projects/PEG/watdiv100InternalPoints.txt

# 命令行查询
[root@localhost gStoreDemo]$ ./build/PEG_Query 数据库名 SPARQL文件路径

# 命令行删除数据库
[root@localhost gStoreDemo]$ ./build/PEG_Delete 数据库名
```

