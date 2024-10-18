# PEG

## Deploy PEG

### Requirements

| Library | Version |
| --- | --- |
| g++ | 8.4.1, -std=c++17|
| boost | 1.68 |
| curl | 7.61.1 |
| metis | 5.1.0 |
| antlr | 3.4 |

> The metis executable and antlr library have been installed in the project. If you cannot use them, please install them yourself.

### Compile the project
Here we use scripts to install environment and compile 
```
# install env
bash scripts/setup.sh
# compile
bash scripts/build.sh
```
#### Manually compile
This way is included in `scripts/build.sh`,if you run `scripts/build.sh`, there is no need to run Manually compile.
Here Create a new folder called `build`, then get into this folder and use `cmake` to compile manually.

```bash
mkdir build
cd build
cmake ..
make
```

### Configure the gstore sites
We use gstores as slave sites.As PEG is distributed system,you are supposed to deploy mutiple gstores sites in advance.
As example, we config three gstore sites in `conf/servers.json`, which are at `port 21000`、`port 21001` and `port 21002`.Note that before config you must start them in advance!
Edit `conf/servers.json`

```json
{
    "sites": [
        {
            "ip": "gstore severs's ipv4",
            "user": "the Linux system user",
            "port": "gStore http port",
            "http_type": "ghttp",
            "dbuser": "root",
            "dbpasswd": "123456",
            "rootPath": "/opt/gStore"
        }
    ]
}
```

In `Sites`, add site objects:：
- The `IP` field indicates the IP address of the gstore servers
- The `user` field indicates the Linux system user 
- The `port` field indicates the port of gStore http service 
- The `http_type` field indicates the gstore http type, we use `ghttp` here
- The `dbuser` field indicates the username of gStore 
- The `dbpasswd` field indicates the password of the gStore user 
- The `rootPath` field indicates the path of gStore deployed in the site 

> Note: To ensure the normal running of the program, `PEG needs to log in to the site configured above without password` and `the gStore http service has been started`. Note that the version of gStore must be bigger than 1.0.
> To start gStore http service, firstly get into gStore root path, then run `nohup bin/ghttp -p port_num &`.

### How to run

In the project directory:

```bash
# Start web service 
[root@localhost PEG]$ ./build/PEG_Server
# `http://localhost:18081/static/index.html` is the website 

# Create a new database from the command line 
[root@localhost PEG]$ ./build/PEG_Load db_name /path/to/nt/file /path/to/dividefile
./build/PEG_Load watdiv data/watdiv100K.nt data/watdiv100K-3.txt

# Query from the command line 
[root@localhost PEG]$ ./build/PEG_Query db_name /path/to/SPARQL/file

# Delete the database from the command line 
[root@localhost PEG]$ ./build/PEG_Delete db_name

# AddEdge
[root@localhost PEG]$ ./build/PEG_AddEdge database_name fromVertex perdicate toVertex

# RemoveEdge
[root@localhost PEG]$ ./build/PEG_RemoveEdge database_name fromVertex perdicate toVertex

# Delete the database from the command line 
[root@localhost PEG]$ ./build/PEG_Delete db_name
```
> when we use `Load`, we need to use the dividefile which maps points to its partition part number, here we provide a divide solution minimal property cut which is in https://github.com/bnu05pp/mpc, the generated `XXInternalPoints.txt` file is the dividefile
### Algorithm
> to improve the efficiency, we use the `XXInternalPoints.txt` to map vertexs' partition number,please put it in the `entity/` folder, which XX must be the same as db_name.
```
# khop
./build/PEG_Khop database_name <begin, end, propset, k>

# single source shortest path
./build/PEG_ShortestPath database_name <begin, end>

# clossness centrality
./build/PEG_ClosenessCentrality database_name vertex_name

# triangle count
./build/PEG_Triangle2 database_name /path/to/your/triangle/file
# ./data/triangle.q is an example of /path/to/your/triangle/file

# query neighbor
./build/PEG_Neighbor database_name in_out(true/false) vertex_name [pred_name] 
# true indicates forward neighbor,false indicates backword neighbor
```

### Service
```
# start server，it will start `PEG_Server` at port 18081 
[root@localhost PEG]$ ./scripts/start_server.sh
# now you could look through the website `http://localhost:18081/static/index.html`
# stop server
[root@localhost PEG]$ ./scripts/stop_server.sh
# send get request to port 18081,here we check version
[root@localhost PEG]$ curl "http://localhost:18081/api/version"
```

## Deploy the PEG-ui
[PEG-ui](https://github.com/15197580192/PEG-ui.git) is the address of front-end codes of PEG
1. Clone the front-end file from github and open it through vscode or webStorm.
2. Cross-domain development environment: modify the **target** in the proxy in the **vue.config.js** file to modify the server address and port of the back-end deployment.
3. Run **npm run build:prod** to package the files into the **dist** folder.
4. Put the folder in any location on the server. 
5. Cross-domain deployment environment: Use **nginx** reverse proxy to complete cross-domain.
6. Modify the root in **location /** to the path where **dist** is located.
7. Modify the proxy_pass in **location /api/** and modify it to the server address and port of the back-end deployment.
8. Just visit the deployed address and port number

### Another way: no cross-domain
1. Clone the front-end code from github and open it through vscode or webStorm.
2. Run **npm run build:prod** to package the files into the **dist** folder.
3. Replace all files in PEG folder **static** with the files in **dist** , than run PEG_Server and you could look through `http://PEG_server_ip:18081/static/index.html`.
