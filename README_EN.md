## Deploy the front end

1. Clone the front-end file from github and open it through vscode or webStorm.
2. Cross-domain development environment: modify the **target** in the proxy in the **vue.config.js** file to modify the server address and port of the back-end deployment.
3. Run **npm run build:prod** to package the files into the **dist** folder.
4. Put the folder in any location on the server.
5. Cross-domain deployment environment: Use **nginx** reverse proxy to complete cross-domain.
6. Modify the root in **location /** to the path where **dist** is located.
7. Modify the proxy_pass in **location /api/** and modify it to the server address and port of the back-end deployment.
8. Just visit the deployed address and port number

## Deploy the back end

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

Create a new folder called `build`, then get into this folder and use `cmake` to compile.

```bash
mkdir build
cd build
cmake ..
make
```

### Configure the servers

Edit `conf/servers.json`

```json
{
    "sites": [
        {
            "ip": "site's ipv4",
            "user": "Linux user name",
            "port": "gStore http port",
            "dbuser": "root",
            "dbpasswd": "123456",
            "rootPath": "/opt/gStore"
        }
    ]
}
```

In `Sites`, add site objects:：
- The `IP` field indicates the IP address of the site
- The `user` field indicates the Linux system user 
- The `port` field indicates the port of gStore http service 
- The `dbuser` field indicates the username of gStore 
- The `dbpasswd` field indicates the password of the gStore user 
- The `rootPath` field indicates the path of gStore deployed in the site 

> Note: To ensure the normal running of the program, PEG needs to log in to the site configured above without password and the gStore http service has been started.  
> To start gStore http service, firstly get into gStore root path, then run `./bin/ghttp port_num`.

### How to run

In the project directory:

```bash
# Start web service 
[root@localhost gStoreDemo]$ ./build/PEG_Server

# Create a new database from the command line 
[root@localhost gStoreDemo]$ ./build/PEG_Load db_name /path/to/nt/file

# Query from the command line 
[root@localhost gStoreDemo]$ ./build/PEG_Query db_name /path/to/SPARQL/file

# Delete the database from the command line 
[root@localhost gStoreDemo]$ ./build/PEG_Delete db_name
```

