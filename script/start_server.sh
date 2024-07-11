#!/bin/bash

# 定义日志文件路径
LOGFILE=server.log

# 使用 nohup 启动 PEG_Server，并将输出重定向到日志文件
nohup ./build/PEG_Server > $LOGFILE 2>&1 &

# 获取服务器的进程 ID 并存储到文件中，以便关闭时使用
echo $! > server.pid

echo "PEG_Server 已启动，进程 ID: $(cat server.pid)，日志文件: $LOGFILE"

