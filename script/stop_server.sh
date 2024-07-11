#!/bin/bash

# 检查 server.pid 文件是否存在
if [ -f server.pid ]; then
  # 获取进程 ID 并关闭进程
  PID=$(cat server.pid)
  kill $PID
  
  # 移除 server.pid 文件
  rm server.pid

  echo "PEG_Server 已关闭，进程 ID: $PID"
else
  echo "找不到 server.pid 文件。PEG_Server 可能没有运行。"
fi

