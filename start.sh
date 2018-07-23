#!/bin/bash
LOG_DATE=$(date -d yesterday +"%Y-%m-%d")
c_n=`ps aux|grep 'web_until.py'|wc -l`

if [ $c_n -eq 1 ]; then
    echo "start program"
    cd /home/pom/yuqing/code/web/mark
    /home/pom/anaconda3/bin/python35 ./web_until.py >> ./log/${LOG_DATE}.log 2>&1 &
    exit 0
fi
    echo "program runing"
    exit 0
