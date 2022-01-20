#!/bin/sh
rm data/*
rm check.md5
for i in `seq 0 999`
do
    cat /dev/urandom | head -c 102400 > data/data$i
done
cd data
md5sum $(find . -type f) | tee ../check.md5
cd ..

HOST=192.168.3.8
USER=group5
DIR=~/robust/
scp check.md5 ${USER}@${HOST}:${DIR}
