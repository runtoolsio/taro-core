#!/usr/bin/env bash

while true;
do
 date=$(date +%N) ;
 echo "$date stdout count=[$((RANDOM % 100 + 1))]"
 echo "$date stderr" >&2;
 sleep  1 ;
done
