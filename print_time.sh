#!/usr/bin/env bash

while true;
do
 date=$(date +%N) ;
 echo "$date stdout completed=[$((RANDOM % 100 + 1))]"
 echo "$date stderr" >&2;
 sleep  $((RANDOM % 5)) ;
done
