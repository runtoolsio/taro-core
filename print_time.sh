#!/usr/bin/env bash

while true;
do
 date=$(date +%N) ;
 echo "$date stdout";
 echo "$date stderr" >&2;
 sleep  1 ;
done
