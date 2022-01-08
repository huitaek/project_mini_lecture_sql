#!/bin/bash
input=$1

if [ $input = "up" ]
then
    docker-compose up -d
elif [ $input = "down" ]
then
    docker-compose down -v
elif [ $input = "reset" ]
then
    docker-compose down -v
    wait
    docker-compose up -d
else
    echo "No Command {$input}"
fi