#!/bin/bash

CONTAINER_ID=$1
FILENAME=$2

echo -ne "GET /containers/$CONTAINER_ID/stats HTTP/1.1\r\nHost: woof\r\n\r\n" | sudo nc -U /var/run/docker.sock | grep "^{" > $FILENAME
