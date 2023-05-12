#!/bin/bash

go build -race -buildmode=plugin ./mrapps/wc.go

rm ./main/mr-out*

go run -race ./main/mrcoordinator.go ./main/pg-*.txt
