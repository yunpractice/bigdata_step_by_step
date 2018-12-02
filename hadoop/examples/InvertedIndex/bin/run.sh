#!/bin/bash

hadoop fs -rm /inverted_index_out/*
hadoop fs -rmdir /inverted_index_out

hadoop jar InvertedIndex.jar InvertedIndex /inverted_index/* /inverted_index_out
