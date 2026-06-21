@echo off
docker exec -it instacart-namenode hdfs dfs -chmod 777 /
echo HDFS permissions set!