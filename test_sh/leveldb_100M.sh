#/bin/bash

workload="workloads/test_workloada.spec"
dbpath="/mnt/pmem1/ceshi"
moreworkloads="workloads/test_workloada.spec:workloads/test_workloadb.spec:workloads/test_workloadc.spec:workloads/test_workloadd.spec:workloads/test_workloadf.spec:workloads/test_workloade.spec"



cmd="./ycsbc -db leveldb -dbpath $dbpath -threads 1 -P $workload -load true -morerun $moreworkloads -dbstatistics true >>out.out 2>&1 "

if [ -n "$1" ];then    #后台运行
cmd="nohup ./ycsbc -db leveldb -dbpath $dbpath -threads 1 -P $workload -load true -morerun $moreworkloads -dbstatistics true >>out.out 2>&1  &"
echo $cmd >out.out
fi


echo $cmd
eval $cmd