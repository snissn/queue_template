set -x

workername=$1

#current directory
cpu_cores=$(cat /proc/cpuinfo |grep processor -c)
cpu_cores=$(($cpu_cores * 8));
cpu_cores=$(( 8  >  $cpu_cores ? $cpu_cores   : 8 ))
cpu_cores=64 # parallel processes

while true; do 
  python ../build_queue/clear.py $workername
  python ../build_queue/build_queue.py $workername
  seq 1 $cpu_cores | xargs -I{} -n1 -P $cpu_cores python worker.py $workername
done
