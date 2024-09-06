#!/bin/bash
LOOPS=0
TIMEOUT=10  # Timeout in seconds
INTERVAL=1 # Check interval in seconds

#start_time=$(date +%s)

NUM_REQUESTS=$1
ADDRESS_RANGE=$2
var2="xxxxxxx"
TEST_FILE_NAME="test_defag"
# add start time suffix to TEST_FILE_NAME, print in this format: defrag_test_2020-05-04_12-34-56
timestamp=$(date +'%Y-%m-%d_%H-%M-%S')
filename="defrag_test_${timestamp}.log"
touch "$filename"

echo $(date +%Y-%m-%d_%H-%M-%S)
./src/valkey-cli config set maxmemory $3
./src/valkey-cli config get "active-d*" >> "$filename"
#./src/valkey-cli config get "maxmem*" >> "$filename"

run_load_phase () {
  start_cmd="$(date +%s)"
  prev_hits=$(./src/valkey-cli info all | grep "active_defrag_hits" | awk -F: '{print $2}')
  prev_miss=$(./src/valkey-cli info all | grep "active_defrag_misses" | awk -F: '{print $2}')
  ./src/valkey-benchmark -r ${ADDRESS_RANGE} -n ${NUM_REQUESTS} -c 20 -q -P 5 set key__rand_int__ ${var2}__rand_int__ EX $1
  frag_ratio=$(./src/valkey-cli info all | grep "mem_fragmentation_ratio" | awk -F: '{print $2}')
  hits=$(./src/valkey-cli info all | grep "active_defrag_hits" | awk -F: '{print $2}')
  defrag_hits=$(awk 'BEGIN {print ("'$hits'" - "'$prev_hits'")}')
  misses=$(./src/valkey-cli info all | grep "active_defrag_misses" | awk -F: '{print $2}')
  defrag_misses=$(awk 'BEGIN {print ("'$misses'" - "'$prev_miss'")}')
  total=$(awk 'BEGIN {print ("'$defrag_hits'" + "'$defrag_misses'")}')
  if [ $(awk 'BEGIN {print ("'$total'" > 0)}') -eq 1 ]; then
    hit_ratio=$(awk 'BEGIN {print ("'$defrag_hits'" / "'$total'")}')
    echo "[duration:$(($(date +%s) - start_cmd))]:[total:$total,defrag_hit:$hit_ratio]load set ${var2}__rand_int__ EX $1" >> "$filename"
    echo "[duration:$(($(date +%s) - start_cmd))]:[total:$total,defrag_hit:$hit_ratio]load set ${var2}__rand_int__ EX $1"
  fi
  ./src/valkey-cli info all | grep "mem_fragmentation_ratio\|used_memory_human\|defrag_time\|active_defrag\|expire\|evict\|latency_percentiles_usec_set\|cmdstat_set\|db0:keys=\|ncalls_util" >> "$filename"
}

run_workload () {
  defrag_hits=$(./src/valkey-cli info all | grep "active_defrag_hits" | awk -F: '{print $2}')
  defrag_misses=$(./src/valkey-cli info all | grep "active_defrag_misses" | awk -F: '{print $2}')
  defrag_total=0
  defrag_total_hits=0
  defrag_total_misses=0
  echo "$(date +%s):FLUSHALL SYNC - start" >> "$filename"
  ./src/valkey-cli FLUSHALL SYNC
  echo "$(date +%s):FLUSHALL SYNC - end" >> "$filename"
  start="$(date +%s)"
#  echo "$(date):Start $LOOPS: NEW DEFRAG=$(./src/valkey-cli config get newdefrag)" >> "$filename"
  ./src/valkey-cli info all | grep "mem_fragmentation_ratio\|used_memory_human\|defrag_time\|active_defrag\|expire\|evict\|latency_percentiles_usec_set\|cmdstat_set\|db0:keys=\|ncalls_util" >> "$filename"
  var2="xxxxxxx"
  run_load_phase 30
  var2="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  run_load_phase 50
  var2="xxxxxxxxxxxxxx"
  run_load_phase 60
  var2="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  run_load_phase 60
  var2="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  run_load_phase 120
  echo "[$LOOPS][$(./src/valkey-cli config get newdefrag|grep "yes\|no")]:[duration:$(($(date +%s) - start))]:completed for config: frag=$(./src/valkey-cli info all | grep "mem_fragmentation_ratio")" >> "$filename"
  ./src/valkey-cli info all | grep "mem_fragmentation_ratio\|used_memory_human\|defrag_time\|active_defrag\|expire\|evict\|latency_percentiles_usec_set\|cmdstat_set\|db0:keys=\|ncalls_util" >> "$filename"
  start_defrag="$(date +%s)"
  ./src/valkey-cli config set active-defrag-cycle-max 99
  ./src/valkey-cli config set active-defrag-max-scan-fields 1000000
  defrag_hits=$(./src/valkey-cli info all | grep "active_defrag_hits" | awk -F: '{print $2}')
  defrag_misses=$(./src/valkey-cli info all | grep "active_defrag_misses" | awk -F: '{print $2}')
  while true; do
      frag_ratio=$(./src/valkey-cli info all | grep "mem_fragmentation_ratio" | awk -F: '{print $2}')
      defrag_hits_accum=$(./src/valkey-cli info all | grep "active_defrag_hits" | awk -F: '{print $2}')
      curr_defrag_hits=$(awk 'BEGIN {print ("'$defrag_hits_accum'" - "'$defrag_hits'")}')
      defrag_hits=$defrag_hits_accum

      defrag_miss_accum=$(./src/valkey-cli info all | grep "active_defrag_misses" | awk -F: '{print $2}')
      curr_defrag_misses=$(awk 'BEGIN {print ("'$defrag_miss_accum'" - "'$defrag_misses'")}')
      defrag_misses=$defrag_miss_accum

      curr_total=$(awk 'BEGIN {print ("'$curr_defrag_hits'" + "'$curr_defrag_misses'")}')
      if [ $(awk 'BEGIN {print ("'$curr_total'" > 0)}') -eq 1 ]; then  # Compare as floating-point
          curr_hit_ratio=$(awk 'BEGIN {print ("'$curr_defrag_hits'" / "'$curr_total'")}')
      fi
      # Check if fragmentation ratio is below threshold or timeout is reached
      if [ $(awk 'BEGIN {print ("'$frag_ratio'" <= 1.1)}') -eq 1 ]; then  # Compare as floating-point
          echo "Fragmentation ratio reached acceptable level: mem_fragmentation_ratio:$frag_ratio,hit_ratio:$curr_hit_ratio,hits:$curr_defrag_hits,miss:$curr_defrag_misses"
          break  # Exit the loop
      elif (( $(date +%s) - start_defrag > TIMEOUT )); then
          echo "Timeout reached. Current fragmentation ratio: mem_fragmentation_ratio:$frag_ratio,hit_ratio:$curr_hit_ratio,hits:$curr_defrag_hits,miss:$curr_defrag_misses"
          break
      fi
      echo "mem_fragmentation_ratio:$frag_ratio,hit_ratio:$curr_hit_ratio,hits:$curr_defrag_hits,miss:$curr_defrag_misses"
      sleep $INTERVAL  # Wait for the specified interval
  done
  echo "[$LOOPS][$(./src/valkey-cli config get newdefrag|grep "yes\|no")][$(./src/valkey-cli config get active-defrag-strategy|grep "none\|odd\|even\|linear\|page")]:[duration:$(($(date +%s) - start_defrag))]:completed for config: frag=$(./src/valkey-cli info all | grep "mem_fragmentation_ratio")" >> "$filename"
  ./src/valkey-cli config set active-defrag-cycle-max 25
  ./src/valkey-cli config set active-defrag-max-scan-fields 1000
  ./src/valkey-cli info all | grep "mem_fragmentation_ratio\|used_memory_human\|defrag_time\|active_defrag\|expire\|evict\|latency_percentiles_usec_set\|cmdstat_set\|db0:keys=\|ncalls_util" >> "$filename"
  echo "$(date +%s):FLUSHALL SYNC - start" >> "$filename"
  ./src/valkey-cli FLUSHALL SYNC
  echo "$(date +%s):FLUSHALL SYNC - end" >> "$filename"
}

while true
do
    ./src/valkey-cli config set active-defrag-cycle-max 25
    echo "$(date):START Loop $LOOPS: =====================================================================================" >> "$filename"
    ./src/valkey-cli config set newdefrag "no" >> "$filename"
    run_workload $1 $2 $3
    ./src/valkey-cli config set active-defrag-strategy "linear"
    ./src/valkey-cli config set newdefrag "yes" >> "$filename"
    run_workload $1 $2 $3
    ./src/valkey-cli config set active-defrag-strategy "odd"
    ./src/valkey-cli config set newdefrag "yes" >> "$filename"
    run_workload $1 $2 $3
    ./src/valkey-cli config set active-defrag-strategy "even"
    ./src/valkey-cli config set newdefrag "yes" >> "$filename"
    run_workload $1 $2 $3
    ./src/valkey-cli config set active-defrag-strategy "page"
    ./src/valkey-cli config set newdefrag "yes" >> "$filename"
    run_workload $1 $2 $3
    ./src/valkey-cli config set active-defrag-strategy "none"
    ./src/valkey-cli config set newdefrag "yes" >> "$filename"
    run_workload $1 $2 $3
    echo "$(date):END Loop $LOOPS: =====================================================================================" >> "$filename"
    #increment loop counter
    LOOPS=$((LOOPS+1))
done