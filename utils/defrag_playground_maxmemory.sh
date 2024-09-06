#!/bin/bash

# Check if arguments at least 3 arguments provided
# If not, print usage and exit
if  [ $# -lt 3 ]; then
  echo "Usage: $0 <min_used_memory> <valkey-server> <config-matrix>"
  exit 1
fi
MIN_USED_MEMORY=$1
if [ -n "$2" ]; then
  SERVER="valkey-server-$2"
else
  SERVER="valkey-server"
fi
LOOPS=0
DEBUG=0
CONFIG_MATRIX=$3

get_info() {
  defrag_enabled=$(./valkey-cli config get activedefrag|grep "yes\|no")
  memory_info=$(./valkey-cli info all)
  active_defrag_misses=$(echo "$memory_info" | awk -F':' '/active_defrag_misses:/ {print $2}' | tr -d '[:space:]')
  active_defrag_hits=$(echo "$memory_info" | awk -F':' '/active_defrag_hits:/ {print $2}' | tr -d '[:space:]')
  if [ "$((active_defrag_hits - active_defrag_hits_prev + active_defrag_misses - active_defrag_misses_prev))" -gt 0 ]; then
    hits=$((active_defrag_hits - active_defrag_hits_prev))
    misses=$((active_defrag_misses - active_defrag_misses_prev))
    hit_ratio=$((100*hits / (hits + misses)))
    echo "hit:$hit_ratio%"
  else
    hit_ratio=0
  fi

  maxmemory=$(echo "$memory_info" | awk -F':' '/maxmemory:/ {print $2}' | tr -d '[:space:]')
  used=$(echo "$memory_info" | awk -F':' '/used_memory:/ {print $2}' | tr -d '[:space:]')
  fragmentation_ratio=$(echo "$memory_info" | awk -F':' '/allocator_frag_ratio:/ {print $2}' | tr -d '[:space:]')
  echo "[${LINENO}][$LOOPS][$2][$(($(date +%s) - start_t))][$1][$defrag_enabled][ratio:$hit_ratio%:hit:$((hits/1000))K:miss:$((misses/1000))K]::fragmentation_ratio:$fragmentation_ratio,maxmemory:$((maxmemory/1000000))MB,used_memory:$((used/1000000))MB" >> "$filename"
  echo "[${LINENO}][$LOOPS][$2][$(($(date +%s) - start_t))][$1][$defrag_enabled][ratio:$hit_ratio%:hit:$((hits/1000))K:miss:$((misses/1000))K]::fragmentation_ratio:$fragmentation_ratio,maxmemory:$((maxmemory/1000000))MB,used_memory:$((used/1000000))MB"
  active_defrag_misses_prev=$active_defrag_misses
  active_defrag_hits_prev=$active_defrag_hits
}

kill_loader() {
  kill -9 $(pgrep valkey-be)
  sleep 1
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):killed loaders: $(ps -ef | grep valkey)" >> "$filename"
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):killed loaders: $(ps -ef | grep valkey)"
}

kill_server() {
  kill -9 $(pgrep valkey)
  sleep 1
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):killed server: $(ps -ef | grep valkey)" >> "$filename"
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):killed server: $(ps -ef | grep valkey)"
}

launch_server() {
  kill_server
  sleep 1
  kill_server
  sleep 1
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):BEFORE server: $(ps -ef | grep valkey)"
  # Start the server if provided
  if [ -n "$1" ]; then
    nohup ./$1 --io-threads 10 --activedefrag yes --maxmemory $2 --maxmemory-policy allkeys-lru --save ''&
#  else
#    nohup ./valkey-server --io-threads 10 --activedefrag yes --maxmemory ${MIN_USED_MEMORY} --maxmemory-policy allkeys-lru --save ''&
  fi
  echo "sleep 2"
  sleep 2
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):started server: $(ps -ef | grep valkey)" >> "$filename"
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):started server: $(ps -ef | grep valkey)"
}

launch_loader() {
  echo "launch_loader:${LINENO}:$@"
  kill_loader
  kill_loader
  sleep 5
  nohup ./src/valkey-benchmark -c 80 -r 2000000000 -t set -d 24 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 2000000000 -t set -d 128 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 1500000000 -t set -d 212 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 50000000 -t set -d 1234 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 20000000 -t set -d 234 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 2000000000 -t set -d 74 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 100000000 -t set -d 256 -q -l&
  nohup ./src/valkey-benchmark -c 80 -r 25000000 -t set -d 123 -q -l&
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):started loaders: $(ps -ef | grep valkey)" >> "$filename"
  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):started loaders: $(ps -ef | grep valkey)"
}

rotate_defrag_config() {
# print to screen all args $1 to $6
  echo "DEFRAG CONFIG:${LINENO}:$@" >> "$filename"
  echo "DEFRAG CONFIG:${LINENO}:$@"
  ./valkey-cli config set active-defrag-strategy $1 #je-hint
  ./valkey-cli config set active-defrag-recalc $2 #"full_cycle"
  ./valkey-cli config set active-defrag-select $3 #progressive
  ./valkey-cli config set active-defrag-nonfull-factor $4 #2999
  ./valkey-cli config set active-defrag-alloc $5 #none
  ./valkey-cli config set active-defrag-free $6 #none
  ./valkey-cli config set active-defrag-max-scan-fields $7
  ./valkey-cli config set active-defrag-threshold-lower $8
  ./valkey-cli config set active-defrag-ignore-bytes $9
  sleep 2
}

disable_defrag() {
  echo "Line number: ${LINENO}"
  ./valkey-cli config set maxmemory 100000000000
  ./valkey-cli config set maxmemory-policy allkeys-lru
  ./valkey-cli config set activedefrag "no"
  sleep 1
}

wait_memory_growth() {
  echo "${LINENO}:wait_memory_growth:$@"
  while true; do
    used_memory=$(./valkey-cli info memory | awk -F':' '/used_memory:/ {print $2}' | tr -d '[:space:]')

    # Ensure used_memory is a valid integer
    if [[ "$used_memory" =~ ^[0-9]+$ ]]; then
      # Check if used_memory is greater than the minimum threshold
      if [ "$used_memory" -gt "$1" ]; then
        echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):reached requested min used_memory:$((used_memory/1000000))MB"
        get_info "reached requested min used_memory" $LOOPS
        break
      else
        echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):didn't reach min used_memory:$((used_memory/1000000))MB"
#        get_info "wait_memory_growth" $LOOPS
      fi
    fi
    sleep 5
  done
}

wait_memory_shrink() {
  echo "${LINENO}:wait_memory_shrink:$(($1/1000000))"
  FRAG_LOOPS=0
  memory_info=$(./valkey-cli info memory)
  used_memory=$(echo "$memory_info" | awk -F':' '/used_memory:/ {print $2}' | tr -d '[:space:]')
#  echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):maxmemory_new:$(("$1"/1000000))MB"
  while [ "$used_memory" -ge "$1" ]; do
          get_info "wait_frag" $FRAG_LOOPS
    echo "[${LINENO}][$LOOPS][fragment]:$(($(date +%s) - frag_start)):used_memory:$((used_memory/1000000))MB)>maxmemory:$((maxmemory_new/1000000))MB. Sleeping..."
          sleep 1
          ((FRAG_LOOPS++))
          memory_info=$(./valkey-cli info memory)
          used_memory=$(echo "$memory_info" | awk -F':' '/used_memory:/ {print $2}' | tr -d '[:space:]')
        done
  memory_info=$(./valkey-cli info memory)
  used_memory=$(echo "$memory_info" | awk -F':' '/used_memory:/ {print $2}' | tr -d '[:space:]')
  echo "[${LINENO}][$LOOPS][fragment]:$(($(date +%s) - start_t)):Done:used_memory ($((used_memory/1000000))MB) is less than maxmemory ($((maxmemory_new/1000000))MB). After..."
}

start_defrag_wait_target_value() {
  echo "${LINENO}:start_defrag_wait_target_value:$@"
  ./valkey-cli config set activedefrag "yes"
  sleep 1
  echo "ACTIVE DEFRAG ENABLED:"
  echo "$(./valkey-cli config get active-defrag*)"
  pre_memory_info=$(./valkey-cli info all)
  pre_misses=$(echo "$pre_memory_info" | awk -F':' '/active_defrag_misses:/ {print $2}' | tr -d '[:space:]')
  pre_hits=$(echo "$pre_memory_info" | awk -F':' '/active_defrag_hits:/ {print $2}' | tr -d '[:space:]')
  pre_keys_misses=$(echo "$pre_memory_info" | awk -F':' '/active_defrag_key_misses:/ {print $2}' | tr -d '[:space:]')
  pre_keys_hits=$(echo "$pre_memory_info" | awk -F':' '/active_defrag_key_hits:/ {print $2}' | tr -d '[:space:]')
  pre_keys=$(echo "$pre_memory_info" | awk -F'=' '/db0:keys=/ {print $2}' | awk -F',' '{print $1}')
  pre_defrag_active_time=$(echo "$pre_memory_info" | awk -F':' '/current_active_defrag_time:/ {print $2}' | tr -d '[:space:]')
  pre_defrag_total_time=$(echo "$pre_memory_info" | awk -F':' '/total_active_defrag_time:/ {print $2}' | tr -d '[:space:]')

  defrag_start=$(date +%s)
  DEFRAGLOOPS=0
  # Ensure the fragmentation ratio is numeric and a valid float
  if [[ "$fragmentation_ratio" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    # Wait until fragmentation ratio is below the target value
    while (( $(echo "100 * $fragmentation_ratio > $1" | bc -l) )); do
      sleep 1
      memory_info=$(./valkey-cli info memory)
      fragmentation_ratio=$(echo "$memory_info" | awk -F':' '/allocator_frag_ratio:/ {print $2}' | tr -d '[:space:]')
      get_info "wait_defrag" $DEFRAGLOOPS
      # Print debug information
      echo "[${LINENO}][$LOOPS][defrag]:$(($(date +%s) - defrag_start)):fragmentation_ratio:$fragmentation_ratio"

      # Ensure the fragmentation ratio is numeric and a valid float
      if ! [[ "$fragmentation_ratio" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        break  # Exit the loop if the fragmentation_ratio is invalid
      fi
      ((DEFRAGLOOPS++))
    done
    get_info "done" $DEFRAGLOOPS
    after_memory_info=$(./valkey-cli info all)
    after_misses=$(echo "$after_memory_info" | awk -F':' '/active_defrag_misses:/ {print $2}' | tr -d '[:space:]')
    after_hits=$(echo "$after_memory_info" | awk -F':' '/active_defrag_hits:/ {print $2}' | tr -d '[:space:]')
    after_keys_misses=$(echo "$after_memory_info" | awk -F':' '/active_defrag_key_misses:/ {print $2}' | tr -d '[:space:]')
    after_keys_hits=$(echo "$after_memory_info" | awk -F':' '/active_defrag_key_hits:/ {print $2}' | tr -d '[:space:]')
    after_keys=$(echo "$after_memory_info" | awk -F'=' '/db0:keys=/ {print $2}' | awk -F',' '{print $1}')
    after_active_time=$(echo "$after_memory_info" | awk -F':' '/current_active_defrag_time:/ {print $2}' | tr -d '[:space:]')
    after_total_time=$(echo "$after_memory_info" | awk -F':' '/total_active_defrag_time:/ {print $2}' | tr -d '[:space:]')
    if [ "$((after_hits - pre_hits + after_misses - pre_misses))" -gt 0 ]; then
      curr_hits=$((after_hits - pre_hits))
      curr_misses=$((after_misses - pre_misses))
      curr_keys_hits=$((after_keys_hits - pre_keys_hits))
      curr_keys_misses=$((after_keys_misses - pre_keys_misses))
      key_space_proc_ratio_hits=$((100*curr_keys_hits / (after_keys)))
      key_space_proc_ratio_misses=$((100*curr_keys_misses / (after_keys)))
      hit_ratio=$((100*curr_hits / (curr_hits + curr_misses)))
      total_time=$((after_total_time - pre_defrag_total_time))
      echo "hit:$hit_ratio%,curr_hits:$curr_hits,curr_misses:$curr_misses,total_time:$total_time,after_active_time:$after_active_time" >> "$filename"
      echo "hit:$hit_ratio%,curr_hits:$curr_hits,curr_misses:$curr_misses,total_time:$total_time,after_active_time:$after_active_time"

    fi
    echo "[${LINENO}][$LOOPS][defrag]:$(($(date +%s) - start_t)):DONE:fragmentation_ratio:$fragmentation_ratio,hit_ratio:$hit_ratio,key_space_proc_ratio_hits:$key_space_proc_ratio_hits,key_space_proc_ratio_misses:$key_space_proc_ratio_misses"
    echo "[${LINENO}][$LOOPS][defrag]:$(($(date +%s) - start_t)):DONE:fragmentation_ratio:$fragmentation_ratio,hit_ratio:$hit_ratio,key_space_proc_ratio_hits:$key_space_proc_ratio_hits,key_space_proc_ratio_misses:$key_space_proc_ratio_misses" >> "$filename"
    echo "$after_memory_info" | grep "allocator_frag_ratio\|used_memory_human\|defrag_time\|active_defrag\|expire\|evict\|latency_percentiles_usec_set\|cmdstat_set\|db0:keys=\|ncalls_util" >> "$filename"
    echo "$after_memory_info" | grep "allocator_frag_ratio\|used_memory_human\|defrag_time\|active_defrag\|expire\|evict\|latency_percentiles_usec_set\|cmdstat_set\|db0:keys=\|ncalls_util"
  fi
}

# Function to generate all combinations of the provided configuration ranges
generate_combinations() {
    local input=$1
    local strategy_range
    local recalc_range
    local select_range
    local nonfull_factor_range
    local alloc_range
    local free_range
    local max_scan_fields_range
    local threshold_lower_range
    local ignore_bytes_range
    IFS=',' read -r strategy_range recalc_range select_range nonfull_factor_range alloc_range free_range max_scan_fields_range threshold_lower_range ignore_bytes_range <<< "$input"
    for strategy in ${strategy_range[*]}; do
        for recalc in ${recalc_range[@]}; do
            for select in ${select_range[@]}; do
                for nonfull_factor in ${nonfull_factor_range[@]}; do
                    for alloc in ${alloc_range[@]}; do
                        for free in ${free_range[@]}; do
                            for max_scan_fields in ${max_scan_fields_range[@]}; do
                                for threshold_lower in ${threshold_lower_range[@]}; do
                                  for ignore_bytes in ${ignore_bytes_range[@]}; do
                                      echo ""
                                      echo ""
                                      echo "============================================================================"
                                      echo "target frag:$threshold_lower memory usage $((MIN_USED_MEMORY/1000000))MB"
                                      echo "MAIN LOOP START" "$strategy" "$recalc" "$select" "$nonfull_factor" "$alloc" "$free" "$max_scan_fields" "$threshold_lower" "$ignore_bytes" >> "$filename"
                                      echo "MAIN LOOP START:" "$strategy" "$recalc" "$select" "$nonfull_factor" "$alloc" "$free" "$max_scan_fields" "$threshold_lower" "$ignore_bytes"
                                      if [ $(printf "%.0f" $(echo "$threshold_lower * $MIN_USED_MEMORY / 100 - $ignore_bytes - 1000000" | bc)) -lt 0 ]; then
                                        echo "Defrag might stop before target: $(printf "%.0f" $(echo "$threshold_lower * $MIN_USED_MEMORY /100 - $ignore_bytes - 1000000" | bc))"
                                        exit 1
                                        # handle the error
                                      fi
                                      launch_server "$SERVER" "$MIN_USED_MEMORY"
                                      start_t=$(date +%s)
                                      active_defrag_misses_prev=0
                                      active_defrag_hits_prev=0
                                      disable_defrag
#                                      get_info "after_flush" 0
                                      launch_loader
                                      wait_memory_growth $MIN_USED_MEMORY

                                      get_info "before_frag" 0
                                      ./valkey-cli config set active-defrag-cycle-max 99
                                      ./valkey-cli config set active-defrag-cycle-min 98
                                      #  based on value of $1 (numeric 0-4) choose the rotate_defrag_config
                                      # if statement to apply config based on value of $1
                                      rotate_defrag_config "$strategy" "$recalc" "$select" "$nonfull_factor" "$alloc" "$free" "$max_scan_fields" "$threshold_lower" "$ignore_bytes"
                                      frag_start=$(date +%s)
                                      # Ensure used_memory is a valid integer
                                      if [[ "$used_memory" =~ ^[0-9]+$ ]]; then
                                        # Check if used_memory is greater than 0
                                        if [ "$used_memory" -gt 0 ]; then
                                          # Calculate the new maxmemory value by right shifting (dividing by 2)
                                          maxmemory_new=$(($used_memory >> 1))

                                          # Print debug information
                                          echo "[${LINENO}][$LOOPS]:$(($(date +%s) - start_t)):maxmemory_new:$((maxmemory_new/1000000))MB"

                                          if [ "$maxmemory_new" -gt 0 ]; then
                                            # Configure Redis maxmemory
                                            ./valkey-cli config set maxmemory "$maxmemory_new"
                                            wait_memory_shrink $maxmemory_new
                                          fi
                                        fi
                                      fi
                                      get_info "before_defrag" 0
                                      kill_loader
                                      threshold_lower=$((threshold_lower+100))
                                      start_defrag_wait_target_value $((threshold_lower))
                                      kill_loader
                                  done
                                done
                            done
                        done
                    done
                done
            done
        done
    done
}

# Function to parse the input configuration string
parse_config() {
    local input=$CONFIG_MATRIX
    # Default values
    local strategy=("none")
    local recalc=("none")
    local select=("none")
    local nonfull_factor=0
    local alloc=("none")
    local free=("none")
    local max_scan_fields=(1000)
    local threshold_lower=(10)
    local ignore_bytes=(50000000)
    # Parse the input
    IFS=',' read -ra options <<< "$input"
    for option in "${options[@]}"; do
        IFS=':' read -r key value <<< "$option"
        key=$(echo "$key" | xargs)  # trim whitespace
        value=$(echo "$value" | xargs)  # trim whitespace
        IFS='|' read -ra value_array <<< "$value"  # split by comma and convert to array

        case "$key" in
            strategy)
                strategy=("${value_array[@]}")
                ;;
            recalc)
                recalc=("${value_array[@]}")
                ;;
            select)
                select=("${value_array[@]}")
                ;;
            nonfull_factor)
                nonfull_factor=("${value_array[@]}")
                ;;
            alloc)
                alloc=("${value_array[@]}")
                ;;
            free)
                free=("${value_array[@]}")
                ;;
            max_scan_fields)
                max_scan_fields=("${value_array[@]}")
                ;;
            threshold_lower)
                threshold_lower=("${value_array[@]}")
                ;;
            ignore_bytes)
                ignore_bytes=("${value_array[@]}")
                ;;
        esac
    done
    generate_combinations "${strategy[*]},${recalc[*]},${select[*]},${nonfull_factor[*]},${alloc[*]},${free[*]},${max_scan_fields[*]},${threshold_lower[*]},${ignore_bytes[*]}"
}

nohup ./$SERVER&
sleep 1
echo "$(ps -ef | grep valkey)"
# print all arguments
#echo  "Starting defrag test with parameters:$@"
# add start time suffix to TEST_FILE_NAME, print in this format: defrag_test_2020-05-04_12-34-56
timestamp=$(date +'%Y-%m-%d_%H-%M-%S')
filename="defrag_maxmem_${SERVER}.${timestamp}.${MIN_USED_MEMORY}.log"
touch "$filename"
#echo "$(ps -ef | grep valkey)" >> "$filename"
hits=1
misses=1

while true; do
  parse_config
  ((LOOPS++))

done
