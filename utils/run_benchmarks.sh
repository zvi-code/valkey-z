#!/bin/bash

# Default to not looping
LOOP=false

# Parse command line arguments
while getopts "l" opt; do
  case ${opt} in
    l )
      LOOP=true
      ;;
    \? )
      echo "Usage: $0 [-l]"
      echo "  -l    Loop the sequences indefinitely"
      exit 1
      ;;
  esac
done

# Define ranges and sequences
# Format: "TYPE,KEY_RANGE,FIELD_RANGE(optional),SEQUENCE"
#configurations=(
#  "hset|2000000:1000000,0:100|(50:30:-l -P 4),(150:90),(0:60),(324:180)"
#  "sadd|5000000:2000000,100:500|(40:45),(0:15),(400:90),(100:135)"
#  "dels|5000000:2000000,100:500|(40:45),(0:15,(200:90),(200:135)"
#  "delh|2000000:1000000,0:90|(200:45),(0:15),(40:90),(6000:135)"
#  "del|20000000:50000000|(20:45),(0:15),(4000:90),(6000:135)"
#  "set|10000000:50000000|(0:120),(120:60),(0:120)"
#  "set|10000000:20000000|(345:120),(0:60),(75:60),(127:60),(256:60),(512:60)"
#  "set|20000000:30000000|(782:120),(75:60),(17:60),(256:60),(512:60),(0:60)"
#  "set|30000000:40000000|(342:120),(127:60),(356:60),(512:60),(1:10),(123:50),(75:60)"
#  "set|40000000:50000000|(530:120),(256:60),(512:60),(0:60),(75:60),(127:60)"
#)
#configurations=(
#  "del|0:50000000|(0:300),(-1:300:-l -P 8 -c 40),(0:90),(-1:120:-l)"
#  "set|0:100000000|(120:300:-l -P 4 -c 40),(0:120)"
#  "set|50000000:50000000|(0:30),(75:120:-l),(258:120:-l -P 4 -c 40)"
#  "set|0:50000000|(0:30),(0:120),(258:120:-l),(258:120:-l)"
#  "set|0:100000000|(123:120:-l -c 1)"
#)
#configurations=(
#  "set|0:150000000|(102:300),(0:9999999)"
#  "set|0:100000000|(513:60:-l -P 8 -c 40),(513:60:-l -P 4 -c 40),(513:60:-l -P 1 -c 40)"
#  "set|50000000:100000000|(356:60:-l -P 1 -c 40 --threads 10),(356:60:-l -P 2 -c 40),(356:60:-l -P 8 -c 40)"
#  "set|25000000:100000000|(2034:60:-l -P 2 -c 40),(2034:60:-l -P 1 -c 40),(2034:60:-l -P 4 -c 40)"
#)
##generates frag of 1.2
#configurations=(
#  "set|0:1500000|(102:300:-l -P 8 -c 40),(0:9999999)"
#  "set|1500000:1500000|(129:300:-l -P 8 -c 40),(0:9999999)"
#  "set|3000000:1500000|(200:300:-l -P 8 -c 40),(0:9999999)"
#  "set|0:1500000|(0:300),(0:10),(102:30:-l),(0:10),(0:10)"
#  "del|0:1500000|(0:300),(-1:10:-n 750000),(0:30),(0:10),(0:10)"
#  "set|1500000:1500000|(0:300),(0:10),(0:10),(129:30:-l),(0:10)"
#  "del|1500000:1500000|(0:300),(0:10),(-1:10:-n 750000),(0:10),(0:30)"
#  "set|3000000:1500000|(0:300),(0:10),(0:10),(0:10),(200:30:-l)"
#  "del|3000000:1500000|(0:300),(0:10),(0:10),(0-1:10:-n 750000),(0:30)"
#)
##generates frag of 2 , maxmemory 5 gb, all lru
#configurations=(
#  "set|0:15000000|(1024:30:-l -P 8 -c 120 --threads 8),(700:30:-l -P 8 -c 120 --threads 8),(500:30:-l -P 8 -c 120 --threads 8),(400:30:-l -P 8 -c 120 --threads 8),(300:30:-l -P 8 -c 120 --threads 8),(200:30:-l -P 8 -c 120 --threads 8),(100:30:-l -P 8 -c 120 --threads 8)"
#)
##generates frag of 1.5-2.2 , maxmemory 5 gb, all lru
#configurations=(
#  "set|0:15000000|(1024:10:-l -P 8 -c 120 --threads 8),(700:10:-l -P 8 -c 120 --threads 8),(500:10:-l -P 8 -c 120 --threads 8),(400:10:-l -P 8 -c 120 --threads 8),(300:10:-l -P 8 -c 120 --threads 8),(200:10:-l -P 8 -c 120 --threads 8),(100:10:-l -P 8 -c 120 --threads 8),(10:10:-l -P 8 -c 120 --threads 8)"
#)

#configurations=(
#  "set|0:15000000|(1024:10:-l -P 4 -c 120 --threads 8),(132:5:-l -P 2 -c 120 --threads 2),(700:10:-l -P 4 -c 120 --threads 8),(342:10:-l -P 2 -c 120 --threads 2),(500:10:-l -P 4 -c 120 --threads 8),(723:10:-l -P 2 -c 120 --threads 2),(400:10:-l -P 4 -c 120 --threads 8),(600:10:-l -P 2 -c 120 --threads 2),(300:10:-l -P 4 -c 120 --threads 8),(900:10:-l -P 2 -c 120 --threads 2),(200:10:-l -P 4 -c 120 --threads 8),(500:10:-l -P 2 -c 120 --threads 2),(100:10:-l -P 4 -c 120 --threads 8),(1023:10:-l -P 2 -c 120 --threads 2),(10:10:-l -P 4 -c 120 --threads 8)"
#)

configurations=(
  "set|0:15000000|(1024:15:-l -P 2 -c 120 --threads 8),(132:5:-l -P 1 -c 2 --threads 2),(700:20:-l -P 2 -c 120 --threads 8),(342:10:-l -P 1 -c 2 --threads 2),(500:20:-l -P 2 -c 120 --threads 8),(723:10:-l -P 2 -c 1 --threads 2),(400:20:-l -P 2 -c 120 --threads 8),(600:10:-l -P 1 -c 2 --threads 2),(300:20:-l -P 2 -c 120 --threads 8),(900:10:-l -P 1 -c 2 --threads 2),(200:20:-l -P 2 -c 120 --threads 8),(500:10:-l -P 1 -c 2 --threads 2),(100:20:-l -P 2 -c 120 --threads 8),(1023:10:-l -P 1 -c 2 --threads 2),(10:20:-l -P 2 -c 120 --threads 8)"
  "set|15000000:10000|(1024:60:-l -P 1 -c 10 --threads 1),(1024:10:-l -P 1 -c 10 --threads 1)"
  "set|15010000:10000|(900:30:-l -P 1 -c 10 --threads 1),(900:30:-l -P 1 -c 10 --threads 1)"
  "set|15020000:10000|(800:30:-l -P 1 -c 10 --threads 1),(800:30:-l -P 1 -c 10 --threads 1)"
  "set|15030000:10000|(700:30:-l -P 1 -c 10 --threads 1),(700:30:-l -P 1 -c 10 --threads 1)"
  "set|15040000:10000|(600:30:-l -P 1 -c 10 --threads 1),(600:30:-l -P 1 -c 10 --threads 1)"
  "set|15050000:10000|(500:30:-l -P 1 -c 10 --threads 1),(500:30:-l -P 1 -c 10 --threads 1)"
  "set|15060000:10000|(400:30:-l -P 1 -c 10 --threads 1),(400:30:-l -P 1 -c 10 --threads 1)"
  "set|15070000:10000|(300:30:-l -P 1 -c 10 --threads 1),(300:30:-l -P 1 -c 10 --threads 1)"
  "set|15080000:10000|(200:30:-l -P 1 -c 10 --threads 1),(200:30:-l -P 1 -c 10 --threads 1)"
  "set|15090000:10000|(100:30:-l -P 1 -c 10 --threads 1),(100:30:-l -P 1 -c 10 --threads 1)"
  "set|15100000:10000|(10:30:-l -P 1 -c 10 --threads 1),(10:30:-l -P 1 -c 10 --threads 1)"
  "get|15010000:15050000|(-1:600:-l -P 1 -c 40 --threads 4)"
)
./valkey-cli config set maxmemory-policy allkeys-lru
./valkey-cli config set maxmemory 5000000000
# Function to run a benchmark sequence for a single configuration
run_configuration_sequence() {
  config=$1
  IFS='|' read -r type ranges sequence <<< "$config"

  IFS=',' read -r key_range field_range <<< "$ranges"

  IFS=',' read -ra seq <<< "$sequence"
  echo "Starting sequence for $type keys with range $key_range seq $seq"
  while true; do
    for ((j=0; j<${#seq[@]}; j+=1)); do
      IFS='(:)' read -r p block_size time args c <<< "${seq[$j]}"

      if [ "$block_size" -eq 0 ]; then
        #echo "Pausing for $type keys for $time seconds"
        sleep $time
      else
        #echo "Running benchmark for $type keys with block size $block_size for $time seconds args $args"
        # Construct the valkey-benchmark command based on the type
        cmd="./valkey-benchmark -t $type -r $key_range -q $args"
        if [[ "$type" != del* ]]; then
          if [ "$type" != "get" ]; then
            cmd+=" -d $block_size"
            if [ "$type" != "set" ]; then
              cmd+=" -e $field_range"
            fi
          fi
        fi
        #echo "$cmd"
        if [ "$type" != "get" ]; then
          taskset -c '9-24' $cmd >> /dev/null &
        else
          taskset -c '9-24' $cmd &
        fi
        pid=$!
        sleep $time
        kill $pid
        wait $pid
        echo "Finished benchmark for $type keys with key range $key_range  block size $block_size"
      fi
    done

    if [ "$LOOP" = false ]; then
      break
    fi
    echo "Restarting sequence for $type keys"
  done
  echo "Finished sequence for $type keys"
}

# Main execution
for config in "${configurations[@]}"; do
  run_configuration_sequence "$config" &
done

# Wait for all configurations to complete
wait

echo "All benchmarks completed"