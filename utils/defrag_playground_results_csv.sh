#!/bin/bash

# Check if the log file argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <log_file>"
    exit 1
fi

# Log file
log_file=$1

# Define the output CSV file
output_file="defrag_playground_results.csv"
#  sleep 1
#  ./valkey-cli config set active-defrag-max-scan-fields $7
#  sleep 1
#  ./valkey-cli config set active-defrag-threshold-lower $8
#  sleep 1
#  ./valkey-cli config set active-defrag-ignore-bytes $9
# Write the CSV header
echo "strategy,recalc,select,nonfull-factor,alloc,free,max-scan-fields,threshold-lower,ignore-bytes,duration,ratio,curr_hits,curr_misses,total_time,hit_ratio,key_space_proc_ratio_hits,key_space_proc_ratio_misses" > $output_file

# Initialize variables to hold the data
strategy=""
recalc=""
select=""
nonfull_factor=""
alloc=""
free=""
duration=""
ratio=""
curr_hits=""
curr_misses=""
total_time=""
hit_ratio=""
key_space_proc_ratio_hits=""
key_space_proc_ratio_misses=""

# Function to extract values from a line
extract_values() {
    line=$1
    case $line in
        DEFRAG\ CONFIG*)
            IFS=': ' read -r _ _ _ strategy recalc select nonfull_factor alloc free max_scan_fields threshold_lower ignore_bytes <<< "$line"
            ;;
        \[38\]*)
            IFS='[] ' read -r _ _ _ _ duration _ _ _ _ _ _ _ ratio_info _ <<< "$line"
            ratio=${ratio_info##*:}
            ;;
        hit:*)
            IFS=',' read -r _ curr_hits_part curr_misses_part total_time_part _ <<< "$line"
            curr_hits=${curr_hits_part##*:}
            curr_misses=${curr_misses_part##*:}
            total_time=${total_time_part##*:}
            ;;
        \[227\]*)
            IFS=',' read -r _ hit_ratio_part key_space_proc_ratio_hits_part key_space_proc_ratio_misses_part <<< "$line"
            hit_ratio=${hit_ratio_part##*:}
            key_space_proc_ratio_hits=${key_space_proc_ratio_hits_part##*:}
            key_space_proc_ratio_misses=${key_space_proc_ratio_misses_part##*:}
            ;;
    esac
}

# Read the log line by line
while IFS= read -r line; do
    if [[ $line == DEFRAG\ CONFIG* ]]; then
        if [[ -n $strategy ]]; then
            # Write the collected data to the CSV
            echo "$strategy,$recalc,$select,$nonfull_factor,$alloc,$free,$max_scan_fields,$threshold_lower,$ignore_bytes,$duration,$ratio,$curr_hits,$curr_misses,$total_time,$hit_ratio,$key_space_proc_ratio_hits,$key_space_proc_ratio_misses" >> $output_file
        fi
        # Reset variables for the next config block
        strategy=""
        recalc=""
        select=""
        nonfull_factor=""
        alloc=""
        free=""
        max_scan_fields=""
        threshold_lower=""
        ignore_bytes=""
        duration=""
        ratio=""
        curr_hits=""
        curr_misses=""
        total_time=""
        hit_ratio=""
        key_space_proc_ratio_hits=""
        key_space_proc_ratio_misses=""
    fi
    extract_values "$line"
done < "$log_file"

# Write the last collected data to the CSV
if [[ -n $strategy ]]; then
    echo "$strategy,$recalc,$select,$nonfull_factor,$alloc,$free,$max_scan_fields,$threshold_lower,$ignore_bytes,$duration,$ratio,$curr_hits,$curr_misses,$total_time,$hit_ratio,$key_space_proc_ratio_hits,$key_space_proc_ratio_misses" >> $output_file
fi
