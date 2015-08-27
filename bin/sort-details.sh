#!/usr/bin/env bash

# For local mode use only! Example output:
# === Details for data-sort-tim.log ===
# * Metadata read (ms): 24
# * Sample (ms): 943
# * Input read (ms): 826 / 2414.82 / 5036
# * Input read (bytes): 1000000000 / 1000000000 / 1000000000
# * Map side sort (ms): 3532 / 5022.28 / 7112
# * Shuffle write (ms): 2927 / 9783.04 / 18688
# * Shuffle read (ms): 2300 / 8961.71 / 14382
# * Reduce side sort (ms): 1347 / 2543.08 / 4415
# * Output write (ms): 4750 / 9412.99 / 16945
# * Num output records: 7363506 / 10000000 / 13319639

script="$1"
echo "=== Details for $script ==="

# Metadata read
metadata_read=$(grep "^1.*INFO.*XXX took.*metadata" "$script" | awk '{print $7}')
echo "* Metadata read (ms): $metadata_read"

# Sample
sample=$(grep "^1.*INFO.*XXX sampling" "$script" | awk '{print $10}')
echo "* Sample (ms): $sample"

# Input read (time)
input_read_min=$(grep "^1.*INFO.*XXX finished reading file" "$script" | awk '{print $13}' | sort -n | head -n 1)
input_read_max=$(grep "^1.*INFO.*XXX finished reading file" "$script" | awk '{print $13}' | sort -n | tail -n 1)
input_read_avg=$(grep "^1.*INFO.*XXX finished reading file" "$script" | awk '{print $13}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Input read (ms): $input_read_min / $input_read_avg / $input_read_max"

# Input read (bytes)
input_read_bytes_min=$(grep "^1.*INFO.*XXX finished reading file" "$script" | awk '{print $10}' | sed s/\(//g | sort -n | head -n 1)
input_read_bytes_max=$(grep "^1.*INFO.*XXX finished reading file" "$script" | awk '{print $10}' | sed s/\(//g | sort -n | tail -n 1)
input_read_bytes_avg=$(grep "^1.*INFO.*XXX finished reading file" "$script" | awk '{print $10}' | sed s/\(//g | awk '{s += $1} END {print int(s / NR)}')
echo "* Input read (bytes): $input_read_bytes_min / $input_read_bytes_avg / $input_read_bytes_max"

# Map-side sort
map_sort_min=$(grep "^1.*INFO.*XXX Sort" "$script" | awk '{print $10}' | sort -n | head -n 1)
map_sort_max=$(grep "^1.*INFO.*XXX Sort" "$script" | awk '{print $10}' | sort -n | tail -n 1)
map_sort_avg=$(grep "^1.*INFO.*XXX Sort" "$script" | awk '{print $10}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Map side sort (ms): $map_sort_min / $map_sort_avg / $map_sort_max"

# Shuffle write
shuffle_write_min=$(grep "^1.*INFO.*XXX Time taken to write" "$script" | awk '{print $13}' | sort -n | head -n 1)
shuffle_write_max=$(grep "^1.*INFO.*XXX Time taken to write" "$script" | awk '{print $13}' | sort -n | tail -n 1)
shuffle_write_avg=$(grep "^1.*INFO.*XXX Time taken to write" "$script" | awk '{print $13}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Shuffle write (ms): $shuffle_write_min / $shuffle_write_avg / $shuffle_write_max"

# Shuffle read
shuffle_read_min=$(grep "^1.*INFO.*XXX Reduce:.*fetch" "$script" | awk '{print $7}' | sort -n | head -n 1)
shuffle_read_max=$(grep "^1.*INFO.*XXX Reduce:.*fetch" "$script" | awk '{print $7}' | sort -n | tail -n 1)
shuffle_read_avg=$(grep "^1.*INFO.*XXX Reduce:.*fetch" "$script" | awk '{print $7}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Shuffle read (ms): $shuffle_read_min / $shuffle_read_avg / $shuffle_read_max"

# Reduce-side sort
reduce_sort_min=$(grep "^1.*INFO.*XXX Reduce: Sorting" "$script" | awk '{print $11}' | sort -n | head -n 1)
reduce_sort_max=$(grep "^1.*INFO.*XXX Reduce: Sorting" "$script" | awk '{print $11}' | sort -n | tail -n 1)
reduce_sort_avg=$(grep "^1.*INFO.*XXX Reduce: Sorting" "$script" | awk '{print $11}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Reduce side sort (ms): $reduce_sort_min / $reduce_sort_avg / $reduce_sort_max"

# Output write
output_write_min=$(grep "^1.*INFO.*XXX Reduce: writing.*took" "$script" | awk '{print $11}' | sort -n | head -n 1)
output_write_max=$(grep "^1.*INFO.*XXX Reduce: writing.*took" "$script" | awk '{print $11}' | sort -n | tail -n 1)
output_write_avg=$(grep "^1.*INFO.*XXX Reduce: writing.*took" "$script" | awk '{print $11}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Output write (ms): $output_write_min / $output_write_avg / $output_write_max"

# Num output records
output_records_min=$(grep "^1.*INFO.*XXX Reduce: writing.*took" "$script" | awk '{print $8}' | sort -n | head -n 1)
output_records_max=$(grep "^1.*INFO.*XXX Reduce: writing.*took" "$script" | awk '{print $8}' | sort -n | tail -n 1)
output_records_avg=$(grep "^1.*INFO.*XXX Reduce: writing.*took" "$script" | awk '{print $8}' | awk '{s += $1} END {print int(s / NR)}')
echo "* Num output records: $output_records_min / $output_records_avg / $output_records_max"

