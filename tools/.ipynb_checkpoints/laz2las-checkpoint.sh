#!/bin/bash
echo "convert laz to las"
LAS_PATH='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las'
input_file="/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/is2_calval/data/calval_laz_list.txt"
#input_file="/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/is2_calval/data/test.txt"
while IFS= read -r line; do
    basefile=$(basename "$line")
    region=$(echo "$line" | awk -F'/' '{print $8}')
    name=$(echo "$line" | awk -F'/' '{print $9}')
    out_dir="${LAS_PATH}/${region}/${name}"
    mkdir -p "$out_dir"
    out_f="${out_dir}/${basefile}"
    if [ ! -f "$out_f" ]; then
        echo "Processing $out_f (does not exist)"
        las2las64 -i "$line" -o "$out_f"
    else
        echo "Skipping $out_f (already exists)"
    fi
done < "$input_file"