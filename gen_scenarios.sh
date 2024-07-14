#!/bin/bash

dir="data"

# cp -r data12/ID_000 $dir/ID_000

# create empty folders

cp -r $dir/ID_000 $dir/ID_001
cp -r $dir/ID_000 $dir/ID_002
cp -r $dir/ID_000 $dir/ID_003
cp -r $dir/ID_000 $dir/ID_004

# first generate a netwrok with 40 forwarders
python gen_networks.py $dir ID_004 small-world 40

# then remove 10 forwarders at a time to generate the rest of scenarios
python remove_ff.py $dir ID_004 ID_003 30
python remove_ff.py $dir ID_003 ID_002 20
python remove_ff.py $dir ID_002 ID_001 10