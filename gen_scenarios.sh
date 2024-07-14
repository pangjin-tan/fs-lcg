#!/bin/bash

dir="data"

# cp -r data12/ID_000 $dir/ID_000

cp -r $dir/ID_000 $dir/ID_001
cp -r $dir/ID_000 $dir/ID_002
cp -r $dir/ID_000 $dir/ID_003
cp -r $dir/ID_000 $dir/ID_004
cp -r $dir/ID_000 $dir/ID_005

python gen_networks.py $dir ID_005 small-world 50

python remove_ff.py $dir ID_005 ID_004 40
python remove_ff.py $dir ID_004 ID_003 30
python remove_ff.py $dir ID_003 ID_002 20
python remove_ff.py $dir ID_002 ID_001 10