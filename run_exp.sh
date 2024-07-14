#!/bin/bash

dir="data"

python shapley_fast_arb_prec.py $dir ID_001
python shapley_skibski.py $dir ID_001
python shapley_fast_arb_prec.py $dir ID_002
python shapley_skibski.py $dir ID_002
python shapley_fast_arb_prec.py $dir ID_003
python shapley_skibski.py $dir ID_003
python shapley_fast_arb_prec.py $dir ID_004
python shapley_skibski.py $dir ID_004
