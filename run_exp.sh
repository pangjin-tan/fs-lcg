#!/bin/bash

dir="data"

# shapley_fast_arb_prec.py is based on our proposed approach implementing FS-LCG
# shapley_skibski.py is based on Skibski's implementation the baseline approach
# results (Shapley values and runtime) are saved in subfolders "log" under ID_XXX

python shapley_fast_arb_prec.py $dir ID_001
python shapley_skibski.py $dir ID_001

python shapley_fast_arb_prec.py $dir ID_002
python shapley_skibski.py $dir ID_002

python shapley_fast_arb_prec.py $dir ID_003
python shapley_skibski.py $dir ID_003

python shapley_fast_arb_prec.py $dir ID_004
python shapley_skibski.py $dir ID_004
