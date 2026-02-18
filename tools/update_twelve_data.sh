#!/bin/bash

set -e

# Twelve Data majors
python3 main.py twelve_data --major --tf_length 1 --tf_unit day
python3 main.py twelve_data --major --tf_length 1 --tf_unit hour
python3 main.py twelve_data --major --tf_length 5 --tf_unit min

# Gold (XAU/USD)
python3 main.py twelve_data --base XAU --quote USD --tf_length 1 --tf_unit day
python3 main.py twelve_data --base XAU --quote USD --tf_length 1 --tf_unit hour
python3 main.py twelve_data --base XAU --quote USD --tf_length 5 --tf_unit min
