#!/bin/bash

set -e

# Daily majors
python3 main.py alpha_vantage --major --tf_length 1 --tf_unit day
