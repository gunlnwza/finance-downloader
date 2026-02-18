#!/bin/bash

set -e

# Daily majors
python3 main.py massive --major --tf_length 1 --tf_unit day
