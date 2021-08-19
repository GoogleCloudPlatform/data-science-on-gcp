#!/bin/bash
bash download.sh
python3 remove_cols.py
bash upload.sh
