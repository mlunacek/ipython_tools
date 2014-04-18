#!/usr/bin/bash

# Test ex_command.py
iptools start --ppn=6
python ex_command.py
iptools stop

# Test ex_module.py
python ex_module.py

iptools start --ppn=6 --profile='iptools'
python ex_ipython.py
iptools stop
