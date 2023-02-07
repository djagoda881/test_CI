#!/bin/bash

# Installing integra
pip3 install -r requirements.txt --user
cd integra/
pip3 install flit --user
flit install --symlink --deps=develop --user