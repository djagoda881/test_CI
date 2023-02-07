
#!/bin/bash

dbt init postgres -s
cp -R ../integra/ postgres/ 
cp -R ../macros/ postgres/
cp packages.yml postgres/
cp countries.csv postgres/seeds/
cp requirements.txt postgres/
cp setup.sh postgres/


