
#!/bin/bash

dbt init postgres
cp -R ../cli/ postgres/ 
cp -R ../macros/ postgres/
cp packages.yml postgres/
mkdir -p postgres/seeds/master_data/
cp countries.csv postgres/seeds/master_data/
cp "Average salary test.xlsx" postgres/seeds/master_data/
cp requirements.txt postgres/
cp setup.sh postgres/


