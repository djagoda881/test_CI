
#!/bin/bash

dbt init postgres
cp -R ../cli/ postgres/ 
cp -R ../macros/ postgres/
cp packages.yml postgres/
cp countries.csv postgres/seeds/
cp requirements.txt postgres/
cp setup.sh postgres/