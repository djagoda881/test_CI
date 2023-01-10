
#!/bin/bash

dbt init databricks
cp -R ../cli/ databricks/ 
cp -R ../macros/ databricks/
cp packages.yml databricks/
cp countries.csv databricks/seeds/