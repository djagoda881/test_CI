# Data Platform CLI tool
## Contributing

### Running the tests
Currently, as the tests connect to Databricks, they have to be ran from within the `data_platform` container:

`docker exec -it data_platform sh -c "cd dbt/data_platform/cli && python -m pytest test_cli.py"`

### Local installation
Local installation requires Databricks Connect.

```
# in ~/.bashrc
export SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark

# in a shell
sudo apt install -y wget apt-transport-https && \
    sudo mkdir -p /etc/apt/keyrings && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo tee /etc/apt/keyrings/adoptium.asc && \
    sudo apt update && \
    sudo apt install temurin-11-jdk

cd data_platform/cli && flit install --symlink --deps=all

```
### Generating coverage badge
```
cd dbt/data_platform/cli
coverage xml -o coverage/coverage.xml
coverage html -d coverage/report
genbadge coverage -i coverage/coverage.xml
```

You can instal the VSCode "Live Preview" extension to view the HTML reports generated in `coverage/report`.