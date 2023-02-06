# Contributing to nesso CLI
### Testing
1. Clone the repo
2. cd `integra`
3. Run `docker-compose up -d --build`
4. Run `docker exec -it integra bash`
5. Run `cd tests/dbt_projects/<project> && dbt deps`
6. Develop
