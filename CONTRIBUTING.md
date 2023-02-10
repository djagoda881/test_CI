# Contributing to nesso CLI
## Testing
1. Clone the repo
2. Run `cd nesso`
3. Run `docker/setup`
4. Run `./up -w tests`
5. Run `cd <project> && pytest ../../test_source.py`, eg.
```bash
cd postgres && pytest ../../test_source.py
```

We utlize fixtures in specific test files, as well as global fixtures, such as `setup()`, in `conftest.py`.

In the tests, we pre-create two test tables in Postgres, `test_table_contact` and `test_table_account`. All tests should assume these tables exist.

## Releasing
Releases are made from the `master` branch. To release a version, publish a version tag from the `master` branch:
```bash
git checkout master
git pull
git tag -a v0.0.2
git push origin v0.0.2
```


## Test coverage
Run below to generate a coverage badge
```bash
cd cli
coverage xml -o coverage/coverage.xml
coverage html -d coverage/report
genbadge coverage -i coverage/coverage.xml
```

You can instal the VSCode "Live Preview" extension to view the HTML reports generated in `coverage/report`.