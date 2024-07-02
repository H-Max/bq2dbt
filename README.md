# bq2dbt

This script is used to generate the explicit SQL file (explicit = don't use select star) from an existing table in BigQuery, as well as the [_dbt_](https://www.getdbt.com/) model YAML definition including:

- contract enforcement
  - with column types
  - with description import
  - with not_null contraints
  - with primary key if any
  - with handling of the ARRAY/STRUCT types

Output is generated in the `./target/bq2dbt/{project}/{dataset}` folder from where you run the script.

You can change it with CLI argument `--output`.

## Disclaimer

I know this looks like something the [codegen](https://hub.getdbt.com/dbt-labs/codegen/latest/) _dbt_ package could do, but it was faster for me to develop this rather than contribute both to the BigQuery adapter in _dbt_ and the codegen package (this does not mean that I won't).

We used this script to convert an existing project with missing contracts and with `SELECT *` statements to something for robust and implicit, and with existing tables in BigQuery. This is the main purpose of this.

# Installation

## From source

```
pip install .
```

## From Github

```
# Latest version
pip install git+https://github.com/H-Max/bq2dbt.git

# Pinned tag/version
pip install git+https://github.com/H-Max/bq2dbt.git@v0.2.5
```

# How to run it ?

Just run the script with complete path of your BigQuery table or dataset.

When using a dataset only, it will generate SQL and YAML files for all found tables in the dataset.

Examples:

```shell
# Single table
bq2dbt myproject.mydataset.mytable
 
# Complete dataset
bq2dbt myproject.mydataset
```

# CLI arguments

| Option                | Description                                                                             |
|-----------------------|-----------------------------------------------------------------------------------------|
| `-l`, `--lower`       | Output type names as lowercase in YAML file                                             |
| `--snake`             | Convert field names to snake_case                                                       |
| `--prefix`            | Prefix to add to columns names (default: None)                                          |
| `--suffix`            | Suffix to add to column names (default: None)                                           |
| `--output`            | Destination folder for scripts. (default: target/bq2dbt)                                |
| `--empty_description` | Add empty description property to YAML file if field description is empty (placeholder) |

# TODO

- [ ] Error handling
- [ ] Unit testing
- [ ] Merging with existing yaml definition files
- [x] Generate the files for a complete dataset rather than a single table
- [x] Support for clustering
- [x] Support for time partitioning
- [ ] Support for range paritioning
- [ ] Option to output to stdout
  - [ ] With the option to select SQL or YAML file only