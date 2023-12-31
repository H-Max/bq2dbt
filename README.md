# bq2dbt

This script is used to generate the explicit SQL file (explicit = don't use select star) from an existing table in bigquery, as well as the dbt model yaml definition including:

- contract enforcement
  - with correct column types
  - with description import
  - with correct not_null contraints
  - with primary key if any
  - with correct handling of the ARRAY/STRUCT types

Ouput is generated in the `./target/bq2dbt/{{project}}/{dataset}` folder from where you run the script. You can change it with option `--output`.

## Disclaimer

I know this looks like something the codegen dbt package could do, but it was faster for me to develop this rather than contribute both to the BigQuery adapter in dbt and the codegen package (this does not mean that I won't).

We used this script to convert an existing project with missing contracts and with `SELECT *` statements to something for robust and implicit, and with existing tables in BigQuery. This is the main purpose of this.

# Install the script localy

Just use pip:

```
# Latest version
pip install git+https://github.com/H-Max/bq2dbt.git

# Pinned tag/version
pip install git+https://github.com/H-Max/bq2dbt.git@v0.1.0
```

# How to run it ?

Just run the script with the complete table path in argument.

Example:
```shell
 bq2dbt myproject.mydataset.mytable
```

# CLI arguments

| Option          | Description                                              |
|-----------------|----------------------------------------------------------|
| `-l`, `--lower` | Output type names as lowercase in YAML file              |
| `--snake`       | Convert field names to snake_case (SQL and YAML)         |
| `--prefix`      | Prefix to add to columns names (default: None)           |
| `--suffix`      | Suffix to add to column names (default: None)            |
| `--output`      | Destination folder for scripts. By default target/bq2dbt |

# TODO

- Error handling
- Unit testing
- Merging with existing yaml definition files
- Generate the files for a complete dataset rather than a single table
- Option to output to stdout
  - With the option to select SQL or YAML file only