"""
This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.
"""

import argparse
import logging
import os
import sys
import re
import math
import yaml

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
)

logger = logging.getLogger(__name__)
case_convert_regex = re.compile(r'(?<!^)(?=[A-Z])')
SQL_INDENTATION = "\t"


def convert_to_snake_case(input_string: str) -> str:
    """
    Converts a string from CamelCase to snake_case.

    Args:
        input_string (str): The CamelCase string to be converted.

    Returns:
        str: The string converted to snake_case.
    """
    return case_convert_regex.sub('_', input_string).lower()


def parse_command_line():
    parser = argparse.ArgumentParser(description="Generate YAML and SQL output for a BigQuery table.")
    parser.add_argument("target", help="Complete BigQuery dataset or table ID (project.dataset[.table])")
    parser.add_argument("-l", "--lower", action="store_true", help="Lowercase type names in YAML file")
    parser.add_argument("--snake", action="store_true", help="Convert field names to snake_case")
    parser.add_argument("--empty_description", action="store_true",
                        help="Include empty description property in YAML file")
    parser.add_argument("--prefix", help="Prefix to add to columns names", default=None)
    parser.add_argument("--suffix", help="Suffix to add to column names", default=None)
    parser.add_argument("--output", help="Output folder of scripts. By default 'target/bq2dbt'",
                        default='target/bq2dbt')
    return parser.parse_args()


def bq2dbt():
    """
    Main function. Parse arguments and do the job
    """
    args = parse_command_line()

    tables = []
    target_split = args.target.split(".")
    if len(target_split) == 3:  # Complete project.dataset.table
        project_id = target_split[0]
        dataset_id = target_split[1]
        tables.append(target_split[2])
    elif len(target_split) == 2:  # project.dataset only
        project_id = target_split[0]
        dataset_id = target_split[1]
    else:
        logger.error("Invalid BigQuery dataset or table ID.")
        sys.exit(1)

    client = bigquery.Client(project=project_id)

    if len(tables) == 0:
        # Get table list from bigquery
        logger.info("Retrieving table list for dataset %s...", dataset_id)
        table_list = client.list_tables(dataset_id)
        tables = [table.table_id for table in table_list]
        logger.info("%d table(s) found in dataset %s...", len(tables), dataset_id)

    prefix = args.prefix
    suffix = args.suffix
    empty_description = args.empty_description

    output_folder = args.output

    for table_id in tables:
        logger.info("Starting generation of YAML and SQL for table \"%s\"...", table_id)

        fields_query = f"""
            SELECT
                CFP.field_path,
                CFP.data_type,
                CFP.description,
                COLUMNS.is_nullable
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS CFP
                LEFT JOIN `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` AS COLUMNS
                    ON CFP.table_catalog = COLUMNS.table_catalog
                        AND CFP.table_schema = COLUMNS.table_schema
                        AND CFP.table_name = COLUMNS.table_name
                        AND CFP.column_name = COLUMNS.column_name
                        AND CFP.field_path = COLUMNS.column_name
            WHERE CFP.table_name = '{table_id}'
        """

        pk_query = f"""
            SELECT column_name
                FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
            WHERE table_name = '{table_id}'
            AND constraint_name = '{table_id}.pk$'
            ORDER BY ordinal_position
        """

        # Run the query to get column information and fetch the results
        fields = client.query(fields_query).result()
        field_types = {}

        # Get table description
        table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        table_description = table.description or ""

        # Create a list to store the YAML data
        yaml_data = {
            "models": [
                {
                    "name": table_id,
                    "description": table_description,
                    "config": {
                        "contract": {
                            "enforced": True
                        }
                    },
                    "constraints": None,
                    "columns": []
                }
            ]
        }

        # Add primary key constraint to table if there is any field declared as such in table
        # Note : this is done in respect of fields order in the primary key declaration
        primary_key = list(client.query(pk_query).result())
        if primary_key:
            yaml_data['models'][0]['constraints'] = [
                {
                    "type": "primary_key",
                    "columns": [field.column_name for field in primary_key]
                }
            ]
        else:
            del yaml_data['models'][0]['constraints']

        sql_columns = []

        # Iterate through the query results and add them to the YAML data
        for field in fields:
            data_type = field.data_type.split('<')[0]
            data_type = data_type.lower() if args.lower else data_type
            field_types[field.field_path] = data_type

            destination = convert_to_snake_case(field.field_path) if args.snake else field.field_path
            destination = "_".join(filter(None, [prefix, destination, suffix]))

            field_info = {
                "name": destination,
                "data_type": data_type
            }
            if field.description or empty_description:
                field_info['description'] = field.description or ""

            if field.is_nullable == 'NO':
                # BigQuery says array cannot be null, but they can and they don't support not_null constraint
                if data_type not in ['ARRAY']:
                    field_info = {**field_info, **{
                        "constraints": [
                            {"type": "not_null"}
                        ]
                    }}

            yaml_data["models"][0]["columns"].append(field_info)
            if '.' not in field.field_path:
                if destination != field.field_path:
                    sql_columns.append(f"`{field.field_path}` AS `{destination}`")
                else:
                    sql_columns.append(f"`{field.field_path}`")

        # Support for time partioning retrieval
        table_time_partitioning = table.time_partitioning
        table_partition_expiration = table.time_partitioning.expiration_ms if table_time_partitioning else None
        table_require_partition_filter = table.require_partition_filter
        logger.info("Time partition : %s", table_time_partitioning)
        logger.info("Partition expiration : %s", table_partition_expiration)
        logger.info("Require partition filter : %s", table_require_partition_filter)
        if table_time_partitioning:
            yaml_data['models'][0]['config']['partition_by'] = {
                "field": table_time_partitioning.field,
                "granularity": table_time_partitioning.type_,
                "data_type": field_types[table_time_partitioning.field]
            }

            if table_require_partition_filter:
                yaml_data['models'][0]['config']['require_partition_filter'] = True

            if table_partition_expiration:
                yaml_data['models'][0]['config']['partition_expiration_days'] \
                    = math.floor(table_partition_expiration / (1000 * 60 * 60 * 24))

        table_clustering_fields = table.clustering_fields
        if table_clustering_fields:
            yaml_data['models'][0]['config']['clustering'] = table_clustering_fields

        # Generate the YAML output
        yaml_output = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)

        # Generate the SQL output
        sql_columns_statement = f"\n{SQL_INDENTATION}, ".join(sql_columns)
        sql_from_statement = f"\n{SQL_INDENTATION}`{project_id}.{dataset_id}.{table_id}`"
        sql_output = (f"SELECT\n{SQL_INDENTATION}{sql_columns_statement}\nFROM{sql_from_statement}"
                      f"  -- Replace this with ref() or source() macro\n")

        output_path = f"./{output_folder}/{project_id}/{dataset_id}"
        os.makedirs(output_path, exist_ok=True)

        with open(f"{output_path}/{table}.yaml", "w", encoding="utf-8") as yaml_file:
            yaml_file.write(yaml_output)

        with open(f"{output_path}/{table}.sql", "w", encoding="utf-8") as sql_file:
            sql_file.write(sql_output.strip())

        logger.info("YAML and SQL files wrote in path: %s", output_path)

    logger.info("Operation complete")


if __name__ == "__main__":
    bq2dbt()