"""
This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.
"""

import yaml
import argparse
import os
import logging

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
)

logger = logging.getLogger(__name__)


def bq2dbt():
    parser = argparse.ArgumentParser(description="Generate YAML and SQL output for a BigQuery table.")
    parser.add_argument("table_id", help="Complete BigQuery table ID (project.dataset.table)")
    parser.add_argument("-l", "--lower", action="store_true", help="Lowercase type names in YAML file")
    args = parser.parse_args()

    project_id, dataset_id, table_name = args.table_id.split(".")

    logger.info(f"Starting generation of YAML and SQL for table {args.table_id}...")

    client = bigquery.Client(project=project_id)

    fields_query = f"""
        SELECT C.field_path, C.data_type, C.description, C2.is_nullable
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS C
            LEFT JOIN `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` AS C2
                ON C.table_catalog = C2.table_catalog
                    AND C.table_schema = C2.table_schema
                    AND C.table_name = C2.table_name
                    AND C.column_name = C2.column_name
                    AND C.field_path = C2.column_name
        WHERE C.table_name = '{table_name}'
    """

    pk_query = f"""
        SELECT C.column_name
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` AS C
        WHERE table_name = '{table_name}'
        AND constraint_name = '{table_name}.pk$'
        ORDER BY ordinal_position
    """

    # Run the query to get column information and fetch the results
    fields = client.query(fields_query).result()
    primary_key = client.query(pk_query).result()

    # Get table description
    table = client.get_table(f"{project_id}.{dataset_id}.{table_name}")
    table_description = table.description or ""

    # Add primary key constraint to table if there is any field declared as such in table
    # Note : this is done in respect of fields order in the primary key declaration
    constraints = [
        {
            "type": "primary_key",
            "columns": [field.column_name for field in primary_key]
        }
    ] if primary_key else None

    # Create a list to store the YAML data
    yaml_data = {
        "models": [
            {
                "name": table_name,
                "description": table_description,
                "config": {
                    "contract": {
                        "enforced": True
                    }
                },
                "constraints": constraints,
                "columns": []
            }
        ]
    }

    sql_columns = []

    # Iterate through the query results and add them to the YAML data
    for field in fields:
        data_type = field.data_type.split('<')[0]
        field_info = {
            "name": field.field_path,
            "data_type": data_type.lower() if args.lower else data_type,
            "description": field.description
        }
        if field.is_nullable == 'NO':
            field_info = {**field_info, **{
                "constraints": [
                    {"type": "not_null"}
                ]
            }}

        yaml_data["models"][0]["columns"].append(field_info)
        if '.' not in field.field_path:
            sql_columns.append(f"`{field.field_path}`")

    # Generate the YAML output
    yaml_output = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)

    # Generate the SQL output
    sql_columns_statement = "\n    , ".join(sql_columns)
    sql_output = f"""
    SELECT
        {sql_columns_statement}
    FROM
        `{project_id}.{dataset_id}.{table_name}`  -- Don't leave this in your DBT Statement
    """

    output_path = f"./output/{project_id}/{dataset_id}"
    os.makedirs(output_path, exist_ok=True)

    logger.info(f"Generating YAML and SQL files to path: {output_path}")

    with open(f"{output_path}/{table_name}.yaml", "w") as yaml_file:
        yaml_file.write(yaml_output)

    with open(f"{output_path}/{table_name}.sql", "w") as sql_file:
        sql_file.write(sql_output.strip())

    logger.info("Operation complete")


if __name__ == '__main__':
    bq2dbt()