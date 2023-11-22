"""
bq2dbt

Read a table/view from BigQuery and generates model definition YAML file
and basic select query with explicit columns list
"""

from .bq2dbt import bq2dbt


def main():
    """Entry point for the application script"""
    bq2dbt()
