from setuptools import setup, find_packages

setup(name='bq2dbt', version='0.1.0',
      description='Read a table/view from BigQuery and generates model definition YAML file'
                  'and basic select query with explicit columns list',
      url='https://github.com/H-Max/bq2dbt',
      author='Henri-Maxime Ducoulombier',
      author_email='hmducoulombier@gmail.com',
      license='GNU General Public Licence v3',
      packages=find_packages(),
      entry_points={
        'console_scripts': [
            'bq2dbt = bq2dbt.bq2dbt:bq2dbt',
        ],
    },
      install_requires=['PyYAML==6.0.1', 'google-cloud-bigquery==3.11.4']
)
