# kubesql
Query kubernetes clusters using SQL

## Installation
Install locally (editable mode)
```
pip install -e .
```

Install from git
```
pip install git+https://github.com/LobsterMan/kubesql.git
```

## Usage

Basic Usage
```
kubesql SELECT * from pods WHERE namespace = 'kube-system' > pods.csv
```

Basic query
```
SELECT * from pods WHERE namespace = 'kube-system'
```
Select specific columns

Functions

## SQL Syntax
####Full usage
```
SELECT
    [ * | expression [ [ AS ] output_name ] [, ...] ]
    [ FROM kubernetes_resource]
    [ WHERE condition ]
    [ LIMIT count ]
```

#### expression
expression can be "column" name, or fuc(column_name)

#### functions
__cell()__  - Applied by default. 
If the cell is a list, will return "<list>", 
if the cell is dict, will return "<dict>"
else will return the string representation of the cell

This default behavior is to keep outputs readable when selecting *

__str()__ - Returns the string representation of the cell

__json()__ - returns the json representation of the cell

#### condition
Supported conditions are:

__namespace__ - kubernetes namespace. If no namespace is selected, `--all-namespaces` will be used

#### kubernetes_resource
Any valid kubernetes resource type

## TODO
- SELECT lists (e.g. spec.pods[].image)
- WHERE Conditions applied to data (other than namespace)
- output type (csv or table)
- refactor object oriented
- ORDER BY


## Credits
https://github.com/mozilla/moz-sql-parser

https://github.com/jgehrcke/python-cmdline-bootstrap