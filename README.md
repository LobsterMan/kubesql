# kubesql
Query kubernetes clusters using SQL

## Usage
Install locally
```
pip install -e .
```

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

## TODO
- Select sub columns (e.g. metadata.name)
- Data based WHERE Conditions (other than namespace)
- output type
- Select lists (e.g. spec.pods[].image)
- Select functions json, yaml
- refactor object oriented
- limits


## Credits
https://github.com/mozilla/moz-sql-parser

https://github.com/jgehrcke/python-cmdline-bootstrap