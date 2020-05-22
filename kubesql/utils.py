from moz_sql_parser import parse
from kubesql.kubesql_exception import KubeSqlException
import json


def parse_query(sql):
    # hack, the sql parser cannot accept [] as part of an identifier
    sql = sql.replace("[]", '@')

    query = parse(sql)

    print(json.dumps(query, indent=4, separators=(',', ': ')))

    conditions = parse_conditions(query.get('where',[]))

    return {
        'query': query,
        'conditions': conditions
    }


def parse_conditions(where):
    conditions = {}
    for key in where:
        if key == "and":
            for q in where[key]:
                conditions.update(parse_conditions(q))
        elif key == "eq":
            condition_name = where[key][0]
            condition_value = parse_value(where[key][1])
            conditions.update({condition_name: condition_value})
        else:
            raise KubeSqlException(f"Cannot process '{key}' conditions")
    return conditions


def parse_value(value):
    if isinstance(value, dict):
        if value.get("literal"):
            return value.get("literal")
        else:
            raise KubeSqlException("Only literals can be parsed")
    else:
        return str(value)