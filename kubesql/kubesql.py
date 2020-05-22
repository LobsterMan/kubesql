__version__ = "0.0.1"


from moz_sql_parser import parse
import json
import subprocess
import sys
import pyparsing
import pandas as pd
from io import StringIO
import kubesql.cell_processing as cp
from kubesql.kubesql_exception import KubeSqlException


def main():
    try:
        args = parse_args()
        result_df = query_kubernetes(args['sql'])

        # convert to csv
        fp = StringIO()
        result_df.to_csv(fp, index=False)
        csv_string = fp.getvalue()

        # print csv to output
        print(csv_string)

    except KubeSqlException as e:
        print(str(f"ERROR: {e}"))
    except pyparsing.ParseException as e:
        print(str(f"SQL ERROR: {e}"))


def parse_args():
    sql = " ".join(sys.argv[1:])
    return {'sql': sql}


def query_kubernetes(sql):
    # sql = "select * from nodes where namespace = 'kubeflow'"

    # parse query
    parsed_query = parse_query(sql)

    print(json.dumps(parsed_query, indent=4, separators=(',', ': ')))

    # get results from kubectl
    kubectl_result = get_kubectl_result(parsed_query)

    # select columns
    results_selected_columns = select_columns(kubectl_result, parsed_query)

    if parsed_query['query'].get('limit'):
        limit = parsed_query['query']['limit']
        results_selected_columns = results_selected_columns[:limit]

    # get results as DataFrame
    result_df = pd.DataFrame(results_selected_columns)

    return result_df


def parse_query(sql):
    query = parse(sql)
    conditions = parse_conditions(query['where'])

    return {
        'query': query,
        'conditions': conditions
    }


def get_kubectl_result(parsed_query):
    # get namespace
    if parsed_query['conditions'].get('namespace'):
        namespace = ["-n", parsed_query['conditions'].get('namespace')]
    else:
        namespace = ["--all-namespaces"]

    # get resource
    resource = parsed_query['query']["from"]

    #build kubectl commant
    command = ['kubectl', 'get', resource, *namespace, '-o', 'json']

    # get result.items
    raw_result = subprocess.run(command, stdout=subprocess.PIPE)
    result = json.loads(raw_result.stdout.decode("utf8"))
    result_items = result['items']
    return result_items


def select_columns(result, query):
    rows = []

    columns = get_column_names(result, query)

    for row in result:
        flattened_row = {**row['metadata'], **row['spec']} # temporary hack
        rows.append(process_row(flattened_row, columns))

    return rows


def process_row(row, columns):
    final_row = {}

    for col in columns:
        column_name = col["name"]
        column_content = cp.process_cell(row.get(col["column"], None), col["func"], col["column"])
        final_row[column_name] = column_content

    return final_row


def get_column_names(result, query):
    if query['query']['select'] == '*':
         return get_all_column_names(result)
    else:
        return get_selected_column_names(query)


def get_all_column_names(result):
    columns = []

    row = {**result[0]['metadata'], **result[0]['spec']} #temporary hack

    # extract columns from 1st row
    for key in row:
        # type is 'column' when selecting *
        columns.append({"column": key, "func": "cell", "name": key})

    return columns


def get_selected_column_names(query):
    columns = []

    for column in query['query']['select']:
        if isinstance(column['value'], str):
            column_name = column['value']
            alias = column.get("name", column_name)
            columns.append({"column": column_name, "func": "cell", "name": alias})
        else:
            for func, column_name in column["value"].items():
                alias = column.get("name", column_name)
                columns.append({"column": column_name, "func": func, "name": alias})
    return columns


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


