import json
import kubesql.utils as utils
import pandas as pd
import kubesql.cell_processing as cp


def query_json(object, query):
    if type(object) == str:
        object = json.loads(object)

    if type(query) == str:
        query = utils.parse_query(query)

    # select columns
    results_selected_columns = select_columns(object, query)

    # limit
    if query['query'].get('limit'):
        limit = query['query']['limit']
        results_selected_columns = results_selected_columns[:limit]

    # get results as DataFrame
    result_df = pd.DataFrame(results_selected_columns)
    return result_df


def select_columns(result, query):
    rows = []

    columns = get_column_names(result, query)

    for row in result:
        # flattened_row =  {**row['metadata'], **row['spec']} # temporary hack
        rows.extend(process_row(row, columns))

    return rows


def process_row(row, columns):
    final_row = {}

    for col in columns:
        column_name = col["name"]
        cell_value = cp.get_cell_value(row, col['column'])
        column_content = cp.process_cell(cell_value, col["func"], col["column"])
        final_row[column_name] = column_content

    return [final_row]


def get_column_names(result, query):
    if query['query']['select'] == '*':
         return get_all_column_names(result)
    else:
        return get_selected_column_names(query)


def get_all_column_names(result):
    columns = []

    row = result[0] # {**result[0]['metadata'], **result[0]['spec']} #temporary hack

    # extract columns from 1st row
    for key in row:
        # type is 'column' when selecting *
        columns.append({"column": key, "func": "cell", "name": key})

    return columns


def get_selected_column_names(query):
    columns = []

    # if there's a single column
    if type(query['query']['select']) is dict:
        column_name = query['query']['select']['value']
        alias = query['query']['select'].get("name", column_name)
        columns.append({"column": column_name, "func": "cell", "name": alias})
        return columns

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
