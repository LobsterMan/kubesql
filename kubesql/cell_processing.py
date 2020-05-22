from kubesql.kubesql_exception import KubeSqlException
import json

def process_cell(data, func, col_name):
    functions = {
        "cell": process_cell_cell,
        "str": process_cell_str,
        "json": process_cell_json
    }

    if func not in functions.keys():
        raise KubeSqlException(f"Unknow function '{func}' for column '{col_name}'")

    return functions[func](data)


def process_cell_cell(data):
    if isinstance(data, dict):
        formatted_cell = "<dict>"
    elif isinstance(data, list):
        formatted_cell = "<list>"
    else:
        formatted_cell = str(data)
    return formatted_cell


def process_cell_str(data):
    return str(data)


def process_cell_json(data):
    return json.dumps(data)