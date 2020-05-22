from kubesql.kubesql_exception import KubeSqlException
import json


def process_cell(data, func, col_name):
    functions = {
        "cell": process_cell_cell,
        "str": process_cell_str,
        "json": process_cell_json,
        "dict_keys": process_cell_dict_keys,
    }

    if func not in functions.keys():
        raise KubeSqlException(f"Unknow function '{func}' for column '{col_name}'")

    return functions[func](data)


def process_cell_cell(data):
    if isinstance(data, dict):
        keys = ",".join(data.keys())
        formatted_cell = f"<dict: {keys}>"
    elif type(data) is list:
        formatted_cell = "<list>"

    elif type(data) is ExpandableList:
        """
        Since we don't support expanding lists yet, if the list only has 1 item, show as stirng
        """
        if len(data) == 1:
            formatted_cell = str(data[0])
        else:
            formatted_cell = str(data)
    else:
        formatted_cell = str(data)
    return formatted_cell


def process_cell_str(data):
    return str(data)


def process_cell_json(data):
    return json.dumps(data)


def process_cell_dict_keys(data):
    keys = ", ".join(data.keys())
    return f"[{keys}]"


def get_cell_value(obj, cell_name):
    """
    Traverse a tree using . and [] notations to get requested value
    """
    # get parts using . as delimiter
    cell_name_parts = cell_name.split(".")
    # get current cell name
    cur_cell_name = cell_name_parts[0]
    # calculate next level's cell name
    next_cell_name = ".".join(cell_name_parts[1:])


    # HACK. @ is the internal notation for an array
    if cur_cell_name.endswith("@"):
        is_list = True
        cur_cell_name = cur_cell_name.replace('@', '')
    else:
        is_list = False

    if isinstance(obj, dict):
        cur_obj = obj.get(cur_cell_name)
    else:
        return obj

    if len(cell_name_parts) > 1 and cur_obj:
        if is_list:
            return expand_list(cur_obj, next_cell_name)
        else:
            return get_cell_value(cur_obj, next_cell_name)
    else:
        return cur_obj


def expand_list(list_to_expand, cell_name):
    result_list = ExpandableList()
    for item in list_to_expand:
        list_item = get_cell_value(item, cell_name)
        result_list.append(list_item)

    return result_list


class ExpandableList(list):
    pass
