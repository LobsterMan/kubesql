import json
import kubesql.utils as utils
import pandas as pd
import kubesql.cell_processing as cp


def query_json(json_list, query):
    if type(json_list) == str:
        json_list = json.loads(json_list)

    if type(query) == str:
        query = utils.parse_query(query)

    # select columns
    resultset = build_result_set(json_list, query)

    print("==============================")
    print(resultset)
    exit()

    # limit
    if query['query'].get('limit'):
        limit = query['query']['limit']
        resultset = resultset[:limit]

    # get results as DataFrame
    result_df = pd.DataFrame(resultset)
    return result_df


def build_result_set(rows, query):
    new_rows = []

    columns = get_column_names(rows, query)

    for row in rows:
        new_rows.extend(build_result_set_for_row(row, columns))

    return new_rows


def build_result_set_for_row(row, selects):
    final_row = []

    # if there's only 1 select, it is given as a single dict
    if type(selects) is dict:
        selects = [selects]

    for select in selects:
        column_value = []
        column_value.extend(get_column_values(row, select))
        final_row.extend(column_value)

    return [final_row]


def get_column_values(obj, selector):
    if type(selector) is dict:
        func = list(selector.keys())[0]
        inner_select = selector[func]
        return_value = get_column_values(obj, inner_select)
        return apply_func(func, return_value)
    elif type(selector) is str:
        # get parts using . as delimiter
        col_name_parts = selector.split(".")
        # get current col name
        cur_col_name = col_name_parts[0]
        # calculate next level's col name
        next_col_name = ".".join(col_name_parts[1:])
        # HACK. @ is the internal notation for an array
        if cur_col_name.endswith("@"):
            is_list = True
            cur_col_name = cur_col_name.replace('@', '')
        else:
            is_list = False

        if isinstance(obj, dict):
            cur_obj = obj.get(cur_col_name)
        else:
            return obj

        if len(col_name_parts) > 1 and cur_obj:
            if is_list:
                return expand_list(cur_obj, next_col_name)
            else:
                return get_column_values(cur_obj, next_col_name)
        else:
            return cur_obj


def expand_list(list_to_expand, col_name):
    result_list = ExpandableList()
    for item in list_to_expand:
        list_item = get_column_values(item, col_name)
        result_list.append(list_item)

    return result_list


class ExpandableList(list):
    pass



def apply_func(func, data):
    return data


def select_from_row(select, row):
    return row.get(select)



def get_column_names(result, query):
    if query['query']['select'] == '*':
         return get_all_column_names(result)
    else:
        return query['query']['select']


def get_all_column_names(result):
    select = []

    row = result[0]

    # extract columns from 1st row
    for key in row:
        # type is 'column' when selecting *
        select.append({"value": key})

    return select

