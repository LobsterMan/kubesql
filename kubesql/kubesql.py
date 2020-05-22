__version__ = "0.0.1"

import json
import subprocess
import sys
import pandas as pd
from io import StringIO
from kubesql.kubesql_exception import KubeSqlException
import kubesql.utils as utils
import kubesql.sql_json as sqlj


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
    # except pyparsing.ParseException as e:
    #     print(str(f"SQL ERROR: {e}"))


def parse_args():
    sql = " ".join(sys.argv[1:])
    return {'sql': sql}


def query_kubernetes(sql):
    # sql = "select * from nodes where namespace = 'kubeflow'"

    # parse query
    parsed_query = utils.parse_query(sql)

    print(json.dumps(parsed_query, indent=4, separators=(',', ': ')))

    # get results from kubectl
    # kubectl_result = get_kubectl_result(parsed_query)
    kubectl_result = get_mock_kubectl_result('test2')

    result_df = sqlj.query_json(kubectl_result, parsed_query)

    return result_df


def get_kubectl_result(parsed_query):
    # get namespace
    if parsed_query['conditions'].get('namespace'):
        namespace = ["-n", parsed_query['conditions'].get('namespace')]
    else:
        namespace = ["--all-namespaces"]

    # get resource
    resource = parsed_query['query']["from"]

    # build kubectl commant
    command = ['kubectl', 'get', resource, *namespace, '-o', 'json']

    # get result.items
    raw_result = subprocess.run(command, stdout=subprocess.PIPE)
    result = json.loads(raw_result.stdout.decode("utf8"))
    result_items = result['items']
    return result_items


def get_mock_kubectl_result(filename = "test"):
    with open(f"kubesql/{filename}.json", 'r') as file:
        result = json.loads(file.read())
    return result['items']




