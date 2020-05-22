import re
from setuptools import setup


version = re.search(
    '^__version__\s*=\s*"(.*)"',
    open('kubesql/kubesql.py').read(),
    re.M
    ).group(1)




setup(
    name = "kubesql",
    packages = ["kubesql"],
    entry_points = {
        "console_scripts": ['kubesql = kubesql.kubesql:main']
        },
    version = version,
    description = "Query Kubernetis using SQL",
    long_description = "",
    author = "Reuven Kaplan",
    author_email = "rkaplan18 at google's email service",
    url = "",
    )
