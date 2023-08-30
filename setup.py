import os

from setuptools import find_packages, setup

REQUIRE_POSTGRES = os.getenv("REQUIRE_POSTGRES") == "1"
PSYCOPG2_VERSION = "psycopg2>=2.8"

requirements = [
    # By using the extra deps boto3 and awscli, we help the solver find
    # a solution faster, since aiobotocore currently requires pinned dependencies of
    # boto3 and awscli.
    "aiobotocore[boto3,awscli]>=2.0.1",
    "aiohttp>=3.7.4,<4",
    "alembic>=1.4",
    "boto3>=1.16.63",
    # Avoid version botocore-1.28.0
    # https://github.com/iterative/dvc/issues/8513#issuecomment-1298761683
    "botocore>=1.22.8,!=1.28.0",
    "fancycompleter>=0.9.1",
    "s3fs>=2021.11.1",
    # Using 2.1 instead of 3.0 in case future 2.x versions drop support
    # for some legacy APIs still supported in 2.0
    "sqlalchemy>=1.4.0,<2.1",
    "python-dateutil>=2.8",
    # cython3 and pyyaml conflicts
    # https://github.com/yaml/pyyaml/issues/724#issuecomment-1638591821
    "pyyaml!=6.0.0,!=5.4.0,!=5.4.1",
    "requests>=2.27.1",
    "rich>=13.3.5",
    "textual>=0.24.1",
    # If updating this list, check executors/aws_glue.py stays up to date with
    # packages needed to run in the glue environment.
]

extras = {
    "glue": ["pandas", "pyarrow", "pyspark"],
    "k8s": "kubernetes>=22.6",
    "viz": "pygraphviz",
    "google-batch": [
        "gcsfs>=2021.4.0",
        "google-cloud-batch>=0.2.0",
        "google-cloud-compute>=1.11.0",
    ],
}

if REQUIRE_POSTGRES:
    requirements.append(PSYCOPG2_VERSION)
else:
    extras["postgres"] = [PSYCOPG2_VERSION]


def get_version() -> str:
    """
    Get the redun package version.
    """
    # Technique from: https://packaging.python.org/guides/single-sourcing-package-version/
    basedir = os.path.dirname(__file__)
    module_path = os.path.join(basedir, "redun", "version.py")
    with open(module_path) as infile:
        for line in infile:
            if line.startswith("version ="):
                _, version, _ = line.split('"', 2)
                return version
    assert False, "Cannot find redun package version"


setup(
    name="redun",
    version=get_version(),
    zip_safe=True,
    packages=find_packages(),
    author="Matt Rasmussen",
    author_email="rasmus@insitro.com",
    url="https://github.com/insitro/redun/",
    description="Yet another redundant workflow engine.",
    long_description="""
redun aims to be a more expressive and efficient workflow framework, built on
top of the popular Python programming language. It takes the somewhat contrarian
view that writing dataflows directly is unnecessarily restrictive, and by doing
so we lose abstractions we have come to rely on in most modern high-level
languages (control flow, compositiblity, recursion, high order functions, etc).
redun's key insight is that workflows can be expressed as lazy expressions, that
are then evaluated by a scheduler which performs automatic parallelization,
caching, and data provenance logging.

redun's key features are:

- Workflows are defined by lazy expressions that when evaluated emit dynamic directed acyclic
  graphs (DAGs), enabling complex data flows.
- Incremental computation that is reactive to both data changes as well as code changes.
- Workflow tasks can be executed on a variety of compute backend (threads, processes, AWS batch
  jobs, Spark jobs, etc).
- Data changes are detected for in memory values as well as external data sources such as files
  and object stores using file hashing.
- Code changes are detected by hashing individual Python functions and comparing against
  historical call graph recordings.
- Past intermediate results are cached centrally and reused across workflows.
- Past call graphs can be used as a data lineage record and can be queried for debugging and
  auditing.
    """,
    scripts=["bin/redun"],
    include_package_data=True,
    python_requires=">= 3.7",
    install_requires=requirements,
    extras_require=extras,
)
