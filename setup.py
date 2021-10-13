import os
from setuptools import setup, find_packages

REQUIRE_POSTGRES = os.getenv("REQUIRE_POSTGRES") == "1"
PSYCOPG2_VERSION = "psycopg2>=2.8"

requirements = [
    "aiohttp>=3.7.4",
    "alembic>=1.4",
    "gcsfs>=2021.4.0",
    "promise>=2.2",
    "sqlalchemy>=1.3.17,<2",
    "s3fs>=0.4.2",
    "boto3>=1.16.63",
    "python-dateutil>=2.8",
]

extras = {
    "glue": ["pandas", "pyarrow"],
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
    module_path = os.path.join(basedir, "redun", "__init__.py")
    with open(module_path) as infile:
        for line in infile:
            if line.startswith("__version__"):
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
    description="The redundant workflow engine.",
    long_description="",
    scripts=["bin/redun"],
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras,
)
