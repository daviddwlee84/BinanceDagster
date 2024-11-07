from setuptools import find_packages, setup

setup(
    name="BinanceDagster",
    packages=find_packages(exclude=["BinanceDagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
