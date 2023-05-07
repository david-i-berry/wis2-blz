from setuptools import find_packages, setup

setup(
    name="blz_synop",
    packages=find_packages(exclude=["blz_synop_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
