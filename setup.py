from setuptools import find_packages, setup

setup(
    name="ticketeer",
    packages=find_packages(exclude=["ticketeer_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
