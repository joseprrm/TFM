from setuptools import setup, find_packages

def parse_requirements():
    with open('requirements.txt', 'rt') as f:
        return f.readlines()

setup(
    name="tfm",
    version="0.1",
    packages=find_packages(where="tfm"),
    package_dir={"": "tfm"},
    install_requires=parse_requirements()
)
