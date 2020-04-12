# -*- coding: utf-8 -*-
import datetime
from pathlib import Path
from setuptools import setup, find_namespace_packages

setup(
    name="emmet-core",
    use_scm_version={"relative_to": Path(__file__).parent},
    setup_requires=["setuptools_scm"],
    description="Core Emmet Library",
    author="The Materials Project",
    author_email="feedback@materialsproject.org",
    url="https://github.com/materialsproject/emmet",
    packages=find_namespace_packages(include=["emmet.*"]),
    install_requires=["pymatgen", "monty", "pydantic", "pybtex"],
    license="modified BSD",
    zip_safe=False,
)