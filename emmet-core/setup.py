# -*- coding: utf-8 -*-
from setuptools import find_namespace_packages, setup

setup(
    name="emmet-core",
    use_scm_version={"root": "..", "relative_to": __file__},
    setup_requires=["setuptools_scm~=6.0"],
    description="Core Emmet Library",
    author="The Materials Project",
    author_email="feedback@materialsproject.org",
    url="https://github.com/materialsproject/emmet",
    packages=find_namespace_packages(include=["emmet.*"]),
    package_data={"emmet.core.vasp.calc_types": ["*.yaml"]},
    include_package_data=True,
    install_requires=[
        "pymatgen>=2021.3,<2023.0",
        "monty~=2021.3",
        "pydantic[email]~=1.8",
        "pybtex~=0.24",
        "typing-extensions>=3.7,<4.0",
        "bson~=0.5",
    ],
    license="modified BSD",
    zip_safe=False,
)
