import setuptools
from setuptools import find_packages, find_namespace_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taro",
    version="0.0.12",
    author="Stan Svec",
    author_email="stan.x.svec@gmail.com",
    description="Tool for managing your (mainly cron) jobs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/StanSvec/taro",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers, Ops, Admins",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.6',
    packages=find_packages(include=("taro", "taroapp", "taros")) + find_namespace_packages(include=("taro.db",)),
    install_requires=[
        "PyYAML>=5.1.2",
        "pypager>=3.0.0",
        "bottle>=0.12.18",
        "urllib3>=1.26.2",
        "python-daemon>=2.2.4",
    ],
    package_data={
        'taro': ['config/*.yaml'],
    },
    entry_points={
        "console_scripts": [
            "taro = taroapp:main_cli",
        ]
    },
)
