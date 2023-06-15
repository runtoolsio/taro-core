import setuptools
from setuptools import find_packages, find_namespace_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taro",
    version="0.10.1",
    author="Stan Svec",
    author_email="dev@stansvec.com",
    description="Tool for managing your jobs",
    license='MIT',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/taro-suite/taro",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers, Ops, Admins",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.8',
    packages=find_packages(exclude=("test*",)) + find_namespace_packages(include=("taro.jobs.db",)),
    install_requires=[
        "PyYAML>=5.1.2",
        "pypager>=3.0.0",
        "bottle>=0.12.18",
        "urllib3>=1.26.2",
        "portalocker>=2.6.0",
        "python-dateutil>=2.8.2",
        "pygrok>=1.0.0",
        "python-daemon>=2.3.0",
    ],
    package_data={
        'taro': ['config/*.yaml'],
        'taros': ['config/*.yaml'],
    },
    entry_points={
        "console_scripts": [
            "taro = taroapp:main_cli",
            "taros = taros:main_cli",
        ]
    },
)
