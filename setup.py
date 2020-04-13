import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taro",
    version="0.0.1",
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
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.5',
    packages=setuptools.find_packages(include=("taro",), exclude=("test",)),
    install_requires=[
        "PyYAML>=5.1.2",
        "pypager>=3.0.0",
    ],
    package_data={
        'taro': ['config/*.yaml'],
    },
    entry_points={
        "console_scripts": [
            "taro = taro.app:main_cli",
        ]
    },
)