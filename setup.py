import os

from setuptools import setup, find_packages


def read(filename, split_line=False):
    with open(os.path.join(os.path.dirname(__file__), filename)) as f:
        if split_line:
            return f.read().splitlines()
        else:
            return f.read()


setup(
    name="monitoring",
    version="0.1.0",
    author="Jay Xu",
    author_email="cryingmouse2019@163.com",
    description="A configurable scheduled task framework",
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    url="https://github.com/Cryingmouse/monitoring",
    packages=find_packages(where="monitoring"),
    package_dir={"": "monitoring"},
    install_requires=read("requirements.txt", split_line=True),
    entry_points={
        'console_scripts': [
            'monitor_cli=monitoring.main:main',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
