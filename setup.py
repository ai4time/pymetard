import io

import setuptools


CLIENT_VERSION = "0.0.4"
PACKAGE_NAME = "pymetard"

try:
    with io.open("README.md", encoding="utf-8") as f:
        LONG_DESCRIPTION = f.read()
except FileNotFoundError:
    LONG_DESCRIPTION = ""

REQUIRES = []
with open('requirements.txt') as f:
    for line in f:
        line, _, _ = line.partition('#')
        line = line.strip()
        REQUIRES.append(line)

setuptools.setup(
    name=PACKAGE_NAME,
    version=CLIENT_VERSION,
    license="MIT License",
    description="Download parsed METAR data.",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="AI4Time",
    author_email="huang.yipeng@hotmail.com",
    url="https://github.com/ai4time/pymetard",
    install_requires=REQUIRES,
    python_requires='>=3.7',
    packages=setuptools.find_packages(),
    package_dir={"pymetard": "pymetard"},
    package_data={
        'pymetard': [
            "metar/stations.txt",
        ],
    },
    entry_points = '''
        [console_scripts]
        pymetard = pymetard.cli:main
    ''',
    include_package_data=True,
    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
