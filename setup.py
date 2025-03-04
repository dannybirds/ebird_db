from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="ebird_db",
    version="0.1.0",
    author="Your Name",
    author_email="dannywyatt@gmail.com",
    description="Import eBird data into PostgreSQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dannybirds/ebird_db",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "psycopg>=3.0.0",
        "tqdm>=4.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ebird-db=ebird_db.main:main",
        ],
    },
)