from distutils.core import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="buckets-for-babies",
    author="Zev Averbach",
    author_email="zev@averba.ch",
    url="https://github.com/zevaverbach/buckets-for-babies",
    license="Apache License 2.0",
    python_requires=">=3.10.0",
    install_requires=[
        "boto3",
    ],
    description="A friendly way to interact with S3 buckets and things in them.",
    long_description=long_description,
    version="0.0.1",
    packages=[
        "buckets_for_babies",
    ],
)
