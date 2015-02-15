from setuptools import setup, find_packages
setup(
    name = "papoprot",
    version = "0.1",
    packages = find_packages(),
    install_requires = ["protobuf", "txZMQ"],
)
