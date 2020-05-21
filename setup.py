import setuptools

with open("README.md", "r") as fh:
    _LONG_DESCRIPTION = fh.read()


setuptools.setup(
    name="cloud-utils",
    version="0.0.1",
    long_description=_LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    packages=setuptools.find_namespace_packages(),
    zip_safe=False,
    install_requires=[
        "azure-storage-blob==2.1.0",
        "google-cloud-storage",
        "pymongo",
        "xmltodict",
        "gamla",
    ],
)
