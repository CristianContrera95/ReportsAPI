from setuptools import find_packages, setup


packages = [
    "pandas",
    "numpy",
    "pytest<7,>=5",
    "pytest-timeout",
]

setup(
    name="vehicle-sales-report-pytools",
    version="1.0.2",
    author="Devskiller.com",
    author_email="support@devskiller.com",
    packages=find_packages(),
    python_requires=">=3.8",
    include_package_data=True,
    install_requires=packages,
    setup_requires=["pytest-runner"],
    tests_require=packages,
)
