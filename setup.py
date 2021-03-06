#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import (
    setup,
    find_packages,
)

extras_require = {
    'test': [
        "factory-boy==2.12.0",
        "hypothesis==5.10.4",
        "pytest==5.4.1",
        "pytest-xdist==1.31.0",
        "pytest-trio==0.5.2",
        "tox==3.14.6",
        # This doesn't actually result in a correct install of the testing
        # requirements so instead we supply the exact eth-tester requirement
        # manually.
        # "web3[tester]",
        "eth-tester[py-evm]==v0.2.0-beta.2",
    ],
    'lint': [
        'black>=18.6b4,<19',
        "flake8==3.7.9",
        "flake8-bugbear==20.1.4",
        "isort>=4.3.18,<5",
        "mypy==0.770",
        "pydocstyle>=3.0.0,<4",
    ],
    'doc': [
        "Sphinx>=1.6.5,<2",
        "sphinx_rtd_theme>=0.1.9",
        "towncrier>=19.2.0, <20",
    ],
    'dev': [
        "bumpversion>=0.5.3,<1",
        "pytest-watch>=4.1.0,<5",
        "wheel",
        "twine",
        "ipython",
    ],
    'postgres': [
        "psycopg2==2.8.5",
    ],
}

extras_require['dev'] = (
    extras_require['dev'] +  # noqa: W504
    extras_require['test'] +  # noqa: W504
    extras_require['lint'] +  # noqa: W504
    extras_require['doc']
)


with open('./README.md') as readme:
    long_description = readme.read()


setup(
    name='cthaeh',
    # *IMPORTANT*: Don't manually change the version here. Use `make bump`, as described in readme
    version='0.1.0-alpha.0',
    description="""Stand alone application for serving the Ethereum JSON-RPC logging APIs""",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='The Ethereum Foundation',
    author_email='snakecharmers@ethereum.org',
    url='https://github.com/ethereum/cthaeh',
    include_package_data=True,
    install_requires=[
        "async-service==0.1.0a7",
        "eth-typing==2.2.1",
        "eth-utils>=1,<2",
        "SQLAlchemy==1.3.16",
        "sqlalchemy-stubs==0.3",
        "trio==0.13.0",
        "trio-typing==0.3.0",
        "web3==5.7.0",
    ],
    python_requires='>=3.6, <4',
    extras_require=extras_require,
    py_modules=['cthaeh'],
    license="MIT",
    zip_safe=False,
    keywords='ethereum',
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    entry_points={
        'console_scripts': [
            'cthaeh=cthaeh._boot:_boot',
        ],
    },
)
