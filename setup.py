import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()

requires = [
    'SQLAlchemy>=1.4',
    'sqlalchemy_utils',
]

tests_require = [
    'psycopg2',
    'testing.postgresql',
    'tox',
]

setup_requires = [
    'setuptools-git-versioning',
]

setup(
    name = 'ltree_models',
    version_config = True,
    packages = find_packages(),
    setup_requires = setup_requires,
    install_requires = requires,
    extras_require = {
        'testing': tests_require,
    },
    description = 'sqlalchemy models for ltree.',
    long_description=README,
    long_description_content_type='text/markdown',
    author = 'Colin Higgs',
    author_email = 'colin.higgs70@gmail.com',
    license = 'GNU Affero General Public License v3 or later (AGPLv3+)',
    url = '',
    keywords = ['json', 'api', 'json-api', 'jsonapi', 'jsonschema', 'openapi', 'pyramid', 'sqlalchemy'],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python :: 3',
    ],
)
