import os

from setuptools import find_packages, setup


base_dir = os.path.dirname(__file__)

about = {}
with open(os.path.join(base_dir, 'elasticstats', '__about__.py')) as f:
    exec(f.read(), about)


setup(
    name=about['__title__'],
    version=about['__version__'],

    description=about['__summary__'],
    license=about['__license__'],
    url=about['__uri__'],
    author=about['__author__'],
    author_email=about['__email__'],

    packages=find_packages(),
    entry_points={
        'console_scripts': ['elasticstats=elasticstats.main:main'],
    },
    zip_safe=False,
)
