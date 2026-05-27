import os

from setuptools import (
    find_packages,
    setup,
)


def get_requirements():
    basedir = os.path.dirname(__file__)
    try:
        with open(os.path.join(basedir, 'requirements.txt')) as f:
            return f.readlines()
    except FileNotFoundError:
        print(os.listdir(basedir))
        print(os.path.join(basedir, 'requirements.txt'))
        raise RuntimeError('No requirements info found.')


setup(
    name='dbcls',
    version='1.2.18',
    license='BSD',
    author='Maksim Nikitenko',
    author_email='iam@sets88.com',
    packages=find_packages(exclude=("tests",)),
    description='dbcls is a versatile client that supports various databases',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    include_package_data=True,
    package_data={'dbcls': ['weights.json']},
    zip_safe=False,
    platforms='any',
    install_requires=get_requirements(),
    extras_require={
        'cassandra': ['scylla-driver==3.29.9'],
    },
    python_requires='>=3.9',
    url="https://github.com/Sets88/dbcls",
    entry_points={
        'console_scripts': [
            'dbcls = dbcls:main',
        ]
    }
)
