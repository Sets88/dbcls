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
    version='1.1.0',
    license='BSD',
    author='Maksim Nikitenko',
    author_email='iam@sets88.com',
    packages=find_packages(),
    description='dbcls is a versatile client that supports various databases',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=get_requirements(),
    python_requires='>=3.9',
    entry_points={
        'console_scripts': [
            'dbcls = dbcls:main',
        ]
    }
)
