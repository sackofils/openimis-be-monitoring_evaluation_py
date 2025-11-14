import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='openimis-be-monitoring_evaluation',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    url='https://openimis.org/',
    author='Sekou SACKO',
    author_email='sekou.sacko@yahoo.fr',
    license='GNU AGPL v3',
    description='The openIMIS Backend Monitoring evaluation module.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'django',
        'django-db-signals',
        'djangorestframework',
        'openimis-be-core',
        'openimis-be-individual'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Framework :: Django :: 2.1',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
