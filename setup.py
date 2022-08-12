import os

from setuptools import find_packages, setup

_HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(_HERE, 'README.md'), 'r') as f:
    long_desc = f.read()

setup(
    name='s1bursts',
    use_scm_version=True,
    description='A python package for utilizing Sentinel-1 burst SLCs',
    long_description=long_desc,
    long_description_content_type='text/markdown',

    url='https://github.com/forrestfwilliams/burst_workflow',

    author='ASF APD/Tools Team',
    author_email='uaf-asf-apd@alaska.edu',

    license='BSD',
    include_package_data=True,

    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],

    python_requires='~=3.8',
    #TODO
    install_requires=[],

    extras_require={
        'develop': [
            'pytest',
            'pytest-cov',
            'responses',
        ]
    },

    packages=find_packages(),

    zip_safe=False,
)