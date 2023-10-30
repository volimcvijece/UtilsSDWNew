from setuptools import setup

setup(
    name='UtilsSDW',
    version='0.0.1',
    description='Utility functions for dealing with SDW specific tables (dlookup etc)',
    url='git@github.com:volimcvijece/UtilsSDW.git',
    author='Tonko Caric',
    author_email='caric.tonko@gmail.com',
    license='unlicense',
    packages=['utilssdw'],
    # Needed for dependencies
    install_requires=['pandas'], #no nr - any version. specify - "numpy>=1.13.3"
    zip_safe=False
)