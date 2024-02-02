from setuptools import setup,find_packages

setup(
    name='utilssdw',
    version='0.0.7',
    description='Utility functions for dealing with SDW specific tables (dlookup etc)',
    url='git@github.com:volimcvijece/UtilsSDW.git',
    author='Tonko Caric',
    author_email='caric.tonko@gmail.com',
    license='unlicense',
    #packages=find_packages(),
    packages=find_packages(where="src"),
    package_dir={'': 'src'},
    # Needed for dependencies
    install_requires=['pandas'], #no nr - any version. specify - "numpy>=1.13.3"
    zip_safe=False
)