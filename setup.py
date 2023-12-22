from setuptools import setup, find_packages

setup(
    name='max_nhl_scraper',
    version='0.1.4',
    author='Max Tixador',
    author_email='maxtixador@gmail.com',
    packages=find_packages(),
    description='A package for scraping NHL data',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'beautifulsoup4==4.12.2',
    'numpy==1.26.2',
    'pandas==2.1.4',
    'Requests==2.31.0', # BeautifulSoup should be specified as beautifulsoup4
    ],
    python_requires='>=3.6',
    include_package_data=True,
    classifiers=[
        # Classifiers help users find your project
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
