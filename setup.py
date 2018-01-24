import os

from setuptools import setup

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()

setup(
    name='pysheet',
    packages=['pysheet'],
    version='0.3.13',
    description='Read, write and manipulate delimited text files',
    long_description=(read('README.rst')),
    url='https://github.com/isthisthat/Pysheet',
    download_url='https://github.com/isthisthat/Pysheet/archive/0.3.13.tar.gz',
    license='LGPL',
    author='Stathis Kanterakis',
    author_email='me@stathiskanterakis.com',
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=['natsort'],
    entry_points = {
        'console_scripts': [
            'pysheet = pysheet.pysheet:main'
        ]
    }
)
