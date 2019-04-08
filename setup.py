import setuptools
import sys
import re
import os

if sys.version_info < (3, 6):
    print('gurglefish requires at least Python 3.6 to run.')
    sys.exit(1)

with open(os.path.join('gurglefish', '__init__.py'), encoding='utf-8') as f:
    version = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M).group(1)

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='gurglefish',
    version=version,
    python_requires='>=3.6',
    author='Marshall L Smith Jr',
    author_email='marshallsmithjr@gmail.com',
    description='Sync and maintain Salesforce sobjects in a Postgres database',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url='https://github.com/mlsmithjr/gurglefish',
    include_package_data=True,
#    data_files=[('share/doc/gurglefish', ['README.md', 'LICENSE' ]), ('config', ['gurglefish/logging.yml'])],
    packages=setuptools.find_packages(),
    install_requires=['requests==2.21.0', 'psycopg2-binary==2.8', 'fastcache==1.0.2', 'arrow==0.13.1', 'python-dateutil==2.8.0', 'pyyaml==5.1'],
    entry_points={"console_scripts": ["gurglefish=gurglefish.sfarchive:main"]},
    classifiers=[
      'Programming Language :: Python :: 3',
      'Environment :: Console',
      'Topic :: Database',
      'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
      'Intended Audience :: System Administrators',
      'Natural Language :: English',
      'Operating System :: POSIX :: Linux',
      'Operating System :: MacOS :: MacOS X',
      'Operating System :: Microsoft :: Windows :: Windows 10',
    ],
    keywords='salesforce sobject database synchronization snapshots postgres postgresql',
)

