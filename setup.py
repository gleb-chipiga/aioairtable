import re
from pathlib import Path

from setuptools import setup  # type: ignore

path = Path(__file__).parent
txt = (path / 'aioairtable' / '__init__.py').read_text('utf-8')
version = re.findall(r"^__version__ = '([^']+)'\r?$", txt, re.M)[0]
readme = (path / 'README.rst').read_text('utf-8')

setup(
    name='aioairtable',
    version=version,
    description='Asynchronous client library for Airtable API',
    long_description=readme,
    long_description_content_type="text/x-rst",
    url='https://github.com/gleb-chipiga/aioairtable',
    license='MIT',
    author='Gleb Chipiga',
    # author_email='',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Topic :: Internet',
        'Framework :: AsyncIO',
        'Topic :: Office/Business',
        'Topic :: Office/Business :: Financial :: Spreadsheet',
        'Topic :: Office/Business :: Groupware',
        'Topic :: Office/Business :: Scheduling'
    ],
    packages=['aioairtable'],
    package_data={'aioairtable': ['py.typed']},
    python_requires='>=3.8',
    install_requires=['aiohttp', 'multidict', 'yarl', 'backoff',
                      'aiofreqlimit'],
    tests_require=['pytest', 'pytest-asyncio', 'hypothesis', 'attrs']
)
