from distutils.core import setup
import py2exe

setup(
    windows=['Main.py'],
    options={"py2exe": {
    "includes": ["sip", "PyQt4.QtGui", "PyQt4.QtCore", "bs4.*"]}},
    name='Amazon',
    version='1.0',
    packages=['spiders', 'logs', 'utils', 'works', 'views'],
    url='',
    license='',
    author='Rabbi',
    author_email='',
    description=''
)
