try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name = "iptools",
    version = "0.1",
    author = "Monte Lunacek",
    author_email = "monte.lunacek@colorado.edu",
    description = ("Tools for helping launch ipython"),
    license = "BSD",
    keywords = "IPython Parallel tools",
    packages=['iptools'],
    long_description=open('README.md').read(),
    scripts=['scripts/iptools'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ]
)
