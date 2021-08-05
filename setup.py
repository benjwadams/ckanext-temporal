from setuptools import setup


setup(
     name='ckanext-temporal',
     version='0.0.1',
     description="Temporal filtering extension for CKAN",
)

entry_points="""
    [ckan.plugins]
    temporal=ckanext.temporal.plugin:TemporalPlugin
"""

install_requires=[
    'pendulum>=2.0.3',
],
