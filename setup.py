from setuptools import setup

setup(
    name='csv2influx',
    version='0.1.dev0',
    description='Write CSV data into InfluxDB thanks to Influx\'s line protocol syntax',
    url='https://github.com/qn7o/csv2influx',
    author='Antonin Bourguignon',
    author_email='antonin.bourguignon@gmail.com',
    license='MIT',
    packages=['csv2influx'],
    install_requires=[
        'docopt',
        'arrow',
        'lineprotocol',
        'requests',
    ],
    entry_points = {
        'console_scripts': [
            'csv2influx = csv2influx:main'
        ],
    },
    zip_safe=False)
