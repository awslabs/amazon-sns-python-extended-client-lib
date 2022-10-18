import io

from setuptools import setup

setup(
    name='sns-extended-client',
    version='0.0.0',
    description='AWS SNS extended client functionality from amazon-sns-java-extended-client-lib',
    author='',
    author_email='',
    url='https://github.com/',
    license='Apache 2.0',
    long_description=io.open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    packages=['sns_extended_client'],
    package_dir={'sns_extended_client': 'src/sns_extended_client'},
    install_requires = ['botoinator'],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
    ],
)
