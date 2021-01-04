from setuptools import setup, find_namespace_packages

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

packages = find_namespace_packages(include=('ajenga.protocol.*', ))

setup(
    name='ajenga-protocol',
    version='0.9.8',
    url='https://github.com/project-ajenga/ajenga',
    license='MIT License',
    author='Hieuzest',
    author_email='girkirin@hotmail.com',
    description='An asynchronous QQ bot framework.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=packages,
    package_data={
        '': ['*.pyi'],
    },
    install_requires=['ajenga', 'quart', 'aiocqhttp'],
    extras_require={
        'scheduler': ['apscheduler'],
    },
    python_requires='>=3.7',
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Framework :: Robot Framework',
        'Framework :: Robot Framework :: Library',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],
)
