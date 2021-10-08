from setuptools import setup

setup(
  name='rclone-pathmap',
  version='0.1.2',
  py_modules=['rclone_pathmap'],
  url='https://github.com/maayanLab/rclone-pathmap/',
  author='Daniel J. B. Clarke',
  author_email='danieljbclarkemssm@gmail.com',
  long_description=open('README.md', 'r').read(),
  long_description_content_type='text/markdown; charset=UTF-8',
  license='Apache-2.0',
  classifiers=[
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3",
    "Topic :: Scientific/Engineering",
    "Topic :: Software Development :: Libraries :: Application Frameworks",
    "Framework :: Jupyter",
  ],
  python_requires='>=3.7.0',
  install_requires=list(map(str.strip, open('requirements.txt', 'r').readlines())),
  entry_points={
    'console_scripts': ['rclone-pathmap=rclone_pathmap:cli'],
  },
)
