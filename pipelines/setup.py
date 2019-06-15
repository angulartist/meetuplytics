import setuptools

REQUIRED_PACKAGES = []

setuptools.setup(
        name='meetuplytics',
        version='0.0.1',
        description='Meetup\'s RSVPs analytics demo.',
        install_requires=REQUIRED_PACKAGES,
        packages=setuptools.find_packages()
)
