from setuptools import setup, find_packages


setup(
    name="emap_runner",
    version="0.1.0",
    packages=find_packages("."),
    author="Sarah Keating, Tom Young",
    url="https://github.com/inform-health-informatics/emap-setup",
    entry_points={"console_scripts": ["emap = emap_runner.runner:main"]},
    description="Setup, updating and docker orchestration of EMAP",
    platforms=["linux", "macos", "windows"],
)
