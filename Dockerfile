# Test+Install Dask

## Upstream scipy image
ARG BASE=celsiustx/base
FROM $BASE

WORKDIR /opt/src

# Clone
RUN git clone --recurse-submodules https://github.com/celsiustx/dask.git
WORKDIR dask
RUN git remote add -f upstream https://github.com/dask/dask.git

# Checkout
ARG REF=origin/ctx
RUN git checkout $REF

# Install
RUN apt-get update && apt-get install -y graphviz
RUN pip install scipy \
 && pip install -e .[complete]

# Test
RUN pip install pytest

## Run tests as a non-root user (dask/tests/test_config.py::test_collect_yaml_permission_errors fails when run as root!)
RUN useradd user -g root
RUN chmod g+w /opt/src/dask  # some distributed tests write temporary data under the source tree
USER user

# Canary test; prone to failure if linalg (in numpy/scipy) isn't compiled/available as expected
RUN pytest -v dask/array/tests/test_linalg.py::test_inv

# Run all tests
RUN pytest -v

USER root
WORKDIR /
