# Test+Install Dask

ARG PYTHON_VERSION=3.7
FROM python:$PYTHON_VERSION

ENV DEBIAN_FRONTEND noninteractive
ENV TZ=America/New_York

RUN mkdir -p /opt/src
WORKDIR /opt/src

# Disable pip version check
SHELL ["/bin/bash", "-c"]
RUN echo $'[global]\n\
disable-pip-version-check = True' > /etc/pip.conf

RUN mkdir -p /opt/src
WORKDIR /opt/src

# Clone
RUN git clone --recurse-submodules https://github.com/celsiustx/dask.git
WORKDIR dask
RUN git remote add -f upstream https://github.com/dask/dask.git

# Checkout
ARG REF=origin/ctx
RUN git checkout $REF

# Install dask
RUN apt-get update && apt-get install -y graphviz
# Base deps, test deps, extra deps
RUN pip install -e .[complete] \
 && pip install pytest \
 && pip install \
    fastavro fastparquet \
    h5py \
    pyarrow \
    python-snappy \
    requests \
    s3fs \
    scikit-image scipy \
    sparse \
    tiledb xarray zarr


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
