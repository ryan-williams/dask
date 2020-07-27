ARG organization=runsascoded
ARG tag=sum-mat
FROM $organization/dask/install:$tag

# dask/tests/test_config.py::test_collect_yaml_permission_errors fails as root
# because it relies on a certain path being unreadable
RUN useradd -ms /bin/bash user
USER user
WORKDIR /home/user

ENTRYPOINT [ "pytest", "-v", "dask/tests/test_config.py::test_collect_yaml_permission_errors" ]
