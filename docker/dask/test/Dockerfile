ARG organization=runsascoded
ARG tag=sum-mat
FROM $organization/dask/pre-test:$tag

RUN pytest -v dask/tests/test_config.py::test_collect_yaml_permission_errors
