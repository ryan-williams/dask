ARG organization=runsascoded
ARG tag=sumfix
FROM $organization/numpy/install:$tag

RUN pip install pytest
ENTRYPOINT [ "python", "runtests.py" ]
