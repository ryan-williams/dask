ARG organization=runsascoded
ARG tag=sumfix
FROM $organization/numpy/pre-test:$tag

RUN python runtests.py -b
RUN python runtests.py
