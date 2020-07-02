ARG organization=runsascoded
ARG tag=keepdims
FROM $organization/scipy/pre-test:$tag

RUN python runtests.py -b
RUN python runtests.py
