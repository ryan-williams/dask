ARG organization=runsascoded
ARG tag=sum-mat
FROM $organization/dask/build:$tag

RUN pip install papermill ipykernel jupyter
ARG kernel=3.7.7
RUN python -m ipykernel install --user --name $kernel
ADD sum-test-matrix.ipynb sum-test-sparse.ipynb ./
ADD sum-patched.sh ./
ENTRYPOINT [ "./sum-patched.sh" ]
