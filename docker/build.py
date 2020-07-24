#!/usr/bin/env python

from pathlib import Path
from subprocess import check_call, CalledProcessError, DEVNULL

from .cli import *


dir = Path(__file__).parent


def sh(*args, **kwargs):
    print(f'Running: {args}')
    check_call(args, **kwargs)


def check(*args, **kwargs):
    try:
        sh(*args, stdout=DEVNULL, stderr=DEVNULL, **kwargs)
        return True
    except CalledProcessError:
        return False


@cmd()
@flag('-b','--build',default=True, help='Build containers')
@flag('-p','--push', help='Push containers')
@flag('-r','--run', help='Run containers')
@flag('-f','--force', help='When running containers, first check that their name is free, and rm any existing container if not (has no effect if -r/--run not set)')
@flag('-c','--copy', help='Copy result files out of run containers (has no effect if -r/--run not set)')
@opt('-o', '--organization', default='runsascoded', help='Docker organization to use in image tags')
@opt('-v', '--dask_version', default='2.16.0', help='Dask version to build containers against')
@opt('-w', '--workdir', default='/home/user/dask', help='Workdir in run containers to copy files out from')
@paths('-i', '--input', '--dockerfile', default=['clone', 'test', 'sum-crash', 'patched'], help='Suffixes of Dockerfiles to operate on')
def main(build, push, run, force, copy, organization, dask_version, workdir, input):
    for name in input:
        repository=f'dask-{name}'
        tag = f'{organization}/{repository}:{dask_version}'
        if build:
            sh(
                'docker', 'build',
                '-f', str(dir / f'Dockerfile.{name}'),
                '-t', tag,
                '--build-arg', f'dask_version={dask_version}',
                str(dir.parent),
            )
        if name in ['sum-crash','patched']:
            if run:
                container_name = f'{repository}_{dask_version}'
                if force:
                    if check('docker','inspect',container_name):
                        sh('docker','rm',container_name)
                sh('docker','run','--name',container_name,tag)
                if copy:
                    if name == 'sum-crash':
                        result = 'crashed'
                    else:
                        result = 'passed'

                    copy_outputs = 'docker/outputs/'
                    sh('mkdir','-p',copy_outputs)
                    sh('docker','cp',f'{container_name}:{workdir}/{result}-matrix.ipynb',copy_outputs)
                    sh('docker','cp',f'{container_name}:{workdir}/{result}-sparse.ipynb',copy_outputs)
        if push:
            sh('docker','push',tag)


if __name__ == '__main__':
    main()
