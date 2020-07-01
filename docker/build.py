#!/usr/bin/env python

from pathlib import Path
from subprocess import check_call, CalledProcessError, DEVNULL

from .cli import *


dir = Path(__file__).parent


def sh(*args, **kwargs):
    cmd = [ str(arg) for arg in args if arg is not None ]
    print(f'Running: {cmd}')
    check_call(cmd, **kwargs)


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
@flag('-t', help='Run tests while building containers')
@paths('--test', default=[], help='Images to run tests in')
@flag('-f','--force', help='When running containers, first check that their name is free, and rm any existing container if not (has no effect if -r/--run not set)')
@flag('-c','--copy', help='Copy result files out of run containers (has no effect if -r/--run not set)')
@opt('-k', '--kernel', default='3.7.7', help='Python version / Jupyter kernel to run patched notebook with')
@opt('-o', '--organization', default='runsascoded', help='Docker organization to use in image tags')
@opt('--github', default='celsiustx', help='GitHub organization to clone repos from ("-" to use main upstream numpy/scipy/dask repos)')
@opt('--numpy', default='origin/keepdims', help='Numpy ref to checkout and build')
@opt('--scipy', default='origin/keepdims', help='Scipy ref to checkout and build')
@opt('--dask', default='2.19.0', help='Dask ref to checkout and build')
@opt('--patched', default='dask-sum-patched', help='Tag for patched image')
@opt('-w', '--workdir', default='/opt/src/dask', help='Workdir in run containers to copy files out from')
@paths('-i', '--input', '--dockerfile', default=['numpy', 'scipy', 'dask', 'patched'], help='Suffixes of Dockerfiles to operate on')
def main(build, push, run, t, test, force, copy, kernel, organization, github, numpy, scipy, dask, patched, workdir, input):
    configs = {
        'numpy': { 'tag':    numpy, },
        'scipy': { 'tag':    scipy, 'from': 'numpy' },
         'dask': { 'tag':     dask, 'from': 'scipy' },
      'patched': { 'tag':  patched, 'from':  'dask', 'kernel': kernel, },
    }
    for name, config in configs.items():
        config['test'] = t or (name in test)

    for name in input:
        repository = name
        config = configs[name]
        tag = config['tag'].rsplit('/',1)[-1]
        full_name = f'{organization}/{repository}:{tag}'
        config['full_name'] = full_name
        if build:
            gh = name if github == '-' else github
            cmd = [
                'docker', 'build',
                '-f', dir / f'Dockerfile.{name}',
                '-t', full_name,
            ]

            base = config.get('from')
            if base:
                base = configs[base]['full_name']
                cmd += [ '--build-arg', f'from={base}' ]

            kernel = config.get('kernel')
            if kernel: cmd += [ '--build-arg', f'kernel={kernel}' ]

            test = '1' if config['test'] else ''
            cmd += [
                '--build-arg', f'github={gh}',
                '--build-arg', f'ref={tag}',
                '--build-arg', f'build={test}',
                '--build-arg', f'test={test}',
                dir.parent,
            ]
            sh(*cmd)

        if name in ['sum-crash','patched']:
            if run:
                container_name = f'{repository}_{tag}'
                if force:
                    if check('docker','inspect',container_name):
                        sh('docker','rm',container_name)
                sh('docker','run','--name',container_name,full_name, kernel)
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
            sh('docker','push',full_name)


if __name__ == '__main__':
    main()
