#!/usr/bin/env python

from pathlib import Path

from .cli import *
from .process import check, sh


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

    repos = {
        'numpy': {
            'ref': numpy,
            'steps': [
                'build',
                'pre-test',
                'test',
            ]
        },
        'scipy': {
            'ref': scipy,
            'steps': [
                'build',
                'pre-test',
                'test',
            ]
        },
        'dask': {
            'ref': dask,
            'steps': [
                'build',
                'pre-test',
                'test',
                'patched',
            ]
        },
    }

    dir = Path(__file__).parent

    prev = None
    for name, repo in repos.items():
        ref = repo['ref']
        tag = ref.rsplit('/',1)[-1]
        repo['tag'] = tag
        gh = name if github == '-' else github
        steps = repo['steps']
        for step in steps:
            repository = f'{name}/{step}'
            full_name = f'{organization}/{repository}:{tag}'
            repo['full_name'] = full_name
            if step == 'build' or step == 'patched':
                if build:
                    dockerfile = dir / name / step / 'Dockerfile'
                    cmd = [
                        'docker', 'build',
                        '-f', dockerfile,
                        '-t', full_name,
                    ]

                    build_args = {
                        'github': gh,
                        'ref': ref,
                        'organization': organization,
                    }

                    if prev: build_args['from'] = prev

                    if kernel: build_args['kernel'] = kernel

                    cmd += [
                        arg
                        for k,v in build_args.items()
                        for arg in [ '--build-arg', f'{k}={v}' ]
                    ]

                    cmd += [ dir.parent ]

                    sh(*cmd)

                if step == 'build':
                    # Record most recent 'build' step for use as base of subsequent containers
                    prev = full_name

                if step == 'patched':
                    if run:
                        container_name = full_name.replace('/','_').replace(':','_')
                        if force:
                            if check('docker','inspect',container_name):
                                sh('docker','rm',container_name)
                        sh('docker','run','--name',container_name,full_name, kernel)
                        if copy:
                            if step == 'sum-crash':
                                result = 'crashed'
                            else:
                                result = 'passed'

                            copy_outputs = dir/'outputs'
                            sh('mkdir','-p',copy_outputs)
                            sh('docker','cp',f'{container_name}:{workdir}/{result}-matrix.ipynb',copy_outputs)
                            sh('docker','cp',f'{container_name}:{workdir}/{result}-sparse.ipynb',copy_outputs)
                if push:
                    sh('docker','push',full_name)

    #
    # for name, config in configs.items():
    #     config['test'] = t or (name in test)
    #
    # for name in input:
    #     repository = name
    #     config = configs[name]
    #     tag = config['tag'].rsplit('/',1)[-1]
    #     full_name = f'{organization}/{repository}:{tag}'
    #     config['full_name'] = full_name
    #     if build:
    #         gh = name if github == '-' else github
    #         cmd = [
    #             'docker', 'build',
    #             '-f', dir / f'Dockerfile.{name}',
    #             '-t', full_name,
    #         ]
    #
    #         base = config.get('from')
    #         if base:
    #             base = configs[base]['full_name']
    #             cmd += [ '--build-arg', f'from={base}' ]
    #
    #         kernel = config.get('kernel')
    #         if kernel: cmd += [ '--build-arg', f'kernel={kernel}' ]
    #
    #         test = '1' if config['test'] else ''
    #         cmd += [
    #             '--build-arg', f'github={gh}',
    #             '--build-arg', f'ref={tag}',
    #             '--build-arg', f'build={test}',
    #             '--build-arg', f'test={test}',
    #             dir.parent,
    #         ]
    #         sh(*cmd)






if __name__ == '__main__':
    main()
