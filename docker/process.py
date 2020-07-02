from subprocess import check_call, CalledProcessError, DEVNULL


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
