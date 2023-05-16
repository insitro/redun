from redun import task


@task(executor="conda")
def run_in_conda():
    """
    This task runs inside a docker container with a conda environment.
    """
    # We've installed numpy in our conda environment.
    import numpy as np

    return np.array([1, 2, 3]).sum()


@task
def main():
    """
    This task runs on the host.
    """
    return run_in_conda()
