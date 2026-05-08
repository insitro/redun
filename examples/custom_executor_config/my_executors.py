from redun.executors.local import LocalExecutor


class MyLocalExecutor(LocalExecutor):
    """
    Example custom executor that extends LocalExecutor.

    In a real project, this might live in a shared library package
    (e.g., mycompany.executors.AzureMLExecutor).
    """

    def __init__(self, name, scheduler=None, config=None):
        super().__init__(name, scheduler=scheduler, config=config)
        self.log_prefix = config.get("log_prefix", "custom") if config else "custom"
