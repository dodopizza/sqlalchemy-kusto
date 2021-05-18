def check_closed(func):
    """Decorator that checks if connection/cursor is closed."""

    def decorator(self, *args, **kwargs):
        if self.closed:
            raise Exception("{klass} already closed".format(klass=self.__class__.__name__))
        return func(self, *args, **kwargs)

    return decorator


def check_result(func):
    """Decorator that checks if the cursor has results from `execute`."""

    def decorator(self, *args, **kwargs):
        if self._results is None:  # pylint: disable=protected-access
            raise Exception("Called before `execute`")
        return func(self, *args, **kwargs)

    return decorator
