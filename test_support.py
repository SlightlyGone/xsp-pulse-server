class SessionDouble:
    """Offline Session lifecycle double; real SDK refresh tests use MockTransport."""

    def __init__(self):
        self.open = False

    async def __aenter__(self):
        self.open = True
        return self

    async def __aexit__(self, *args):
        self.open = False
