def create_listener():
    return InvalidListener()


class InvalidListener:

    def notify(self):
        """Missing job instance parameter"""
        pass
