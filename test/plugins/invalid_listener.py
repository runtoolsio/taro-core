def create_execution_listener():
    return InvalidListener()


class InvalidListener:

    def state_update(self):
        """Missing job instance parameter"""
        pass
