# For plugin testing in test_plugin.py
def create_listener():
    return Listener()


class Listener:

    def notify(self, job_instance):
        pass
