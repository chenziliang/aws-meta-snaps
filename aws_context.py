
class AWSContext(object):

    def __init__(self, eventwriter, access_key, secret_key, region):
        self.eventwriter = eventwriter
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
