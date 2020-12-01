class UnlimitedResourceBarrier(object):

    def __init__(self):
        pass

    def acquire(self, shard, permits):
        pass

    def tryAcquire(self, shard, permits):
        return True

    def release(self, shard, permits):
        pass
