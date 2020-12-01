from threading import RLock
import logging


class FixedResourceBarrier(object):

    def __init__(self, maxSize):
        self.left = maxSize
        self.logger = logging.getLogger(__name__)
        self.logger.debug("now left %s" % self.left)
        self.lock = RLock()
        self.throttled_count = 0

    def acquire(self, shard, permits):
        with self.lock:
            self.left -= permits
        self.logger.debug("shard : %s resource acquire, now left: %s, permits: %s" % (shard, self.left, permits))

    def tryAcquire(self, shard, permits):
        with self.lock:
            if self.left < permits:
                self.throttled_count += 1
                if self.throttled_count % 200 == 0:
                    self.throttled_count = 0
                    self.logger.debug("shard : %s resource tryAcquire failed, now left: %s, permits: %s " % (shard,
                                                                                                             self.left,
                                                                                                             permits))
                return False

            self.left -= permits
            self.logger.debug(
                "shard : %s resource tryAcquire success, now left: %s, permits: %s" % (shard, self.left, permits))
            return True

    def release(self, shard, permits):
        with self.lock:
            self.left += permits
        self.logger.debug("shard : %s resource release , now left: %s, permits: %s" % (shard, self.left, permits))
