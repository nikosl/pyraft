import raft_pb2


class LogEntryPersist(object):
    def __init__(self, term, index, cmd, key, value):
        self.term = term
        self.index = index

        self.cmd = cmd
        self.key = key
        self.value = value
        self.is_empty = False

    def serialize(self):
        return raft_pb2.LogEntry(
            term=self.term,
            index=self.index,
            command=raft_pb2.Command(
                operation=self.cmd,
                key=self.key,
                value=self.value
            )
        )
