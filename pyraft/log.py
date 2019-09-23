class Log(object):
    def __init__(self):
        self.last_log_term = 0
        self.start_log_index = 0
        self.prev_log_index = 0
        self.last_log_index = 0
        self.commit_index = 0
        self.current_term = 0
        self.prev_log_term = 0
        self.log = []

    def append(self, log_entry):
        if log_entry.term > self.current_term:
            self.prev_log_term = self.current_term
            self.current_term = log_entry.term
        self.log[self.last_log_index] = log_entry
        self.prev_log_index = self.last_log_index
        self.last_log_index += 1

    def get_entry(self, index):
        return self.log[index] if index <= len(self.log) - 1 else LogEntry()


class LogEntry(object):
    pass
