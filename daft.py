class Server:
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.state = 'follower'
        self.replicated_log = ReplicatedLog(self)

    def append_entries(self, leader_term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index):
        self._check_if_behind(leader_term)
        if leader_term < self.current_term:
            return False
        if self.replicated_log[pref_log_index].term != prev_log_term:
            return False
        index = prev_log_index
        for entry in entries:
            if entry is Heartbeat:
                continue
            index += 1
            existing_entry = self.replicated_log[index]
            if existing_entry is not None:
                if existing_entry.term == entry.term:
                    continue
                self.replicated_log.clear_from(index)
            self.replicated_log.append_entry(entry)
        self.replicated_log.update_commit_index(leader_commit_index)
        return True

    def request_vote(self, candidate_term, candidate_id, last_log_index, last_log_term):
        self._check_if_behind(candidate_term)
        if candidate_term < self.current_term:
            return (self.current_term, False)
        if self.voted_for is None or self.voted_for == candidate_id:
            if last_log_index >= self.replicated_log.commit_index:
                return (self.current_term, True)
        return (self.current_term, False)

    def _check_if_behind(self, term):
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
        if self.state == 'leader':
            raise DoNotTellMeWhatToDoError()

    def process_entry(self, entry):
        pass


class Heartbeat:
    pass


class DoNotTellMeWhatToDoError(Exception):
    pass


class ReplicatedLog:
    def __init__(self, state_machine):
        self.log = []
        self.commit_index = 0
        self.applied_index = 0
        self.state_machine = state_machine

    def append_entry(self, entry):
        self.log.append(entry)

    def clear_from(self, index):
        self.log = self.log[:index]

    def __getitem__(self, index):
        return self.log[index]

    def __setitem__(self, index, value):
        self.log[index] = value

    def update_commit_index(self, leader_commit_index):
        self.commit_index = min(leader_commit_index, len(self.log))

    def process_entries(self):
        while True:
            index = self.applied_index
            if index >= self.commit_index:
                break
            entry = self.log[index]
            self.state_machine.process_entry(entry)
            self.applied_index += 1


def main():
    server = Server()


if __name__ == '__main__':
    main()
