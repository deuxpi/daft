import asyncio
import collections
import logging
import pathlib
import random
import uuid

import aiohttp.web


class Server:
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.state = 'follower'

        self._id = uuid.uuid4()
        self._followers = {}
        self._log = ReplicatedLog(self)
        self._election_timeout = random.uniform(0.150, 0.300)

        self._wait_for_append_entries = None

        self._logger = logging.getLogger(str(self._id))

        self._http_server = aiohttp.web.Server(self.http_handler)

    async def run(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        server = await loop.create_server(self._http_server, '127.0.0.1', random.randrange(8080, 8180))
        self._logger.info("Serving on http://%s:%d", *server.sockets[0].getsockname())
        try:
            while True:
                if self.state == 'follower':
                    await self.start_follower()
                elif self.state == 'candidate':
                    await self.start_candidate()
                elif self.state == 'leader':
                    await self.start_leader()
                else:
                    raise RuntimeError('Invalid server state %s', self.state)
        except Exception as e:
            self._logger.error('Exception: %s', e)
            raise
        finally:
            server.close()

    async def start_follower(self):
        if self._wait_for_append_entries is not None:
            raise RuntimeError('start_follower called while already waiting for AppendEntries RPC')

        try:
            while True:
                self._wait_for_append_entries = asyncio.Future()
                await asyncio.wait_for(self._wait_for_append_entries, self._election_timeout)
        except asyncio.TimeoutError:
            self.state = 'candidate'
        finally:
            self._wait_for_append_entries = None

    async def start_candidate(self):
        self._start_election()
        try:
            votes = await asyncio.wait_for(self._send_request_vote(), self._election_timeout)
            if votes >= 3:
                self._logger.info("Obtained majority of votes. Converting to leader.")
                self.state = 'leader'
            else:
                self._logger.info("Received AppendEntries RPC from new leader. Converting to follower.")
                self.state = 'follower'
        except asyncio.TimeoutError:
            self._logger.info("Election timeout: starting new election")

    async def start_leader(self):
        while True:
            self._send_append_entries()
            if True:  # yolo
                n = self._log.commit_index + 1
                self._log.commit_index = n
            await asyncio.sleep(0.05)

    async def http_handler(self, request):
        path = pathlib.PurePosixPath(request.path)
        if len(path.parts) != 2:
            return aiohttp.web.Response(status=404)
        dispatch = path.parts[1]

        try:
            meth = getattr(self, 'handle_{}'.format(dispatch))
        except AttributeError:
            return aiohttp.web.Response(status=404)
        json_body = await request.json()
        status = meth(**json_body)
        return aiohttp.web.json_response({'term': self.current_term, 'status': status})

    def receive_command(self, command):
        if self.state != 'leader':
            self._logger.error("Received command without being leader.")
            raise WhatAreYouExpectingError()
        self._log.append_entry(Entry(self.current_term, command))

    def handle_append_entries(self, **kwargs):
        leader_term = int(kwargs['term'])
        prev_log_index = int(kwargs['prevLogIndex'])
        prev_log_term = int(kwargs['prevLogTerm'])
        entries = [Entry(int(entry['term']), entry['command']) for entry in kwargs['entries']]
        leader_commit_index = int(kwargs['leaderCommitIndex'])

        self._check_if_behind(leader_term)
        if leader_term < self.current_term:
            self._logger.debug("Received AppendEntries RPC from old leader.")
            return self._response(False)
        if self._log[prev_log_index].term != prev_log_term:
            return self._response(False)
        index = prev_log_index
        if not entries:
            self._logger.debug("Received heartbeat.")
        else:
            for entry in entries:
                index += 1
                existing_entry = self._log[index]
                if existing_entry is not None:
                    if existing_entry.term == entry.term:
                        continue
                    self._logger.warning("Found log inconsistency. Overwriting conflicting entries.")
                    self._log.clear_from(index)
                self._log.append_entry(entry)
        self._log.update_commit_index(leader_commit_index)
        return self._response(True)

    def handle_request_vote(self, candidate_term, candidate_id, last_log_index, last_log_term):
        self._check_if_behind(candidate_term)
        if candidate_term < self.current_term:
            return self._response(False)
        if self.voted_for is None or self.voted_for == candidate_id:
            if last_log_index >= self._log.commit_index:
                return (self.current_term, True)
        return self._response(False)

    def _check_if_behind(self, term):
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
        if self.state == 'leader':
            self._logger.error("Received RPC while being leader.")
            raise DoNotTellMeWhatToDoError()

    def _start_election(self):
        self.current_term += 1
        self.voted_for = None
        self._election_timeout = random.uniform(0.150, 0.300)
        self._logger.info("Starting election %d (waiting for %.3f)", self.current_term, self._election_timeout)

    async def _send_request_vote(self):
        await asyncio.sleep(random.uniform(0.0, 0.25))
        return random.randrange(5)

    def _send_append_entries(self):
        last_log_index = self._log.commit_index
        for follower, next_index in self._followers.items():
            if last_log_index >= next_index:
                while True:
                    term, success = follower.append_entries(self.current_term, self._id, and_so_on)
                    if success:
                        self._followers[follower] = last_log_index
                        break
                    else:
                        next_index -= 1

    def process_command(self, command):
        raise NotImplementedError('Server subclasses should implement process_command')


Entry = collections.namedtuple('Entry', ['term', 'command'])


class DoNotTellMeWhatToDoError(Exception):
    pass


class WhatAreYouExpectingError(Exception):
    pass


class ReplicatedLog:
    def __init__(self, state_machine):
        self.log = []
        self.commit_index = 0
        self.applied_index = 0
        self._state_machine = state_machine

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
            self._state_machine.process_command(entry.command)
            self.applied_index += 1


class LoggingServer(Server):
    def process_command(self, command):
        self._logger.info("Received: {}".format(command))


def main():
    try:
        import coloredlogs
        coloredlogs.install(
            level='DEBUG',
            fmt='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
            milliseconds=True)
    except ImportError:
        pass

    loop = asyncio.get_event_loop()

    for i in range(5):
        server = LoggingServer()
        asyncio.ensure_future(server.run(loop))
    loop.run_forever()


if __name__ == '__main__':
    main()
