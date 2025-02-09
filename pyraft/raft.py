import logging, platform
import time
import random
import log
import threading
import grpc
from concurrent import futures

import raft_pb2_grpc
import raft_pb2
import log_entry

FOLLOWER = 0
CANDIDATE = 1
LEADER = 2


class HostnameFilter(logging.Filter):
    hostname = platform.node()

    def filter(self, record):
        record.hostname = HostnameFilter.hostname
        return True


def _election_timeout():
    return random.randrange(150, 500) / float(1000)


class State(object):

    def __init__(self, my_id, peers, state=FOLLOWER):
        self._srv_inst = None
        self._running = False
        self._votes = 0
        self._stop_heartbeat = False
        self.my_id = my_id
        self.peers = peers
        self.election_timeout = _election_timeout()

        self.leader_id = 0
        if state == LEADER:
            self.leader_id = self.my_id
        self.state = state

        self.current_term = 0
        self.voted_for = 0

        self.log = log.Log()
        self._callback = None

        self.commit_index = 0

        self._heartbeat_timer = None
        self._election_timer = None

        self._start_election_event = threading.Event()
        self._state_change_event = threading.Event()

        self.withhold_votes_until = time.time() + self.election_timeout

        self.__pool = futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix='int_messaging')
        self._lock = threading.RLock()
        self._state_lock = threading.RLock()

        self._create_peers_conn()
        self.__server = RaftServicer(self.peers[self.my_id].address, self)

        handler = logging.StreamHandler()
        handler.addFilter(HostnameFilter())
        handler.setFormatter(logging.Formatter('%(asctime)s %(hostname)s: %(message)s'))

        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def register(self, fn):
        """fn should take a log entry command action and update its state
        """
        self._callback = fn

    def whois_leader(self):
        return self.peers[self.leader_id] if self.leader_id > 0 else None

    def is_leader(self):
        return self.state == LEADER

    def is_candidate(self):
        return self.state == CANDIDATE

    def is_follower(self):
        return self.state == FOLLOWER

    def request_vote_handler(self, candidate_id, term, last_log_index, last_log_term):
        """ Executed by candidates to become leaders
            1. If we already heard from a leader discard the message
            2. If current term is bigger return false
            3. If request term is bigger step down
        """
        with self._state_lock:
            grant_vote = False
            if self.withhold_votes_until > time.time():
                return self.current_term, grant_vote
            self.reset_election_timeout()
            if term > self.current_term:
                self.current_term = term
                logging.debug("step down remote term is bigger")
                self.step_down()
                self.voted_for = 0
                self.leader_id = 0

            if term == self.current_term:
                if self.is_log_ok(last_log_index, last_log_term) and self.voted_for == 0:
                    logging.debug("vote for {}".format(candidate_id))
                    self.step_down()
                    self.reset_election_timeout()
                    self.voted_for = candidate_id
                    grant_vote = True
            return self.current_term, grant_vote

    def append_entries_handler(self, term, leader_id, prev_log_index, prev_log_term, entries, commit_index):
        """ 1.Return if term < currentTerm
            2.If term > currentTerm, currentTerm <- term
            3.If candidate or leader, step down
            4.Reset election timeout
            5.Return failure if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            6.If existing entries conflict with new entries, delete all existing entries starting with first conflicting entry
            7.Append any new entries not already in the log
            8.Advance state machine with newly committed entries
        """
        with self._state_lock:
            if term < self.current_term:
                return self.current_term, False, self.log.last_log_index

            if term > self.current_term:
                self.current_term = term

            self.step_down()

            self.withhold_votes_until = time.time() + self.election_timeout
            self.reset_election_timeout()

            if self.leader_id == 0:
                self.leader_id = leader_id

            if self.log.last_log_index < prev_log_index:
                return self.current_term, False, self.log.last_log_index

            if self.log.start_log_index <= prev_log_index and self.log.get_entry(prev_log_index):
                if self.log.get_entry(prev_log_index).term != prev_log_term:
                    return self.current_term, False, self.log.last_log_index

            self.append_entries_to_log(entries)
            self.commit_index = commit_index

            self.withhold_votes_until = time.time() + self.election_timeout
            self.reset_election_timeout()
            return self.current_term, True, self.log.last_log_index

    def step_down(self):
        if not self.is_follower():
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
            if self._election_timer:
                self.reset_election_timeout()
            self.set_state(FOLLOWER)

    def has_voted_for(self, candidate_id):
        return not self.voted_for or self.voted_for == candidate_id

    def is_log_ok(self, last_log_index, last_log_term):
        return self.log.last_log_index <= last_log_index and self.log.last_log_term <= last_log_term

    def append_entries_to_log(self, log_entries):
        with self._lock:
            for entry in log_entries:
                logging.info("Append entry {}".format(entry))
                self.log.append(entry)
                if self._callback:
                    self._callback(entry)

    def start_election_timeout(self):
        self.election_timeout = _election_timeout()
        self._election_timer = threading.Timer(self.election_timeout, self.trigger_election)
        self._election_timer.start()

    def reset_election_timeout(self):
        if self._election_timer:
            self._election_timer.cancel()
            self._start_election_event.clear()
        self.start_election_timeout()

    def stop_election_timeout(self):
        logging.info("stop election timeout {}".format(self.election_timeout))
        if self._election_timer:
            self._election_timer.cancel()

    def trigger_election(self):
        self._start_election_event.set()

    def has_quorum(self, count):
        return count >= len(self.peers) / 2 + 1

    def start_heartbeat(self):
        self.send_append_entries_message([])
        self._heartbeat_timer = threading.Timer(0.07, self.start_heartbeat)
        self._heartbeat_timer.setName("heartBeatProcess")
        self._heartbeat_timer.start()

    def stop_heartbeat(self):
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    def send_append_entries_message(self, entries):
        results = {}
        for peer_id in self.peers:
            if peer_id == self.my_id:
                continue
            con = self.peers[peer_id].conn
            if con:
                r = self.__pool.submit(
                    con.send_append_entries,
                    self.current_term,
                    self.leader_id,
                    self.log.prev_log_index,
                    self.log.prev_log_term,
                    entries,
                    self.commit_index
                )
                results[r] = peer_id
                try:
                    for result in futures.as_completed(results, 100.0):
                        r = result.result()
                        if r:
                            term, success, last_log_index = r
                            if not success:
                                if term <= self.current_term and self.log.last_log_index > last_log_index:
                                    peer_id = results[result]
                                    missing = self.log.get_from(last_log_index)
                                    logging.info("Send missing entries: [{}] to peer: {}".format(missing.join(", "), peer_id))
                                    self.peers[peer_id].conn.send_append_entries(
                                        self.current_term,
                                        self.leader_id,
                                        last_log_index,
                                        self.log.get_term(last_log_index),
                                        missing,
                                        self.commit_index
                                    )
                except Exception as e:
                    logging.error("got error from remote call {} {}".format(e, result.exception_info()))

    def _remote_execute_request_votes(self, con):
        return con.send_request_vote

    def start_election(self):
        self.current_term = self.current_term + 1
        votes = 1
        self.voted_for = self.my_id
        args = [
            self.my_id,
            self.current_term,
            self.log.last_log_index,
            self.log.last_log_term
        ]
        backoff = 0.05
        while not self._start_election_event.is_set():
            try:
                voted = []
                for pid, result in self._execute_async(self._remote_execute_request_votes, *args):
                    t, v = result
                    if v and t == self.current_term:
                        if pid not in voted:
                            votes = votes + 1
                            logging.debug("Got vote from {}".format(pid))
                            voted.append(pid)
            except Exception as e:
                logging.error("Error getting votes. {}".format(e))

            if self.has_quorum(votes):
                logging.info(
                    "Got {} votes term: {} last log index: {}".format(votes, self.current_term, self.log.last_log_index))
                self.reset_election_timeout()
                self.leader_id = self.my_id
                self.set_state(LEADER)
                return
            time.sleep(backoff)
            backoff = 2 * backoff
        self._start_election_event.clear()

    def _execute_async(self, fn, *args):
        results = {}
        for peer_id in self.peers:
            if peer_id == self.my_id:
                continue
            con = self.peers[peer_id].conn
            if con:
                r = self.__pool.submit(fn(con), *args)
                results[r] = peer_id
            for r in futures.as_completed(results, 10):
                yield results[r], r.result()

    def __print_result(self, result):
        print "result {}".format(result.result())

    def next_state(self, cmd, key, value):
        with self._lock:
            if not self.leader_id == self.my_id:
                logging.error("This node is not leader")
                return False
            entry = log_entry.LogEntryPersist(
                self.current_term,
                self.log.last_log_index,
                cmd,
                key,
                value
            )
            logging.info("Append new entry {}".format(entry))
            self.log.append(entry)
            self.send_append_entries_message([entry])
            if self._callback:
                self._callback(entry)
        return True

    def get_current_leader(self):
        return (self.leader_id, self.peers[self.leader_id].external_address) if self.leader_id > 0 else (
            self.leader_id, "")

    def set_state(self, state):
        self.state = state
        self._state_change_event.set()
        logging.info("trigger change state to {}".format(state))

    def run(self):
        self._srv_inst = self.__server.serve()
        self._srv_inst.start()
        self._running = True
        while self._running:
            while not self._state_change_event.is_set():
                if self.is_follower():
                    logging.info("state change to FOLLOWER start listen for leader events.")
                    self.stop_heartbeat()
                    self.reset_election_timeout()
                    while not self._start_election_event.is_set():
                        self._start_election_event.wait()
                        logging.info("election timeout passed.")
                    self._start_election_event.clear()
                    self.set_state(CANDIDATE)
                elif self.is_candidate():
                    logging.info("state change to CANDIDATE send request votes.")
                    while self.is_candidate():
                        self.stop_heartbeat()
                        self.reset_election_timeout()
                        logging.debug("start new election.")
                        self.start_election()
                elif self.is_leader():
                    self._state_change_event.clear()
                    logging.info("state change to LEADER.")
                    self.stop_election_timeout()
                    self.start_heartbeat()
                    self._state_change_event.wait()
            self._state_change_event.clear()

    def _create_peers_conn(self):
        for pid in self.peers:
            if pid != self.my_id:
                if not self.peers[pid].conn:
                    self.peers[pid].conn = RaftSender(self.peers[pid].address)

    def stop(self):
        self._running = False
        self._srv_inst.stop(0)
        self._state_change_event.set()


class Peer(object):
    def __init__(self, pid, address, external_address):
        self.pid = pid
        self.address = address
        self.external_address = external_address
        self.conn = None

    @property
    def conn(self):
        return self.__conn

    @conn.setter
    def conn(self, conn):
        self.__conn = conn


class RaftServicer(raft_pb2_grpc.raftServicer):
    def __init__(self, address, state):
        self.address = address
        self.state = state

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix='protocol'))
        raft_pb2_grpc.add_raftServicer_to_server(self, server)
        server.add_insecure_port(self.address)
        return server

    def AppendEntries(self, request, context):
        entries = []
        for entry in request.entries:
            entries.append(
                log_entry.LogEntryPersist(
                    entry.term,
                    entry.index,
                    entry.command.operation,
                    entry.command.key,
                    entry.command.value
                )
            )
        current_term, ok, last_log_index = self.state.append_entries_handler(
            request.term,
            request.leaderId,
            request.prevLogIndex,
            request.prevLogTerm,
            entries,
            request.commitIndex
        )
        return raft_pb2.AppendEntriesRes(
            term=current_term,
            success=ok,
            lastLogIndex=last_log_index
        )

    def RequestVote(self, request, context):
        term, vote_granted = self.state.request_vote_handler(
            request.candidateId,
            request.term,
            request.lastLogIndex,
            request.lastLogTerm
        )

        return raft_pb2.RequestVoteRes(
            term=term,
            voteGranted=vote_granted
        )


class RaftSender(object):
    def __init__(self, address):
        self.address = address

    def send_append_entries(self, term,
                            leader_id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            commit_index):
        try:
            with grpc.insecure_channel(target=self.address,
                                       options=[('grpc.enable_retries', False),
                                                ('grpc.keepalive_timeout_ms', 100)]) as channel:

                stub = raft_pb2_grpc.raftStub(channel)

                msg = raft_pb2.AppendEntriesReq(
                    term=term,
                    leaderId=leader_id,
                    prevLogIndex=prev_log_index,
                    prevLogTerm=prev_log_term,
                    commitIndex=commit_index
                )

                entries_ser = [entry.serialize() for entry in entries]
                msg.entries.extend(entries_ser)

                res = stub.AppendEntries(msg)
                try:
                    res.term
                except AttributeError:
                    return None
                return res.term, res.success, res.lastLogIndex
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.UNAVAILABLE:
                logging.error("error sending append entries. {}".format(e.details()))
            return None

    def send_request_vote(self, candidate_id, term, last_log_index, last_log_term):
        try:
            with grpc.insecure_channel(target=self.address,
                                       options=[('grpc.enable_retries', False),
                                                ('grpc.keepalive_timeout_ms', 100)]) as channel:
                stub = raft_pb2_grpc.raftStub(channel)
                res = stub.RequestVote(raft_pb2.RequestVoteReq(
                    candidateId=candidate_id,
                    term=term,
                    lastLogIndex=last_log_index,
                    lastLogTerm=last_log_term
                ))
                try:
                    res.term
                except AttributeError:
                    return 0, None
                return res.term, res.voteGranted
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.UNAVAILABLE:
                logging.error("error sending request vote {}.".format(e.details()))
            return 0, None
