import logging
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


def _election_timeout():
    return random.randrange(150, 500) / float(1000)


class State(object):

    def __init__(self, my_id, peers, state=FOLLOWER):
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
        logging_format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=logging_format, level=logging.INFO,
                            datefmt="%H:%M:%S")

    def register(self, fn):
        """fn should take a log entry command action and update its state
        """
        self._callback = fn

    def whois_leader(self):
        return self.peers[self.leader_id] if self.leader_id > 0 else None

    def is_leader(self):
        with self._state_lock:
            return self.state == LEADER

    def is_candidate(self):
        with self._state_lock:
            return self.state == CANDIDATE

    def is_follower(self):
        with self._state_lock:
            return self.state == FOLLOWER

    def request_vote_handler(self, candidate_id, term, last_log_index, last_log_term):
        """ Executed by candidates to become leaders
            1. If we already heard from a leader discard the message
            2. If current term is bigger return false
            3. If request term is bigger step down
        """
        grant_vote = False
        if self.withhold_votes_until > time.time():
            return self.current_term, grant_vote
        self.reset_election_timeout()
        if term > self.current_term:
            self.current_term = term
            self.step_down()
            self.voted_for = 0
            self.leader_id = 0

        if term == self.current_term:
            if self.is_log_ok(last_log_index, last_log_term) and self.voted_for == 0:
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
        if self.is_candidate() or self.is_leader():
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
        logging.info("trigger election")
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
                    for result in futures.as_completed(results, 10.5):
                        r = result.result()
                        if r:
                            term, success, last_log_index = r
                            if term <= self.current_term and self.log.last_log_index > last_log_index:
                                peer_id = results[r]
                                self.peers[peer_id].con.send_append_entries(
                                    self.current_term,
                                    self.leader_id,
                                    last_log_index,
                                    self.log.get_term(last_log_index),
                                    self.log.get_from(last_log_index),
                                    self.commit_index
                                )
                except Exception as e:
                    logging.error("got error {}".format(e))

    def start_election(self):
        voting = {}
        self.current_term = self.current_term + 1
        for peer_id in self.peers:
            if peer_id == self.my_id:
                continue
            con = self.peers[peer_id].conn
            if con:
                r = self.__pool.submit(
                    con.send_request_vote,
                    self.my_id,
                    self.current_term,
                    self.log.last_log_index,
                    self.log.last_log_term
                )
                voting[r] = peer_id

        votes = 1
        try:
            for vote in futures.as_completed(voting, 10):
                if self._start_election_event.is_set():
                    self._start_election_event.clear()
                    self.step_down()
                    for voted in voting:
                        voted.cancel()
                    return
                _, v = vote.result()
                if v:
                    votes = votes + 1
        except Exception as e:
            print("".format(e))

        if self.has_quorum(votes):
            self.reset_election_timeout()
            self.set_state(LEADER)
        else:
            self.step_down()

    def __print_result(self, result):
        print "result {}".format(result.result())

    def next_state(self, cmd, key, value):
        succ = False
        with self._lock:
            if not self.leader_id == self.my_id:
                logging.error("This node is not leader")
                succ = False
            entry = log_entry.LogEntryPersist(
                self.current_term,
                self.log.last_log_index,
                cmd,
                key,
                value
            )
            self.log.append(entry)
            self.send_append_entries_message([entry])
            if self._callback:
                self._callback(entry)
            succ = True
        return succ

    def get_current_leader(self):
        return self.leader_id, self.peers[self.leader_id].external_address if self.leader_id > 0 else self.leader_id, 0

    def set_state(self, state):
        with self._state_lock:
            self.state = state
        self._state_change_event.set()

    def run(self):
        hb = False
        server = self.__server.serve()
        threading.Thread(target=server.start).start()
        while True:
            while not self._state_change_event.is_set():
                if self.is_follower():
                    logging.info("state change to FOLLOWER start listen for leader events.")
                    self.stop_heartbeat()
                    self.reset_election_timeout()
                    while not self._start_election_event.is_set():
                        self._start_election_event.wait()
                        print "election timeout passed"
                    self._start_election_event.clear()
                    self.set_state(CANDIDATE)
                elif self.is_candidate():
                    logging.info("state change to CANDIDATE send request votes.")
                    self.stop_heartbeat()
                    self.reset_election_timeout()
                    self.start_election()
                elif self.is_leader():
                    logging.info("state change to LEADER.")
                    self.stop_election_timeout()
                    self.start_heartbeat()
                    self._state_change_event.wait()
                    self.stop_heartbeat()
            self._state_change_event.clear()

    def _create_peers_conn(self):
        for pid in self.peers:
            if pid != self.my_id:
                if not self.peers[pid].conn:
                    self.peers[pid].conn = RaftSender(self.peers[pid].address)


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
                    entry.command.cmd,
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
            logging.error("error sending append entries.", e)
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
            logging.error("error sending request vote.", e)
            return 0, None
