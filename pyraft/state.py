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


def _current_milli_time():
    return round(time.time() * 1000)


class State(object):

    def __init__(self, my_id, peers):
        self._stop_heartbeat = False
        self.my_id = my_id
        self.peers = peers
        self.election_timeout = random.randrange(150, 500) / float(1000)
        self.leader_id = -1

        self.state = FOLLOWER

        self.current_term = 0
        self.voted_for = -1

        self.log = log.Log()
        self._callback = None

        self.commit_index = 0

        self._heartbeat_timer = None
        self._election_timer = None

        self._start_election_event = threading.Event()

        self.withhold_votes_until = _current_milli_time() + self.election_timeout

        self.__pool = futures.ThreadPoolExecutor(max_workers=10)
        self._lock = threading.Lock()

    def whois_leader(self):
        return self.peers[self.leader_id] if self.leader_id >= 0 else None

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
        grant_vote = False
        if self.withhold_votes_until > _current_milli_time():
            return self.current_term, grant_vote

        if term > self.current_term:
            self.current_term = term
            self.step_down()
            self.voted_for = -1
            self.leader_id = -1

        if term == self.current_term:
            if self.is_log_ok(last_log_index, last_log_term) and self.voted_for == -1:
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

        self.withhold_votes_until = _current_milli_time() + self.election_timeout
        self.reset_election_timeout()

        if self.leader_id == -1:
            self.leader_id = leader_id

        if self.log.last_log_index < prev_log_index:
            return self.current_term, False, self.log.last_log_index

        if (self.log.start_log_index <= prev_log_index and
                self.log.get_entry(prev_log_index).term != prev_log_term):
            return self.current_term, False, self.log.last_log_index

        self.append_entries_to_log(entries)
        self.commit_index = commit_index

        self.withhold_votes_until = _current_milli_time() + self.election_timeout
        self.reset_election_timeout()

    def step_down(self):
        if self.is_candidate() or self.is_leader():
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
            if self._election_timer:
                self.reset_election_timeout()
            self.state = FOLLOWER

    def has_voted_for(self, candidate_id):
        return not self.voted_for or self.voted_for == candidate_id

    def is_log_ok(self, last_log_index, last_log_term):
        return self.log.last_log_index <= last_log_index and self.log.last_log_term <= last_log_term

    def append_entries_to_log(self, log_entries):
        with self._lock:
            for entry in log_entries:
                self.log.append(entry)

    def start_election_timeout(self):
        self._election_timer = threading.Timer(self.election_timeout, self.trigger_election)
        self._election_timer.start()

    def reset_election_timeout(self):
        print "election timeout is {}".format(self.election_timeout)
        if self._election_timer:
            self._election_timer.cancel()
            self.start_election_timeout()

    def stop_election_timeout(self):
        print "stop election timeout {}".format(self.election_timeout)
        if self._election_timer:
            self._election_timer.cancel()

    def trigger_election(self):
        print "trigger election"
        self._start_election_event.set()

    def has_quorum(self, count):
        return count >= len(self.peers) / 2 + 1

    def start_heartbeat(self):
        self.send_heartbeat()
        self._heartbeat_timer = threading.Timer(0.07, self.start_heartbeat)
        self._heartbeat_timer.start()

    def stop_heartbeat(self):
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    def send_heartbeat(self):
        self._create_peers_conn()
        print "send heartbeat"
        results = {}
        for peer_id in self.peers:
            r = self.__pool.submit(
                self.peers[peer_id].conn.send_append_entries,
                self.current_term,
                self.leader_id,
                self.log.prev_log_index,
                self.log.prev_log_term,
                [],
                self.commit_index
            )
            with self._lock:
                r.add_done_callback(self.__print_result)
                results[peer_id] = r

    def __print_result(self, result):
        print "result {}".format(result.result())

    def next_state(self, cmd, key, value):
        # only allow heartbeats
        succ = False
        with self._lock:
            if not self.leader_id == self.my_id:
                print "This node is not leader"
                succ = False
            self.log.append(log_entry.LogEntry(
                self.current_term,
                self.log.last_log_index,
                cmd,
                key,
                value
            ))
            # send stuff
            succ = True
        return succ

    def get_current_leader(self):
        return self.peers[self.leader_id] if self.leader_id > 0 else (0, "nok")

    def run(self):
        hb = False
        while True:
            if self.is_follower():
                print "start listen leader events"
                self.reset_election_timeout()
                while not self._start_election_event.is_set():
                    print "wait for heartbeats"
                    time.sleep(self.election_timeout / 2)
                self.state = CANDIDATE
            elif self.is_candidate():
                print "become candidate send request votes"
                time.sleep(self.election_timeout)
                self.state = LEADER
            elif self.is_leader():
                if not hb:
                    print "become leader"
                    self.start_heartbeat()
                    hb = True
                time.sleep(self.election_timeout * 2)
                self.step_down()
            hb = False

    def _create_peers_conn(self):
        for pid in self.peers:
            if pid != self.my_id:
                if not self.peers[pid].conn:
                    self.peers[pid].conn = RaftSender(self.peers[pid].address)


class Peer(object):
    def __init__(self, pid, address):
        self.pid = pid
        self.address = address
        self.conn = None

    def conn(self):
        return self.__conn

    @conn.setter
    def conn(self, conn):
        self.__conn = conn


class RaftServicer(raft_pb2_grpc.raftServicer):
    def __init__(self):
        pass

    def AppendEntries(self, request, context):
        pass

    def RequestVote(self, request, context):
        pass


class RaftSender(object):
    def __init__(self, address):
        self.address = address

    def send_append_entries(self, term, leader_id, prev_log_index,
                            prev_log_term,
                            entries,
                            commit_index):
        with grpc.insecure_channel(target=self.address,
                                   options=[('grpc.enable_retries', 3),
                                            ('grpc.keepalive_timeout_ms', 1000)]) as channel:
            stub = raft_pb2_grpc.raftStub(channel)
            msg = raft_pb2.AppendEntriesReq(
                term=term,
                leaderId=leader_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                commitIndex=commit_index
            )
            msg.entries.extend(entries)
            res = stub.AppendEntries(msg)
            return res.term, res.success, res.lastLogIndex

    def send_request_vote(self, candidate_id, term, last_log_index, last_log_term):
        with grpc.insecure_channel(target=self.address,
                                   options=[('grpc.enable_retries', 3),
                                            ('grpc.keepalive_timeout_ms', 1000)]) as channel:
            stub = raft_pb2_grpc.raftStub(channel)
            res = stub.RequestVote(raft_pb2.RequestVoteReq(
                candidateId=candidate_id,
                term=term,
                lastLogIndex=last_log_index,
                lastLogTerm=last_log_term

            ))
            return res.term, res.voteGranted
