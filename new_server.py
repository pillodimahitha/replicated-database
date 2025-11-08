#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 20:52:45 2023

@author: miaweaver
"""
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
import grpc

from random import choice
import time
import signal
import math

import pickle


def set_timer(func, reset):
    global LOG
    LOG[TERM].append("Timer started...")
    if reset:
        signal.alarm(0) #stops current alarm
    heartbeat_timeout = choice(range(0, 5)) ##EDIT THIS..
    signal.signal(signal.SIGALRM, func)
    signal.alarm(heartbeat_timeout)
    return

def stop_timer():
    LOG[TERM].append("Timer stopped...")
    signal.alarm(0)
    return

################################## INTERFACE WITH DB #####################################
#
#
#
##########################################################################################
def set_val(key, val):
    global LOCAL_PENDING, REMOTE_PENDING
    LOCAL_PENDING[key] = val
    return key in LOCAL_PENDING.keys()

def get_val(key):    
    try:
        with open('raft_db.pickle','rb') as handle:
                COMMITTED_DB = pickle.load( handle ) ##load DB
        return COMMITTED_DB[key]
    except:
        return "ERR"

def update_db(key):
    global LOCAL_PENDING
    
    try:
        with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    except:
        COMMITTED_DB = {}
        
    COMMITTED_DB[key] = LOCAL_PENDING[key]
    
    with open('raft_db.pickle','wb+') as handle:
        pickle.dumps( COMMITTED_DB, handle ) ##load DB
    
    del LOCAL_PENDING[key]
    return key in COMMITTED_DB


######################## FCNS TO HANDLE RPC INVOKING STATE CHANGE ########################
#
#
#
##########################################################################################
def handle_vote_request():
    global PENDING_ELECTION
    if not PENDING_ELECTION:
        PENDING_ELECTION = True
        return True
    return False

def handle_appendEntry(key, value):
    LOCAL_PENDING[key] = value
    return LOCAL_PENDING

## ELECTION HANDLER FUNCTIONS:
#  election_lost():
#       called in case of heartbeat timeout, or upon rep db initialization, if
#       db initialized with no leader
def election_lost(): ##invoked when dispatcher in candidate state and receives heartbeat
    global STATE, PENDING_ELECTION, VOTES, REQUESTED_VOTES
    PENDING_ELECTION = False #stop the election
    STATE = "F" #update state of dispatcher back to follower
    VOTES = [] ##re-initialize votes for next election
    REQUESTED_VOTES = []
    set_timer( invoke_election, reset = False) ##initialize heartbeat timer
    return

#  election_timeout():
#       called when election times out; simply re-invokes election process    
def election_timeout(signum, frame):
    global ID
    LOG[TERM].append("Election timeout... invoking re-election on replica %d" % ID )
    election( timeout = True) ##commented out for now so we do not enter infinite elections

#  election():
#       prints message if initializing DB, otherwise called from timeout
#       updates dispatcher state to candidate, increments term, sets election timer
#       and sends "vote_4_me" requests to all clients
def election(timeout):
    global PENDING_ELECTION, TERM, STATE, LOG
    if not timeout:
        LOG[TERM].append("FOLLOWER: Initializing first election... Switching status to candidate...")
    else:
        TERM += 1
        LOG[TERM] = []
        LOG[TERM].append("FOLLOWER: Timeout occurred... Switching status to candidate for term %d" % TERM)
    PENDING_ELECTION = True
    STATE = "C"
    set_timer(election_timeout, reset = True) ##set timer...
    return
    
#  invoke_election():
#       heartbeat timeout occurred; print notification and start election process
def invoke_election(signum, frame): ##invoke election from heartbeat timeout (invoked from dispatcher)
    global STATE, LOG, ID
    if not STATE == "L": #if heartbeat timeout on non-leader replica...
        LOG[TERM].append( "Heartbeat timed out... invoking election on replica %d" %  ID )
        election(timeout = True)
    return


def handle_election_outcome(source_node, source_node_term): ##called by follower after election ends
    global PENDING_ELECTION, TERM, ELECTIONS
    PENDING_ELECTION = False
    ELECTIONS[source_node_term] = source_node
    TERM = source_node_term
    if TERM not in LOG.keys():
        LOG[TERM] = []
    return

def handle_heartbeats(source_node, source_node_term):
    global STATE, LOG
    if STATE == "C": ##if candidate, this means election failed...
        LOG[TERM].append("CANDIDATE: election lost...")
        election_lost()
        
    if STATE == "L":    
        LOG[TERM].append("LEADER: new leader detected; switching status to follower")
        STATE = "F"
        
    if STATE == "F":
        if PENDING_ELECTION:
            set_timer( invoke_election, reset = False ) ##reset timer if new leader after election...
            handle_election_outcome(source_node, source_node_term)
            
    return
    
######################## RPC REQUEST STATE & SIMPLE UPDATE STATE  ##################
#
#
#
####################################################################################
def get_server_id():
    return ID

def get_leader_id():
    global ELECTIONS
    return ELECTIONS[ max(ELECTIONS.keys())]

def get_term():
    global TERM
    return TERM

def get_state():
    global STATE
    return STATE

def update_log(log_str):
    global LOG
    LOG[TERM].append(log_str)
    return

def update_state(new_state):
    global STATE
    STATE = new_state
    return

def set_suspended():
    global SUSPENDED
    SUSPENDED = True
    return

######################## EVENT LOOP HELPERS, SEND RPC ##############################
#
#
#
####################################################################################
##handled when leader...
def send_heartbeats():
    global SERVERS, TERM, ID
    for server_key in SERVERS.keys():
        if server_key == ID:
            continue
        
        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)
        params = raft_pb2.heartbeat(source_node = ID, dst_node = server_key, term = TERM)
        response = stub.heartbeatUpdate(params) ##response can be ignored...
    return

##handled when leader...
def send_appendEntry_requests():
    global SERVERS, REMOTE_PENDING, TERM, ID
        
    for commit_key, commit_val in LOCAL_PENDING[TERM].items():
        if not commit_key in REMOTE_PENDING[TERM].keys():
            REMOTE_PENDING[TERM][commit_key] = []

        for server_key in SERVERS.keys():
            if server_key == ID:
                continue
            if server_key in REMOTE_PENDING[TERM][commit_key]: ##if server in list of servers that has already accepted key,val continue...
                continue
                                
            addr = SERVERS[server_key]
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftStub(channel)
            
            params = raft_pb2.appendEntry(source_node = ID, dst_node = server_key, term = TERM,
                                          key = commit_key, value = commit_val)
            response = stub.AppendEntries(params)
            
        if response.outcome:
            REMOTE_PENDING[TERM][commit_key].append(server_key)
    return

def send_vote_4_mes():
    global SERVERS, VOTES, ID
    for server_key in SERVERS.keys():
        if server_key == ID:
            continue
        if server_key in VOTES:
            continue
                            
        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)
        
        params = raft_pb2.vote_4_me(source_node = ID, dst_node = server_key, term = TERM)
        response = stub.VoteRequest(params)
        
        if response.outcome:
            VOTES.append(server_key)
    return

def send_commit_requests(commit_key):
    global TERM, LOCAL_PENDING, REMOTE_PENDING
    for server_key in SERVERS.keys():
        if server_key == ID:
            continue
        
        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)

        params = raft_pb2.commitVal_request(source_node = ID, dst_node = server_key, term = TERM, key = commit_key)
        response = stub.commitVal(params)
        
        if response.outcome:
            try:
                del REMOTE_PENDING[TERM][commit_key]
            except:
                pass
        return

def check_remote_pending():
    ##check REMOTE_PENDING... if any term has quorum of replicas pending on committing it,
    ##commit and send out commit requests.
    #remove it from LOCAL_PENDING & REMOTE_PENDING
    global TERM, LOCAL_PENDING, REMOTE_PENDING
    
    for key, value in LOCAL_PENDING.items():
        if len( REMOTE_PENDING[TERM][key] ) >= QUORUM:
            send_commit_requests(key)
            update_db(key, value)
    return

######################## SERVER INIT & EVENT LOOP ##################################
#
#
#
####################################################################################
def init(server_id):
    ###FETCH DB FROM PICKLE FILE OR INITIALIZE NEW DB...
    global SERVERS
    global STATE, ID, REPLICAS, QUORUM, TERM, SUSPENDED
    global LOG, ELECTIONS, SUSPENDED
    global PENDING_ELECTION, VOTES, REQUESTED_REPLICAS
    global LOCAL_PENDING, REMOTE_PENDING ##handling pre-commit data
    
    LOCAL_PENDING = {} ##store data until committed
    REMOTE_PENDING = {} ##leader track what is stored on other replicas
    SERVERS = {}
    with open("config.conf", "r") as f:
        for line in f:
            SERVERS[int(line.split(" ")[0])] = f'{line.split(" ")[1]}:{line.split(" ")[2].rstrip()}'
    
    STATE  = "F"
    ID = server_id
    REPLICAS = [i for i in SERVERS.keys() if i != ID ]
    QUORUM = math.floor( (len(REPLICAS) + 1) / 2) + 1
    TERM = 0
    LOG = { 0 : []} #TERM : LOG ENTRIES
    ELECTIONS = {} ##no leaders yet... fill out as leaders get elected
    
    VOTES = []
    REQUESTED_REPLICAS = []
    PENDING_ELECTION = False
    
    SUSPENDED = False
    
    time.sleep( choice(range(0, 3)) ) ##vary the amount of time before declaring self candidate so replicas
                                    ##do not all become candidates at once
    if not PENDING_ELECTION:
        election(timeout = False)
    return
    

def event_loop():      
    global SUSPENDED
    try:
        while True: 
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            raft_pb2_grpc.add_RaftServicer_to_server(RAFTServices(), server)
            server.add_insecure_port(SERVERS[ID])
            server.start()
            
            if SUSPENDED: ##suspending to invoke leader re-election
                server.stop(0)
                time.sleep(5)
                server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
                raft_pb2_grpc.add_RaftServicer_to_server(RAFTServices(), server)
                server.add_insecure_port(SERVERS[ID])
                server.start()
                SUSPENDED = False

            if STATE == "F": ###if follower do nothing
                continue
            elif STATE == "C": ###if candidate send vote_4_mes to replicas that haven't voted for client yet
                send_vote_4_mes()
            elif STATE == "L": ##sending appendetnries and heartbeats
                send_heartbeats() #sends heartbeats
                check_remote_pending() #checks if quorum of replicas have accepted appendEntry, if yes, send commit requests
                send_appendEntry_requests() #sends appendEntries to replicas not in REMOTE_PENDING, if any exist for this term
    except Exception as e:
        ##KEYBOARD INTERRUPT... PRINT LOG & SAVE TO OUTPUT...
        print(e)
        try:
            with open('raft_db.pickle','rb') as handle:
                COMMITTED_DB = pickle.load( handle ) ##load DB
        except:
            COMMITTED_DB = {}
        if ELECTIONS != {}:
            LOG[TERM].append("[END OF SESSION] leader: %d\t term: %d" % (ELECTIONS[ max(ELECTIONS.keys())], TERM) )
        LOG[TERM].append( str("[END OF SESSION] elections: " + str(list(ELECTIONS.items()))) )
        LOG[TERM].append( str("[END OF SESSION] local pendingDB:" + str(list(LOCAL_PENDING.items()))) )
        LOG[TERM].append( str("[END OF SESSION] remote pendingDB:" + str(list(REMOTE_PENDING.items()))) )
        LOG[TERM].append( str("[END OF SESSION] commited_db:" + str(list(COMMITTED_DB.items()))) )
        
        ##turn off alarm...
        stop_timer()
        for item in LOG:
            print(item)
    return

########################################################################

class RAFTServices(raft_pb2_grpc.RaftServicer):
    ##HANDLE RPC CALLS....
    ##handles incoming RPC and passes to leader, follower, or candidate based on status...
    def VoteRequest(source_node, dst_node, term):
        resp_dst_node = source_node
        resp_src_node = dst_node
        replica_term = get_term()
        replica_state = get_state()
        
        if term > replica_term and replica_state == "F":
            outcome = handle_vote_request()
            vote_4_me_reply = raft_pb2.voted_4_u(source_node = resp_src_node, dst_node = resp_dst_node,
                                                 term = replica_term, outcome = outcome)
        else:
            vote_4_me_reply = raft_pb2.voted_4_u(source_node = resp_src_node, dst_node = resp_dst_node,
                                                 term = replica_term, outcome = False)
        return vote_4_me_reply

    def AppendEntryRequest(self, source_node, dst_node, term, key, value):
        resp_dst_node = source_node
        resp_src_node = dst_node
        
        replica_term = get_term()
        replica_state = get_state()

        if term == replica_term and replica_state == "F":
            LOCAL_PENDING = handle_appendEntry(key, value)
            appendEntry_reply = raft_pb2.appendEntry_pending(source_node = resp_src_node, dst_node = resp_dst_node,
                                                             term = replica_term, outcome = key in LOCAL_PENDING.keys())
        else:
            appendEntry_reply = raft_pb2.appendEntry_pending(source_node = resp_src_node, dst_node = resp_dst_node,
                                                             term = replica_term, outcome = False)
        return appendEntry_reply
    
    def heartbeatUpdate(source_node, dst_node, term):
        resp_dst_node = source_node
        resp_src_node = dst_node

        replica_term = get_term()
        replica_state = get_state()
        
        if term > replica_term:
            if replica_state == "C": ##if candidate, this means election failed...
                log_str = str("CANDIDATE: Election for term %d lost... Returning to follower status" % replica_term )
                update_log(log_str)
                election_lost()
                
            else:
                if replica_state == "L": ##notified of new leader
                    log_str = str("LEADER: New leader detected... Updating to term %d and switching to follower status..." % replica_term)
                    update_state("F")
                    
            handle_heartbeats(source_node, term)
        heartbeat_reply = raft_pb2.heartbeat_response(source_node = resp_src_node, dst_node = resp_dst_node,
                                                      term = replica_term)
        return heartbeat_reply
    
    def getVal(key):
        value = get_val(key)
        outcome = True if not value == "ERR" else False
        response = raft_pb2.getVal_response(value = value, outcome = outcome)
        return response
    
    def setVal(key, value):
        set = set_val(key, value)
        response = raft_pb2.setVal_response(outcome = set)
        return response
    
    def commitVal(source_node, dst_node, term, key):
        resp_dst_node = source_node
        resp_src_node = dst_node
        replica_term = get_term()
        if term == replica_term:
            outcome = update_db(key)
        response = raft_pb2.commitVal_response(source_node = resp_src_node, dst_node = resp_dst_node,
                                               term = replica_term, outcome = outcome)
        return response

    def suspend():
        set_suspended() 
        response = raft_pb2.suspend_response(temp = 0)
        return response
    ##END HANDLE RPC CALLS....

if __name__ == "__main__":
    init(1)
    event_loop()
