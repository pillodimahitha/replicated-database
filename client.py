#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 10:06:58 2023

@author: miaweaver
"""

import grpc
import raft_pb2
import raft_pb2_grpc
import sys

    
def parse( msg):
    command =  msg.split(" ")[0]
    parsed_msg =  msg.split(" ")

    if command == "connect" or command == "Connect":
        return ["Connect", parsed_msg[1]]
    
    elif command == "suspend" or command == "Suspend":
        return ["Suspend"]
    
    elif command == "setval" or command == "setVal":
        return ["setVal", parsed_msg[1], parsed_msg[2]]
    
    elif command == "getval" or command == "getVal":
        return ["getVal", parsed_msg[1]]
    
    elif command == "quit" or command == "Quit" or command == "Q":
        return ["Quit"]
    
    else:
        return ("Invalid")

def suspend():
    global SERVER
    channel = grpc.insecure_channel(SERVER)
    stub = raft_pb2_grpc.RaftStub(channel)

    try:
        params = raft_pb2.suspend_request(temp = 0) ##temp is meaningless... idk how to make an empty rpc msg
        response = stub.suspend(params)
    except grpc.RpcError as e:
            print("gRPC ERR: ", e)
            fail()
    return

# Issue a SetVal command to the server
def set_val(key, value):
    global SERVER
    channel = grpc.insecure_channel(SERVER)
    stub = raft_pb2_grpc.RaftStub(channel)

    try:
        params = raft_pb2.setVal_Request(key=key, value=value)
        response = stub.setVal(params)
    except grpc.RpcError as e:
            print("gRPC ERR: ", e)
            fail()
    return

# Issue a GetVal command to the server
def get_val(key):
    global SERVER

    channel = grpc.insecure_channel(SERVER)
    stub = raft_pb2_grpc.RaftStub(channel)

    try:
        params = raft_pb2.getVal_request(key=key)
        response = stub.getVal(params)

        if response.outcome == False:
            print("ERROR: No value associated with key")
        else:
            print(response.value)
                    
    except grpc.RpcError  as e:
            print("gRPC ERR: ", e)
            fail()
    return

# Terminate the client
def terminate():
    print("Terminating client.")
    sys.exit(0)
    
def fail():
    global SERVER
    print("The server % is unavailable" % SERVER)
    return

def connect(ip_addr_port_num):
    global SERVER
    SERVER = ip_addr_port_num

# Initialize the client
def init():
    print("Initializing...")

    while True:
        try:
            user_input = input(">")
            parsed_input = parse(user_input)
            msg_type = parsed_input[0]

            if  msg_type == "Connect" and len(parsed_input) >= 2 : ##make sure enough input to handle request
                connect(parsed_input[1])
                
            elif  msg_type == "Suspend":
                suspend()
                
            elif  msg_type == "setVal" and len(parsed_input) >= 3: ##make sure enough input to handle request
                set_val(parsed_input[1], parsed_input[2])
                
            elif  msg_type == "getVal" and len(parsed_input) >= 2: ##make sure enough input to handle request
                get_val(parsed_input[1])
                
            elif msg_type == "Quit":
                terminate()
                
            else:
                print("Invalid command! Please try again.")

        except KeyboardInterrupt:
            terminate()

if __name__ == "__main__":
    init()
