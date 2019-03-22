#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2013-2014 Abram Hindle, Araien Zach Redfern
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import flask
from flask import Flask, request
from flask_sockets import Sockets
import gevent
from gevent import queue
import time
import json
import os

app = Flask(__name__, static_url_path='/static')
sockets = Sockets(app)
app.debug = True

# Source: https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py
# Author: Abram Hindle https://github.com/abramhindle
# License: Apache License 2.0
# Date Taken: Wednesday, Marrch 20, 2019
################################################################################
gevents = list()
clients = list()

def send_all(msg):
    for client in clients:
        client.put( msg )

def send_all_json(obj):
    send_all( json.dumps(obj) )

class Client:
    def __init__(self):
        self.queue = queue.Queue()

    def put(self, v):
        self.queue.put_nowait(v)

    def get(self):
        return self.queue.get()
################################################################################

class World:
    def __init__(self):
        self.clear()
        # we've got listeners now!
        self.listeners = list()

    def add_set_listener(self, listener):
        self.listeners.append( listener )

    def update(self, entity, key, value):
        entry = self.space.get(entity,dict())
        entry[key] = value
        self.space[entity] = entry
        self.update_listeners( entity )

    def set(self, entity, data):
        self.space[entity] = data
        self.update_listeners( entity )

    def update_listeners(self, entity):
        '''update the set listeners'''
        for listener in self.listeners:
            listener(entity, self.get(entity))

    def clear(self):
        self.space = dict()

    def get(self, entity):
        return self.space.get(entity,dict())

    def world(self):
        return self.space

myWorld = World()

# TODO:
def set_listener( entity, data ):
    ''' do something with the update ! '''

myWorld.add_set_listener( set_listener )

@app.route('/')
def hello():
    '''Return something coherent here.. perhaps redirect to /static/index.html '''
    return app.send_static_file('index.html')

# TODO:
# Source: https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py
# Author: Abram Hindle https://github.com/abramhindle
# License: Apache License 2.0
# Date Taken: Wednesday, Marrch 20, 2019
################################################################################
def read_ws(ws,client):
    '''A greenlet function that reads from the websocket and updates the world'''

    try:
        while True:

            # Read from the websocket.
            msg = ws.receive()
            print("WS RECV: %s" % msg)
            if (msg is not None):
                packet = json.loads(msg)

                # We need to 'broadcast' the change to other clients!
                send_all_json(packet)

                # Update the world.
                for k, v in packet.items():
                    myWorld.set(k, v)
            else:
                break
    except:
        '''Done'''

    return None

@sockets.route('/subscribe')
def subscribe_socket(ws):
    '''Fufill the websocket URL of /subscribe, every update notify the
       websocket and read updates from the websocket '''

    client = Client()
    clients.append(client)

    # gevent is a 'coroutine' networking library that we will use to to manage
    # socket events on a per-client basis. gevent utilizes event loops and
    # context switching to give the feel of asynchronous event handling, when
    # really it is pseudo-asynchronous through good mangement of events occuring
    # in the queue. Think of it like non-blocking polling of each websocket!

    # To add the socket to the event loop, we pass in the function that will
    # read the socket for a particular client, as well as the information about
    # the socket that said client will be using.
    print("New  Client Socket info: %s" % ws)
    g = gevent.spawn( read_ws, ws, client) # TODO: Comments
    try:
        # While the socket is in use (and no errors), send data to the client.
        while True:
            # block here
            msg = client.get()
            ws.send(msg)
    except Exception as e:
        print("Web Socket Error: %s" % e)
    finally:
        # Remove the particular client from the client list
        clients.remove(client)
        # Kill monitoring of this particular event in the event loop.
        gevent.kill(g)

    return None
################################################################################


# I give this to you, this is how you get the raw body/data portion of a post in flask
# this should come with flask but whatever, it's not my project.
def flask_post_json():
    '''Ah the joys of frameworks! They do so much work for you
       that they get in the way of sane operation!'''
    if (request.json != None):
        return request.json
    elif (request.data != None and request.data.decode("utf8") != u''):
        return json.loads(request.data.decode("utf8"))
    else:
        return json.loads(request.form.keys()[0])

@app.route("/entity/<entity>", methods=['POST','PUT'])
def update(entity):
    '''update the entities via this interface'''

    json_data = json.loads(request.data.decode("utf8"))
    for k, v in json_data.items():
        myWorld.update(entity, k, v)

    return Response(json.dumps(myWorld.get(entity)), status=200, mimetype='application/json')

@app.route("/world", methods=['POST','GET'])
def world():
    '''you should probably return the world here'''
    return Response(json.dumps(myWorld.world()), status=200, mimetype='application/json')

@app.route("/entity/<entity>")
def get_entity(entity):
    '''This is the GET version of the entity interface, return a representation of the entity'''

    e = myWorld.get(entity)
    if e == None:
        return Response(json.dumps(e), status=404, mimetype='application/json')
    else:
        return Response(json.dumps(e), status=200, mimetype='application/json')


@app.route("/clear", methods=['POST','GET'])
def clear():
    '''Clear the world out!'''
    myWorld.clear()
    return Response(json.dumps(myWorld.world()), status=200, mimetype='application/json')



if __name__ == "__main__":
    ''' This doesn't work well anymore:
        pip install gunicorn
        and run
        gunicorn -k flask_sockets.worker sockets:app
    '''
    app.run()
