# Copyright 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Class to monitor a MongoDB server on a background thread."""

import weakref

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from pymongo import common, helpers, message, periodic_executor
from pymongo.server_type import SERVER_TYPE
from pymongo.monotonic import time as _time
from pymongo.server_description import ServerDescription
from pymongo.topology_description import (TopologyDescription,
                                          TOPOLOGY_TYPE)

class SDAMMonitor(object):
    def __init__(
            self,
            topology,
            pool,
            topology_settings):
        """Class to monitor a MongoDB server on a background thread.

        Pass an initial ServerDescription, a Topology, a Pool, and
        TopologySettings.

        The Topology is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        self._pool = pool
        self._settings = topology_settings

        # We strongly reference the executor and it weakly references us via
        # this closure. When the monitor is freed, stop the executor soon.
        def target():
            monitor = self_ref()
            if monitor is None:
                return False  # Stop the executor.
            SDAMMonitor._run(monitor) # TODO: (sdam) understand what this does
            return True

        executor = periodic_executor.PeriodicExecutor(
            condition_class=self._settings.condition_class,
            interval=common.HEARTBEAT_FREQUENCY,
            min_interval=common.MIN_HEARTBEAT_INTERVAL,
            target=target)

        self._executor = executor

        # Avoid cycles. When self or topology is freed, stop executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._topology = weakref.proxy(topology, executor.close)

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._executor.open()

    def close(self):
        """Close and stop monitoring.

        open() restarts the monitor after closing.
        """
        self._executor.close()

        # Increment the pool_id and maybe close the socket. If the executor
        # thread has the socket checked out, it will be closed when checked in.
        self._pool.reset()

    def join(self, timeout=None):
        self._executor.join(timeout)

    def _run(self):
        try:
            self._topology_description = self._command_get_latest_topology()
            self._topology.on_change_topology(self._topology_description)
        except ReferenceError:
            # TODO: (sdam) understand weakref scheme better and figure out
            # whether this should ever happen

            # Topology was garbage-collected.
            self.close()

    def request_get_latest_topology(self):
        """If the monitor is sleeping, wake and check the sdam daemon soon."""
        self._executor.wake()

    # TODO: (sdam) implement this asynchronously
    def request_check_all(self):
        self._command_check_all()

    # TODO: (sdam) implement this asynchronously
    def request_check(self, address):
        self._command_check(address)

    # TODO: (sdam) implement this asynchronously
    def request_urgent_mode(self):
        self._command_urgent_mode()

    # TODO: (sdam) implement this asynchronously
    def request_end_urgent_mode(self):
        self._command_end_urgent_mode()

    def _command_get_latest_topology(self):
        # TODO: (sdam) may not need an entire monitor pool
        with self._pool.get_socket({}) as sock_info:
            td = self._command_get_latest_topology_with_socket(sock_info)
            # TODO: (sdam) return a TopologyDescription instead
            return td

    def _command_check_all(self):
        with self._pool.get_socket({}) as sock_info:
            # TODO: (sdam) check the return value here?
            self._command_check_all_with_socket(sock_info)

    def _command_check(self, address):
        with self._pool.get_socket({}) as sock_info:
            # TODO: (sdam) check the return value here?
            self._command_check_with_socket(sock_info, address)

    def _command_urgent_mode(self):
        with self._pool.get_socket({}) as sock_info:
            # TODO: (sdam) check the return value here?
            self._command_urgent_mode_with_socket(sock_info)

    def _command_end_urgent_mode(self):
        with self._pool.get_socket({}) as sock_info:
            # TODO: (sdam) check the return value here?
            self._command_end_urgent_mode_with_socket(sock_info)

    def _command_get_latest_topology_with_socket(self, sock_info):
        """Return TopologyDescription.

        Can raise ConnectionFailure or OperationFailure.
        """
        request_id, msg, max_doc_size = message.query(
            0, 'admin.$cmd', 0, -1, {'command': 'getlatesttopology'},
            None, DEFAULT_CODEC_OPTIONS)

        # TODO: use sock_info.command()
        sock_info.send_message(msg, max_doc_size)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        # TODO: (sdam) build TopologyDescription from response and return it
        print result
        return self._topology_description_from_result(result['data'][0])

    def _command_check_all_with_socket(self, sock_info):
        request_id, msg, max_doc_size = message.query(
            0, 'admin.$cmd', 0, -1, {'command': 'checkall'},
            None, DEFAULT_CODEC_OPTIONS)

        # TODO: use sock_info.command()
        sock_info.send_message(msg, max_doc_size)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        # TODO: (sdam) get "ok" field from response and return bool
        return False

    def _command_check_with_socket(self, sock_info, address):
        request_id, msg, max_doc_size = message.query(
            0, 'admin.$cmd', 0, -1, {'command': 'check', 'argument': address},
            None, DEFAULT_CODEC_OPTIONS)

        # TODO: use sock_info.command()
        sock_info.send_message(msg, max_doc_size)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        # TODO: (sdam) get "ok" field from response and return bool
        return False

    def _command_urgent_mode_with_socket(self, sock_info):
        request_id, msg, max_doc_size = message.query(
            0, 'admin.$cmd', 0, -1, {'command': 'urgentmode'},
            None, DEFAULT_CODEC_OPTIONS)

        # TODO: use sock_info.command()
        sock_info.send_message(msg, max_doc_size)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        # TODO: (sdam) get "ok" field from response and return bool
        return False

    def _command_end_urgent_mode_with_socket(self, sock_info):
        request_id, msg, max_doc_size = message.query(
            0, 'admin.$cmd', 0, -1, {'command': 'endurgentmode'},
            None, DEFAULT_CODEC_OPTIONS)

        # TODO: use sock_info.command()
        sock_info.send_message(msg, max_doc_size)
        raw_response = sock_info.receive_message(1, request_id)
        result = helpers._unpack_response(raw_response)
        # TODO: (sdam) get "ok" field from response and return bool
        return False

    def _topology_description_from_result(self, doc):
        topo = doc['topology']
        servers = {}
        for s_doc in topo['servers']:
            addr = s_doc['address']
            sd = ServerDescription(
                    address=addr,
                    round_trip_time=topo['avgrtts'][addr],
                    error=s_doc['lasterror'])
            sd._server_type = self._server_type_from_int(s_doc['servertype'])
            sd._all_hosts = '' # TODO: what is this exactly?
            sd._tags = s_doc['tags']
            sd._replica_set_name = s_doc['setname']
            sd._primary = s_doc['primary']
            sd._min_wire_version = s_doc['minwireversion'] if s_doc['minwireversion'] >= 0 else None
            sd._max_wire_version = s_doc['maxwireversion'] if s_doc['maxwireversion'] >= 0 else None
            sd._election_id = s_doc['electionid']
            servers[addr] = sd
        topoType = self._topology_type_from_int(topo['topologytype'])
        td = TopologyDescription(topoType, servers, topo['setname'], topo['maxelectionid'])
        return td

    def _topology_type_from_int(self, topo_type):
        if topo_type == 0:
            return TOPOLOGY_TYPE.Single
        elif topo_type == 1:
            return TOPOLOGY_TYPE.ReplicaSetNoPrimary
        elif topo_type == 2:
            return TOPOLOGY_TYPE.ReplicaSetWithPrimary
        elif topo_type == 3:
            return TOPOLOGY_TYPE.Sharded
        elif topo_type == 4:
            return TOPOLOGY_TYPE.Unknown

    def _server_type_from_int(self, server_type):
        if server_type == 0:
            return SERVER_TYPE.Standalone
        elif server_type == 1:
            return SERVER_TYPE.Mongos
        elif server_type == 2:
            return SERVER_TYPE.RSPrimary
        elif server_type == 3:
            return SERVER_TYPE.RSSecondary
        elif server_type == 4:
            return SERVER_TYPE.RSArbiter
        elif server_type == 5:
            return SERVER_TYPE.RSOther
        elif server_type == 6:
            return SERVER_TYPE.RSGhost
        elif server_type == 7:
            return SERVER_TYPE.Unknown

class SDAMMockMonitor(object):

    def __init__(self, server_description, topology, pool, topology_settings):
        pass

    def open(self):
        pass

    def request_check(self):
        pass

    def close(self):
        pass
