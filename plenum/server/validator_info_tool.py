import json
import time
import psutil
import platform
import pip
import os
import base58

from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL
from stp_core.common.log import getlogger

logger = getlogger()

MBs = 1024 * 1024


def none_on_fail(func):

    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            logger.debug('Validator info tool fails to '
                         'execute {} because {}'.format(func.__name__, repr(ex)))
            return None
    return wrap


class ValidatorNodeInfoTool:
    JSON_SCHEMA_VERSION = '0.0.1'
    FILE_NAME_TEMPLATE = '{node_name}_info.json'

    def __init__(self, node):
        self._node = node
        self.__name = self._node.name
        self.__node_info_dir = self._node.node_info_dir

    @property
    def info(self):
        general_info = {
            'alias': self.__alias,
            'bindings': {
                'client': {
                    # ip address is going to be set in
                    # validator-info script
                    # 'ip': self.__client_ip,
                    'port': self.__client_port,
                    'protocol': ZMQ_NETWORK_PROTOCOL,
                },
                'node': {
                    # ip address is going to be set in
                    # validator-info script
                    # 'ip': self.__node_ip,
                    'port': self.__node_port,
                    'protocol': ZMQ_NETWORK_PROTOCOL,
                }
            },
            'did': self.__did,
            'response-version': self.JSON_SCHEMA_VERSION,
            'timestamp': int(time.time()),
            'verkey': self.__verkey,
            'metrics': {
                'average-per-second': {
                    'read-transactions': self.__avg_read,
                    'write-transactions': self.__avg_write,
                },
                'transaction-count': {
                    'ledger': self.__domain_ledger_size,
                    'pool': self.__pool_ledger_size,
                },
                'uptime': self.__uptime,
            },
            'pool': {
                'reachable': {
                    'count': self.__reachable_count,
                    'list': self.__reachable_list,
                },
                'unreachable': {
                    'count': self.__unreachable_count,
                    'list': self.__unreachable_list,
                },
                'total-count': self.__total_count,
            },
        }
        if self.__hardware_info:
            general_info.update(self.__hardware_info)
        if self.__software_info:
            general_info.update(self.__software_info)
        if self.__config_info:
            general_info.update(self.__config_info)
        if self.__pool_info:
            general_info.update(self.__pool_info)
        if self.__protocol_info:
            general_info.update(self.__protocol_info)
        if self.__node_info:
            general_info.update(self.__node_info)
        return general_info

    @property
    @none_on_fail
    def __alias(self):
        return self._node.name

    @property
    @none_on_fail
    def __client_ip(self):
        return self._node.clientstack.ha.host

    @property
    @none_on_fail
    def __client_port(self):
        return self._node.clientstack.ha.port

    @property
    @none_on_fail
    def __node_ip(self):
        return self._node.nodestack.ha.host

    @property
    @none_on_fail
    def __node_port(self):
        return self._node.nodestack.ha.port

    @property
    @none_on_fail
    def __did(self):
        return self._node.wallet.defaultId

    @property
    @none_on_fail
    def __verkey(self):
        return base58.b58encode(self._node.nodestack.verKey)

    @property
    @none_on_fail
    def __avg_read(self):
        return self._node.total_read_request_number / (time.time() - self._node.created)

    @property
    @none_on_fail
    def __avg_write(self):
        return self._node.monitor.totalRequests / (time.time() - self._node.created)

    @property
    @none_on_fail
    def __domain_ledger_size(self):
        return self._node.domainLedger.size

    @property
    @none_on_fail
    def __pool_ledger_size(self):
        return self._node.poolLedger.size if self._node.poolLedger else 0

    @property
    @none_on_fail
    def __uptime(self):
        return int(time.time() - self._node.created)

    @property
    @none_on_fail
    def __reachable_count(self):
        return self._node.connectedNodeCount

    @property
    @none_on_fail
    def __reachable_list(self):
        return sorted(list(self._node.nodestack.conns) + [self._node.name])

    @property
    @none_on_fail
    def __unreachable_count(self):
        return len(self._node.nodestack.remotes) - len(self._node.nodestack.conns)

    @property
    @none_on_fail
    def __unreachable_list(self):
        return list(set(self._node.nodestack.remotes.keys()) - self._node.nodestack.conns)

    @property
    @none_on_fail
    def __total_count(self):
        return len(self._node.nodestack.remotes) + 1

    @property
    @none_on_fail
    def __hardware_info(self):
        hdd = psutil.disk_usage('/')
        ram_all = psutil.virtual_memory()
        current_process = psutil.Process()
        ram_by_process = current_process.memory_info()
        nodes_data = psutil.disk_usage(self._node.ledger_dir)

        return {
            "Hardware": {
                "HDD_all": "{} Mbs".format(int(hdd.used / MBs)),
                "RAM_all_free": "{} Mbs".format(int(ram_all.free / MBs)),
                "RAM_used_by_node": "{} Mbs".format(int(ram_by_process.vms / MBs)),
                "HDD_used_by_node": "{} MBs".format(int(nodes_data.used / MBs)),
            }
        }

    @property
    @none_on_fail
    def __software_info(self):
        os_version = platform.platform()
        installed_packages = [str(pack) for pack in pip.get_installed_distributions()]

        return {
            "Software": {
                "OS_version": os_version,
                "Installed_packages": installed_packages,
                # TODO add this field
                "Indy_packages": "",
            }
        }

    @property
    @none_on_fail
    def __config_info(self):
        main_config = ""
        network_config = ""
        user_config = ""
        return {
            "Config": {
                "Main_config": main_config,
                "Network_config": network_config,
                "User_config": user_config,
            }
        }

    @property
    @none_on_fail
    def __pool_info(self):
        read_only = None
        if "poolCfg" in self._node.__dict__:
            read_only = not self._node.poolCfg.writes
        return {
            "Pool info": {
                "Read_only": read_only,
                "Total_nodes": self._node.totalNodes,
                "f_value": self._node.f,
                "Quorums": str(self._node.quorums),
                "Reachable_nodes": self.__reachable_list,
                "Unreachable_nodes": self.__unreachable_list,
                "Blacklisted_nodes": list(self._node.nodeBlacklister.blacklisted),
                "Suspicious_nodes": "",

            }
        }

    @property
    @none_on_fail
    def __protocol_info(self):
        return {
            "Protocol": {
            }
        }

    @property
    @none_on_fail
    def __replicas_status(self):
        res = {}
        for replica in self._node.replicas:
            replica_stat = {}
            replica_stat["Primary"] = replica.primaryName
            replica_stat["Watermarks"] = "{}:{}".format(replica.h, replica.H)
            replica_stat["Last_ordered_3PC"] = replica.last_ordered_3pc
            stashed_txns = {}
            stashed_txns["Stashed_checkoints"] = len(replica.stashedRecvdCheckpoints)
            if replica.prePreparesPendingPrevPP:
                stashed_txns["Min_stashed_PrePrepare"] = replica.prePreparesPendingPrevPP.itervalues[-1]
            replica_stat["Stashed_txns"] = stashed_txns
            res[replica.name] = replica_stat

    @property
    @none_on_fail
    def __node_info(self):
        ledger_statuses = {}
        waiting_cp = {}
        num_txns_in_catchup = {}
        last_txn_3PC_keys = {}
        root_hashes = {}
        uncommited_root_hashes = {}
        uncommited_txns = {}
        for idx, linfo in self._node.ledgerManager.ledgerRegistry.items():
            ledger_statuses[idx] = linfo.state.name
            waiting_cp[idx] = linfo.catchUpTill
            num_txns_in_catchup[idx] = linfo.num_txns_caught_up
            last_txn_3PC_keys[idx] = linfo.last_txn_3PC_key
            if linfo.ledger.uncommittedRootHash:
                uncommited_root_hashes[idx] = base58.b58encode(linfo.ledger.uncommittedRootHash)
            uncommited_txns[idx] = linfo.ledger.uncommittedTxns
            if linfo.ledger.tree.root_hash:
                root_hashes[idx] = base58.b58encode(linfo.ledger.tree.root_hash)

        replicas_status = self.__replicas_status
        ic_queue = {}
        for view_no, queue in self._node.view_changer.instanceChanges.items():
            ics = {}
            ics["Vouters"] = list(queue.voters)
            ics["Message"] = str(queue.msg)
            ic_queue[view_no] = ics
        return {
            "Node info": {
                "Name": self._node.name,
                "Last N pool ledger txns": "",
                "Mode": self._node.mode,
                "Metrics": self._node.monitor.metrics(),
                "Root_hashes": root_hashes,
                "Uncommited_root_hashes": uncommited_root_hashes,
                "Uncommited_txns": uncommited_txns,
                "View_change_status": {
                    "View_No": self._node.viewNo,
                    "VC_in_progress": self._node.view_changer.view_change_in_progress,
                    "IC_queue": ic_queue,
                    "VCDone_queue": str(self._node.view_changer._view_change_done)
                },
                "Catchup_status": {
                    "Ledgers_statuses": ledger_statuses,
                    "Received_LedgerStatus": "",
                    "Waiting_consistency_proof_msgs": waiting_cp,
                    "Number txns_in_catchup": num_txns_in_catchup,
                    "Last_txn_3PC_keys": last_txn_3PC_keys,
                },
                "Count_of_replicas": len(self._node.replicas),
                "Replicas_status": replicas_status,
            }
        }

    def dump_json_file(self):
        file_name = self.FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__node_info_dir, file_name)
        with open(path, 'w') as fd:
            try:
                json.dump(self.info, fd)
            except json.JSONDecodeError as ex:
                logger.error("Erro while dumping into json: {}".format(repr(ex)))
