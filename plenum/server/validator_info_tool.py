import json
import time
import psutil
import platform
import pip
import os
import base58
import subprocess

from ledger.genesis_txn.genesis_txn_file_util import genesis_txn_path
from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL
from stp_core.common.log import getlogger

logger = getlogger()

MBs = 1024 * 1024
INDY_ENV_FILE = "indy.env"
NODE_CONTROL_CONFIG_FILE = "node_control.conf"
INDY_NODE_SERVICE_FILE_PATH = "/etc/systemd/system/indy-node.service"
NODE_CONTROL_SERVICE_FILE_PATH = "/etc/systemd/system/indy-node-control.service"


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

    def _prepare_for_json(self, item):
        try:
            json.dumps(item)
        except TypeError as ex:
            return str(item)
        else:
            return item

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
        os_version = self._prepare_for_json(platform.platform())
        installed_packages = [self._prepare_for_json(pack) for pack in pip.get_installed_distributions()]
        ret = subprocess.run("dpkg-query --list | grep indy",
                             shell=True,
                             check=True,
                             universal_newlines=True,
                             stdout=subprocess.PIPE,
                             timeout=5)
        indy_packages = ret.stdout.split(os.linesep)

        return {
            "Software": {
                "OS_version": os_version,
                "Installed_packages": installed_packages,
                # TODO add this field
                "Indy_packages": self._prepare_for_json(indy_packages),
            }
        }

    def _cat_file(self, path_to_file):
        ret = subprocess.run("cat {}".format(path_to_file),
                             shell=True,
                             check=True,
                             universal_newlines=True,
                             stdout=subprocess.PIPE,
                             timeout=5)
        return ret.stdout.split(os.linesep) if ret.returncode == 0 else []

    def _get_genesis_txns(self):
        genesis_txns = {}
        genesis_pool_txns_path = os.path.join(genesis_txn_path(self._node.genesis_dir,
                                              self._node.config.poolTransactionsFile))
        genesis_domain_txns_path = os.path.join(genesis_txn_path(self._node.genesis_dir,
                                                self._node.config.domainTransactionsFile))
        genesis_config_txns_path = os.path.join(genesis_txn_path(self._node.genesis_dir,
                                                self._node.config.configTransactionsFile))
        if os.path.exists(genesis_pool_txns_path):
            genesis_txns['pool_txns'] = self._cat_file(genesis_pool_txns_path)
        if os.path.exists(genesis_domain_txns_path):
            genesis_txns['domain_txns'] = self._cat_file(genesis_domain_txns_path)
        if os.path.exists(genesis_config_txns_path):
            genesis_txns['config_txns'] = self._cat_file(genesis_config_txns_path)

        return genesis_txns

    def _get_configs(self):
        main_config = []
        network_config = []
        user_config = []
        path_to_main_conf = os.path.join(self._node.config.GENERAL_CONFIG_DIR,
                                         self._node.config.GENERAL_CONFIG_FILE)
        if os.path.exists(path_to_main_conf):
            main_config = self._cat_file(path_to_main_conf)

        network_config_dir = os.path.join(self._node.config.GENERAL_CONFIG_DIR,
                                          self._node.config.NETWORK_NAME)
        if os.path.exists(network_config_dir):
            path_to_network_conf = os.path.join(network_config_dir,
                                                self._node.config.NETWORK_CONFIG_FILE)
            network_config = self._cat_file(path_to_network_conf)
        if self._node.config.USER_CONFIG_DIR:
            path_to_user_conf = os.path.join(self._node.config.USER_CONFIG_DIR,
                                             self._node.config.USER_CONFIG_FILE)
            user_config = self._cat_file(path_to_user_conf)

        return {
                "Main_config": main_config,
                "Network_config": network_config,
                "User_config": user_config,
        }

    def _get_indy_env_file(self):
        indy_env = ""
        path_to_indy_env = os.path.join(self._node.config.GENERAL_CONFIG_DIR,
                                        INDY_ENV_FILE)
        if os.path.exists(path_to_indy_env):
            indy_env = self._cat_file(path_to_indy_env)
        return indy_env

    def _get_node_control_file(self):
        node_control = ""
        path_to_node_control = os.path.join(self._node.config.GENERAL_CONFIG_DIR,
                                        NODE_CONTROL_CONFIG_FILE)
        if os.path.exists(path_to_node_control):
            node_control = self._cat_file(path_to_node_control)
        return node_control

    def _get_indy_node_service(self):
        service_file = []
        if os.path.exists(INDY_NODE_SERVICE_FILE_PATH):
            service_file = self._cat_file(INDY_NODE_SERVICE_FILE_PATH)
        return service_file

    def _get_node_control_service(self):
        service_file = []
        if os.path.exists(NODE_CONTROL_SERVICE_FILE_PATH):
            service_file = self._cat_file(NODE_CONTROL_SERVICE_FILE_PATH)
        return service_file

    def _get_iptables_config(self):
        return []


    @property
    @none_on_fail
    def __config_info(self):
        configuration = {"Configuration": {}}
        configuration['Configuration']['Config'] = self._prepare_for_json(
            self._get_configs())
        configuration['Configuration']['Genesis_txns'] = self._prepare_for_json(
            self._get_genesis_txns())
        configuration['Configuration']['indy.env'] = self._prepare_for_json(
            self._get_indy_env_file())
        configuration['Configuration']['node_control.conf'] = self._prepare_for_json(
            self._get_node_control_file())
        configuration['Configuration']['indy-node.service'] = self._prepare_for_json(
            self._get_indy_node_service())
        configuration['Configuration']['indy-node-control.service'] = self._prepare_for_json(
            self._get_node_control_service())
        configuration['Configuration']['iptables_config'] = self._prepare_for_json(
            self._get_iptables_config())

        return configuration


    @property
    @none_on_fail
    def __pool_info(self):
        read_only = None
        if "poolCfg" in self._node.__dict__:
            read_only = not self._node.poolCfg.writes
        return {
            "Pool info": {
                "Read_only": self._prepare_for_json(read_only),
                "Total_nodes": self._prepare_for_json(self._node.totalNodes),
                "f_value": self._prepare_for_json(self._node.f),
                "Quorums": self._prepare_for_json(self._node.quorums),
                "Reachable_nodes": self._prepare_for_json(self.__reachable_list),
                "Unreachable_nodes": self._prepare_for_json(self.__unreachable_list),
                "Blacklisted_nodes": self._prepare_for_json(list(self._node.nodeBlacklister.blacklisted)),
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
            replica_stat["Primary"] = self._prepare_for_json(replica.primaryName)
            replica_stat["Watermarks"] = "{}:{}".format(replica.h, replica.H)
            replica_stat["Last_ordered_3PC"] = self._prepare_for_json(replica.last_ordered_3pc)
            stashed_txns = {}
            stashed_txns["Stashed_checkoints"] = self._prepare_for_json(len(replica.stashedRecvdCheckpoints))
            if replica.prePreparesPendingPrevPP:
                stashed_txns["Min_stashed_PrePrepare"] = self._prepare_for_json(replica.prePreparesPendingPrevPP.itervalues[-1])
            replica_stat["Stashed_txns"] = stashed_txns
            res[replica.name] = self._prepare_for_json(replica_stat)
        return res

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
            ledger_statuses[idx] = self._prepare_for_json(linfo.state.name)
            waiting_cp[idx] = self._prepare_for_json(linfo.catchUpTill)
            num_txns_in_catchup[idx] = self._prepare_for_json(linfo.num_txns_caught_up)
            last_txn_3PC_keys[idx] = self._prepare_for_json(linfo.last_txn_3PC_key)
            if linfo.ledger.uncommittedRootHash:
                uncommited_root_hashes[idx] = self._prepare_for_json(base58.b58encode(linfo.ledger.uncommittedRootHash))
            uncommited_txns[idx] = [self._prepare_for_json(txn) for txn in linfo.ledger.uncommittedTxns]
            if linfo.ledger.tree.root_hash:
                root_hashes[idx] = self._prepare_for_json(base58.b58encode(linfo.ledger.tree.root_hash))

        replicas_status = self._prepare_for_json(self.__replicas_status)
        ic_queue = {}
        for view_no, queue in self._node.view_changer.instanceChanges.items():
            ics = {}
            ics["Vouters"] = self._prepare_for_json(list(queue.voters))
            ics["Message"] = self._prepare_for_json(queue.msg)
            ic_queue[view_no] = self._prepare_for_json(ics)
        metrics = self._node.monitor.metrics()
        if metrics:
            metrics = [self._prepare_for_json(m) for m in metrics]
        return {
            "Node info": {
                "Name": self._prepare_for_json(self._node.name),
                "Last N pool ledger txns": "",
                "Mode": self._prepare_for_json(self._node.mode.name),
                "Metrics": self._prepare_for_json(metrics),
                "Root_hashes": self._prepare_for_json(root_hashes),
                "Uncommitted_root_hashes": self._prepare_for_json(uncommited_root_hashes),
                "Uncommitted_txns": self._prepare_for_json(uncommited_txns),
                "View_change_status": {
                    "View_No": self._prepare_for_json(self._node.viewNo),
                    "VC_in_progress": self._prepare_for_json(self._node.view_changer.view_change_in_progress),
                    "IC_queue": self._prepare_for_json(ic_queue),
                    "VCDone_queue": self._prepare_for_json(self._node.view_changer._view_change_done)
                },
                "Catchup_status": {
                    "Ledger_statuses": self._prepare_for_json(ledger_statuses),
                    "Received_LedgerStatus": "",
                    "Waiting_consistency_proof_msgs": self._prepare_for_json(waiting_cp),
                    "Number_txns_in_catchup": self._prepare_for_json(num_txns_in_catchup),
                    "Last_txn_3PC_keys": self._prepare_for_json(last_txn_3PC_keys),
                },
                "Count_of_replicas": self._prepare_for_json(len(self._node.replicas)),
                "Replicas_status": replicas_status,
            }
        }

    def dump_json_file(self):
        file_name = self.FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__node_info_dir, file_name)
        with open(path, 'w') as fd:
            try:
                json.dump(self.info, fd)
            except Exception as ex:
                logger.error("Error while dumping into json: {}".format(repr(ex)))
