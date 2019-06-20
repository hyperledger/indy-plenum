import json
import time
import psutil
import platform
import pip
import os
import base58
import subprocess
import locale
import codecs
from dateutil import parser
import datetime

from ledger.genesis_txn.genesis_txn_file_util import genesis_txn_path
from plenum.common.config_util import getConfig
from plenum.common.util import get_datetime_from_ts
from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys
from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL
from stp_core.common.log import getlogger
from pympler import muppy, summary, asizeof


def decode_err_handler(error):
    length = error.end - error.start
    return length * ' ', error.end


codecs.register_error('decode_errors', decode_err_handler)

logger = getlogger()

MBs = 1024 * 1024
INDY_ENV_FILE = "indy.env"
NODE_CONTROL_CONFIG_FILE = "node_control.conf"
INDY_NODE_SERVICE_FILE_PATH = "/etc/systemd/system/indy-node.service"
NODE_CONTROL_SERVICE_FILE_PATH = "/etc/systemd/system/indy-node-control.service"
NUMBER_TXNS_FOR_DISPLAY = 10
LIMIT_OBJECTS_FOR_PROFILER = 10


def none_on_fail(func):
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            logger.display('Validator info tool fails to execute {} because {}'.
                           format(func.__name__, repr(ex)))
            return None

    return wrap


class ValidatorNodeInfoTool:
    JSON_SCHEMA_VERSION = '0.0.1'
    GENERAL_DB_NAME_TEMPLATE = '{node_name}_info_db'
    GENERAL_FILE_NAME_TEMPLATE = '{node_name}_info.json'
    ADDITIONAL_FILE_NAME_TEMPLATE = '{node_name}_additional_info.json'
    VERSION_FILE_NAME_TEMPLATE = '{node_name}_version_info.json'

    def __init__(self, node, config=None):
        self._node = node
        self._config = config or getConfig()
        self._db = None
        self._use_db = self._config.VALIDATOR_INFO_USE_DB
        self.__name = self._node.name
        self.__node_info_dir = self._node.node_info_dir
        self.dump_version_info()
        if self._use_db:
            self._db = KeyValueStorageRocksdbIntKeys(self.__node_info_dir,
                                                     self.GENERAL_DB_NAME_TEMPLATE.format(
                                                         node_name=self.__name.lower()))

    def stop(self):
        if self._use_db:
            self._db.close()

    @property
    def info(self):
        general_info = {}
        general_info['response-version'] = self.JSON_SCHEMA_VERSION
        general_info['timestamp'] = int(time.time())
        hardware_info = self.__hardware_info
        pool_info = self.__pool_info
        protocol_info = self.__protocol_info
        node_info = self.__node_info
        soft_info = self.software_info

        if hardware_info:
            general_info.update(hardware_info)
        if pool_info:
            general_info.update(pool_info)
        if protocol_info:
            general_info.update(protocol_info)
        if node_info:
            general_info.update(node_info)
        if soft_info:
            general_info.update(soft_info)

        return general_info

    @property
    @none_on_fail
    def memory_profiler(self):
        all_objects = muppy.get_objects()
        stats = summary.summarize(all_objects)
        return {'Memory_profiler': [l for l in summary.format_(stats, LIMIT_OBJECTS_FOR_PROFILER)]}

    @property
    def additional_info(self):
        additional_info = {}
        config_info = self.__config_info
        if config_info:
            additional_info.update(config_info)
        return additional_info

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
    def client_ip(self):
        return self._node.clientstack.ha.host

    @property
    @none_on_fail
    def client_port(self):
        return self._node.clientstack.ha.port

    @property
    @none_on_fail
    def node_ip(self):
        return self._node.nodestack.ha.host

    @property
    @none_on_fail
    def node_port(self):
        return self._node.nodestack.ha.port

    @property
    @none_on_fail
    def bls_key(self):
        return self._node.bls_bft.bls_key_register.get_key_by_name(self._node.name)

    @property
    @none_on_fail
    def __did(self):
        return self._node.wallet.defaultId

    @property
    @none_on_fail
    def __verkey(self):
        return base58.b58encode(self._node.nodestack.verKey).decode("utf-8")

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
    def __config_ledger_size(self):
        return self._node.configLedger.size

    @property
    @none_on_fail
    def __audit_ledger_size(self):
        return self._node.auditLedger.size

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
        inst_by_name = self._node.replicas.inst_id_by_primary_name
        tupl_list = [(name, inst_by_name.get(name, None))
                     for name in list(self._node.nodestack.conns) + [self._node.name]]
        return sorted(tupl_list, key=lambda x: x[0])

    @property
    @none_on_fail
    def __unreachable_count(self):
        return len(self._node.nodestack.remotes) - len(self._node.nodestack.conns)

    @property
    @none_on_fail
    def __unreachable_list(self):
        inst_by_name = self._node.replicas.inst_id_by_primary_name
        tupl_list = [(name, inst_by_name.get(name, None)) for name in
                     list(set(self._node.nodestack.remotes.keys()) - self._node.nodestack.conns)]
        return sorted(tupl_list, key=lambda x: x[0])

    @property
    @none_on_fail
    def __total_count(self):
        return len(self._node.nodestack.remotes) + 1

    def _get_folder_size(self, start_path):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    total_size += os.path.getsize(fp)
                except OSError:
                    pass
        return total_size

    @property
    @none_on_fail
    def node_disk_size(self):
        nodes_data = self._get_folder_size(self._node.ledger_dir)

        return {
            "Hardware": {
                "HDD_used_by_node": "{} MBs".format(int(nodes_data / MBs)),
            }
        }

    @property
    @none_on_fail
    def __hardware_info(self):
        hdd = psutil.disk_usage('/')
        ram_all = psutil.virtual_memory()
        current_process = psutil.Process()
        ram_by_process = current_process.memory_info()

        return {
            "Hardware": {
                "HDD_all": "{} Mbs".format(int(hdd.used / MBs)),
                "RAM_all_free": "{} Mbs".format(int(ram_all.free / MBs)),
                "RAM_used_by_node": "{} Mbs".format(int(ram_by_process.vms / MBs)),
            }
        }

    @none_on_fail
    def _generate_software_info(self):
        os_version = self._prepare_for_json(platform.platform())
        installed_packages = [self._prepare_for_json(pack) for pack in pip.get_installed_distributions()]
        output = self._run_external_cmd("dpkg-query --list | grep indy")
        indy_packages = output.split(os.linesep)
        return {
            "Software": {
                "OS_version": os_version,
                "Installed_packages": installed_packages,
                # TODO add this field
                "Indy_packages": self._prepare_for_json(indy_packages),
            }
        }

    @property
    @none_on_fail
    def software_info(self):
        file_name = self.VERSION_FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__node_info_dir, file_name)
        with open(path, "r") as version_file:
            version_info = json.load(version_file)
        return version_info

    def dump_version_info(self):
        info = self._generate_software_info()
        file_name = self.VERSION_FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__node_info_dir, file_name)
        self._dump_into_file(path, info)

    def _cat_file(self, path_to_file):
        output = self._run_external_cmd("cat {}".format(path_to_file))
        return output.split(os.linesep)

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

        return {"Main_config": main_config,
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
            "Pool_info": {
                "Read_only": self._prepare_for_json(read_only),
                "Total_nodes_count": self._prepare_for_json(self.__total_count),
                "f_value": self._prepare_for_json(self._node.f),
                "Quorums": self._prepare_for_json(self._node.quorums),
                "Reachable_nodes": self._prepare_for_json(self.__reachable_list),
                "Unreachable_nodes": self._prepare_for_json(self.__unreachable_list),
                "Reachable_nodes_count": self._prepare_for_json(self.__reachable_count),
                "Unreachable_nodes_count": self._prepare_for_json(self.__unreachable_count),
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
        for replica in self._node.replicas.values():
            replica_stat = {}
            replica_stat["Primary"] = self._prepare_for_json(replica.primaryName)
            replica_stat["Watermarks"] = "{}:{}".format(replica.h, replica.H)
            replica_stat["Last_ordered_3PC"] = self._prepare_for_json(replica.last_ordered_3pc)
            stashed_txns = {}
            stashed_txns["Stashed_checkpoints"] = self._prepare_for_json(len(replica.stashedRecvdCheckpoints))
            stashed_txns["Stashed_PrePrepare"] = self._prepare_for_json(len(replica.prePreparesPendingPrevPP))
            replica_stat["Stashed_txns"] = stashed_txns
            res[replica.name] = self._prepare_for_json(replica_stat)
        return res

    def _get_node_metrics(self):
        metrics = {}
        for metrica in self._node.monitor.metrics()[1:]:
            if metrica[0] == 'master request latencies':
                latencies = list(metrica[1].values())
                metrics['max master request latencies'] = self._prepare_for_json(
                    max(latencies) if latencies else 0)
            else:
                metrics[metrica[0]] = self._prepare_for_json(metrica[1])
        metrics.update(
            {
                'average-per-second': {
                    'read-transactions': self.__avg_read,
                    'write-transactions': self.__avg_write,
                },
                'transaction-count': {
                    'ledger': self.__domain_ledger_size,
                    'pool': self.__pool_ledger_size,
                    'config': self.__config_ledger_size,
                    'audit': self.__audit_ledger_size,
                },
                'uptime': self.__uptime,
            })
        return metrics

    def _get_ic_queue(self):
        ic_queue = {}
        for view_no, votes in self._node.view_changer.instance_changes.items():
            ics = {voter: {"reason": vote.reason}
                   for voter, vote in votes.items()}
            ic_queue[view_no] = {"Voters": self._prepare_for_json(ics)}
        return ic_queue

    def __get_start_vc_ts(self):
        ts = self._node.view_changer.start_view_change_ts
        return str(datetime.datetime.utcfromtimestamp(ts))

    @property
    @none_on_fail
    def __memory_info(self):
        # Get all memory info and get details with 20 depth
        size_obj = asizeof.asized(self._node, detail=20)
        whole_size = size_obj.size
        size_obj = next(r for r in size_obj.refs if r.name == '__dict__')
        size_dict = dict()
        # Sort in descending order to select most 'heavy' collections
        for num, sub_obj in enumerate(sorted(size_obj.refs, key=lambda v: v.size, reverse=True)):
            if num > 5:
                break
            size_dict[sub_obj.name] = dict()
            size_dict[sub_obj.name]['size'] = sub_obj.size

            # Check if this object (which include __dict__ and __class__) or iterable (dict, list, etc ..)
            if len(sub_obj.refs) <= 2 and any(r.name == '__dict__' for r in sub_obj.refs):
                sub_obj_ref = next(r for r in sub_obj.refs if r.name == '__dict__')
            else:
                sub_obj_ref = sub_obj

            for num, sub_sub_obj in enumerate(sorted(sub_obj_ref.refs, key=lambda v: v.size, reverse=True)):
                if num > 5:
                    break
                size_dict[sub_obj.name][sub_sub_obj.name] = sub_sub_obj.size

        result_dict = {"whole_node_size": whole_size}
        result_dict.update(size_dict)
        return {
            'Memory': result_dict
        }

    @property
    @none_on_fail
    def __node_info(self):
        ledger_statuses = {}
        waiting_cp = {}
        num_txns_in_catchup = {}
        last_txn_3PC_keys = {}
        committed_ledger_root_hashes = {}
        uncommited_ledger_root_hashes = {}
        uncommitted_ledger_txns = {}
        committed_state_root_hashes = {}
        uncommitted_state_root_hashes = {}
        freshness_status = {}

        for lid, linfo in self._node.ledgerManager.ledgerRegistry.items():
            leecher = self._node.ledgerManager._node_leecher._leechers[lid]
            cons_proof_service = leecher._cons_proof_service

            ledger_statuses[lid] = self._prepare_for_json(leecher.state.name)
            waiting_cp[lid] = self._prepare_for_json(leecher.catchup_till._asdict if leecher.catchup_till else None)
            num_txns_in_catchup[lid] = self._prepare_for_json(leecher.num_txns_caught_up)
            last_txn_3PC_keys[lid] = self._prepare_for_json(cons_proof_service._last_txn_3PC_key)
            if linfo.ledger.uncommittedRootHash:
                uncommited_ledger_root_hashes[lid] = self._prepare_for_json(base58.b58encode(linfo.ledger.uncommittedRootHash))
            txns = {"Count": len(linfo.ledger.uncommittedTxns)}
            if len(linfo.ledger.uncommittedTxns) > 0:
                txns["First_txn"] = self._prepare_for_json(linfo.ledger.uncommittedTxns[0])
                txns["Last_txn"] = self._prepare_for_json(linfo.ledger.uncommittedTxns[-1])
            uncommitted_ledger_txns[lid] = txns
            if linfo.ledger.tree.root_hash:
                committed_ledger_root_hashes[lid] = self._prepare_for_json(base58.b58encode(linfo.ledger.tree.root_hash))

        for l_id, state in self._node.db_manager.states.items():
            committed_state_root_hashes[l_id] = self._prepare_for_json(base58.b58encode(state.committedHeadHash))
            uncommitted_state_root_hashes[l_id] = self._prepare_for_json(base58.b58encode(state.headHash))

        ledger_freshnesses = self._get_ledgers_updated_time() or {}
        for idx, updated_ts in ledger_freshnesses.items():
            freshness_status.setdefault(idx, {}).update({'Last_updated_time': self._prepare_for_json(
                get_datetime_from_ts(updated_ts))})
            freshness_status[idx]['Has_write_consensus'] = self._prepare_for_json(
                self._is_updated_time_acceptable(updated_ts))

        return {
            "Node_info": {
                "Name": self._prepare_for_json(self.__alias),
                "Mode": self._prepare_for_json(self._node.mode.name),
                "Client_port": self._prepare_for_json(self.client_port),
                "Client_ip": self._prepare_for_json(self.client_ip),
                "Client_protocol": self._prepare_for_json(ZMQ_NETWORK_PROTOCOL),
                "Node_port": self._prepare_for_json(self.node_port),
                "Node_ip": self._prepare_for_json(self.node_ip),
                "Node_protocol": self._prepare_for_json(ZMQ_NETWORK_PROTOCOL),
                "did": self._prepare_for_json(self.__did),
                'verkey': self._prepare_for_json(self.__verkey),
                'BLS_key': self._prepare_for_json(self.bls_key),
                "Metrics": self._prepare_for_json(self._get_node_metrics()),
                "Committed_ledger_root_hashes": self._prepare_for_json(
                    committed_ledger_root_hashes),
                "Committed_state_root_hashes": self._prepare_for_json(
                    committed_state_root_hashes),
                "Uncommitted_ledger_root_hashes": self._prepare_for_json(
                    uncommited_ledger_root_hashes),
                "Uncommitted_ledger_txns": self._prepare_for_json(
                    uncommitted_ledger_txns),
                "Uncommitted_state_root_hashes": self._prepare_for_json(
                    uncommitted_state_root_hashes),
                "View_change_status": {
                    "View_No": self._prepare_for_json(
                        self._node.viewNo),
                    "VC_in_progress": self._prepare_for_json(
                        self._node.view_changer.view_change_in_progress),
                    "Last_view_change_started_at": self._prepare_for_json(
                        self.__get_start_vc_ts()),
                    "Last_complete_view_no": self._prepare_for_json(
                        self._node.view_changer.last_completed_view_no),
                    "IC_queue": self._prepare_for_json(
                        self._get_ic_queue()),
                    "VCDone_queue": self._prepare_for_json(
                        self._node.view_changer._view_change_done)
                },
                "Catchup_status": {
                    "Ledger_statuses": self._prepare_for_json(
                        ledger_statuses),
                    "Received_LedgerStatus": "",
                    "Waiting_consistency_proof_msgs": self._prepare_for_json(
                        waiting_cp),
                    "Number_txns_in_catchup": self._prepare_for_json(
                        num_txns_in_catchup),
                    "Last_txn_3PC_keys": self._prepare_for_json(
                        last_txn_3PC_keys),
                },
                "Freshness_status": self._prepare_for_json(freshness_status),
                "Requests_timeouts": {
                    "Propagates_phase_req_timeouts": self._prepare_for_json(
                        self._node.propagates_phase_req_timeouts),
                    "Ordering_phase_req_timeouts": self._prepare_for_json(
                        self._node.ordering_phase_req_timeouts)
                },
                "Count_of_replicas": self._prepare_for_json(
                    len(self._node.replicas)),
                "Replicas_status": self._prepare_for_json(
                    self.__replicas_status),
            }
        }

    @property
    @none_on_fail
    def extractions(self):
        return {
            "Extractions":
                {
                    "journalctl_exceptions": self._prepare_for_json(
                        self._get_journalctl_exceptions()),
                    "indy-node_status": self._prepare_for_json(
                        self._get_indy_node_status()),
                    "node-control status": self._prepare_for_json(
                        self._get_node_control_status()),
                    "upgrade_log": self._prepare_for_json(
                        self._get_upgrade_log()),
                    "stops_stat": self._prepare_for_json(
                        self._get_stop_stat()),

                    # TODO effective with rocksdb as storage type.
                    # In leveldb it would be iteration over all txns

                    # "Last_N_pool_ledger_txns": self._prepare_for_json(
                    #     self._get_last_n_from_pool_ledger()),
                    # "Last_N_domain_ledger_txns": self._prepare_for_json(
                    #     self._get_last_n_from_domain_ledger()),
                    # "Last_N_config_ledger_txns": self._prepare_for_json(
                    #     self._get_last_n_from_config_ledger()),
                }
        }

    def _run_external_cmd(self, cmd):
        ret = subprocess.run(cmd,
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             timeout=5)
        return ret.stdout.decode(locale.getpreferredencoding(), 'decode_errors')

    def _get_journalctl_exceptions(self):
        output = self._run_external_cmd("journalctl | sed -n '/Traceback/,/Error/p'")
        return output.split(os.linesep)

    def _get_indy_node_status(self):
        output = self._run_external_cmd("systemctl status indy-node")
        return output.split(os.linesep)

    def _get_node_control_status(self):
        output = self._run_external_cmd("systemctl status indy-node-control")
        return output.split(os.linesep)

    def _get_upgrade_log(self):
        output = ""
        if hasattr(self._node.config, 'upgradeLogFile'):
            path_to_upgrade_log = os.path.join(os.path.join(self._node.ledger_dir,
                                                            self._node.config.upgradeLogFile))
            if os.path.exists(path_to_upgrade_log):
                with open(path_to_upgrade_log, 'r') as upgrade_log:
                    log = upgrade_log.readlines()
                    log_size = self._config.VALIDATOR_INFO_UPGRADE_LOG_SIZE
                    output = log if log_size < 0 or log_size > len(log) else log[-log_size:]
        return output

    def _get_last_n_from_pool_ledger(self):
        i = 0
        txns = []
        for _, txn in self._node.poolLedger.getAllTxn():
            if i >= NUMBER_TXNS_FOR_DISPLAY:
                break
            txns.append(txn)
            i += 1
        return txns

    def _get_last_n_from_domain_ledger(self):
        i = 0
        txns = []
        for _, txn in self._node.domainLedger.getAllTxn():
            if i >= NUMBER_TXNS_FOR_DISPLAY:
                break
            txns.append(txn)
            i += 1
        return txns

    def _get_last_n_from_config_ledger(self):
        i = 0
        txns = []
        for _, txn in self._node.configLedger.getAllTxn():
            if i >= NUMBER_TXNS_FOR_DISPLAY:
                break
            txns.append(txn)
            i += 1
        return txns

    def _dump_into_file(self, file_path, info):
        with open(file_path, 'w') as fd:
            try:
                json.dump(info, fd)
            except Exception as ex:
                logger.error("Error while dumping into json: {}".format(repr(ex)))

    def _dump_into_db(self, info):
        self._db.put(info['timestamp'], json.dumps(info))

    def dump_general_info(self):
        info = self.info
        if self._use_db:
            self._dump_into_db(info)

        file_name = self.GENERAL_FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__node_info_dir, file_name)
        self._dump_into_file(path, info)

    def dump_additional_info(self):
        file_name = self.ADDITIONAL_FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__node_info_dir, file_name)
        self._dump_into_file(path, self.additional_info)

    def _get_time_from_journalctl_line(self, line):
        items = line.split(' ')
        success_items = []
        dtime = None
        for item in items:
            success_items.append(item)
            try:
                dtime = parser.parse(" ".join(success_items))
            except ValueError:
                break
        return dtime

    def _get_stop_stat(self):
        stops = self._run_external_cmd("journalctl | grep 'Stopped Indy Node'").strip()
        res = None
        if stops:
            stop_lines = stops.split(os.linesep)
            if stop_lines:
                first_stop = self._get_time_from_journalctl_line(stop_lines[0])
                last_stop = self._get_time_from_journalctl_line(stop_lines[-1])
                measurement_period = last_stop - first_stop
                res = {
                    "first_stop": str(first_stop),
                    "last_stop": str(last_stop),
                    "measurement_period": str(measurement_period),
                    "total_count": len(stops),
                }
        return res

    def _get_ledgers_updated_time(self)->dict:
        return self._node.master_replica.get_ledgers_last_update_time()

    def _is_updated_time_acceptable(self, updated_time):
        current_time = self._node.utc_epoch()
        return current_time - updated_time <= self._node.config.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT *\
            self._node.config.STATE_FRESHNESS_UPDATE_INTERVAL
