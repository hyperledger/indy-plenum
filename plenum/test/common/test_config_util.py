import pytest
import os
from importlib import import_module
from plenum.common.config_util import extend_with_external_config, \
    extend_with_default_external_config

TEST_NETWORK_NAME = 'test_network'
GENERAL_CONFIG_FILE_NAME = 'test_config.py'
USER_CONFIG_FILE_NAME = 'user_config.py'
NETWORK_CONFIG_FILE_NAME = 'net_config.py'
CFG_REPL_VALS = [1, 2, 3]


@pytest.fixture(scope="function")
def default_config(tdir):
    plenum_cfg = import_module("plenum.config")
    cfg = {'NETWORK_NAME': TEST_NETWORK_NAME,
           'GENERAL_CONFIG_DIR': tdir,
           'GENERAL_CONFIG_FILE': GENERAL_CONFIG_FILE_NAME,
           'NETWORK_CONFIG_FILE': NETWORK_CONFIG_FILE_NAME,
           'baseDir': tdir,
           'USER_CONFIG_FILE': USER_CONFIG_FILE_NAME,
           'int_val': -1,
           'str_val': 'value-1'}
    plenum_cfg.__dict__.update(cfg)
    return plenum_cfg


@pytest.fixture(scope="function")
def default_config_file_system(default_config):
    assert os.path.isdir(default_config.baseDir)
    gen_net_dir = os.path.join(default_config.GENERAL_CONFIG_DIR,
                               default_config.NETWORK_NAME)
    gen_base_dir = os.path.join(default_config.baseDir, default_config.NETWORK_NAME)
    os.makedirs(gen_net_dir, exist_ok=True)
    os.makedirs(gen_base_dir, exist_ok=True)
    assert os.path.isdir(gen_net_dir)
    assert os.path.isdir(gen_base_dir)
    gen_cfg_file = os.path.join(default_config.GENERAL_CONFIG_DIR,
                                default_config.GENERAL_CONFIG_FILE)
    net_cfg_file = os.path.join(gen_net_dir, default_config.NETWORK_CONFIG_FILE)
    usr_cfg_file = os.path.join(gen_base_dir, default_config.USER_CONFIG_FILE)
    for i, f in zip(CFG_REPL_VALS, [gen_cfg_file, net_cfg_file, usr_cfg_file]):
        with open(f, "w") as file_cfg:
            file_cfg.write("int_val = {}\n".format(i))
            file_cfg.write("str_val = 'value{}'\n".format(i))
    assert os.path.isfile(gen_cfg_file)
    assert os.path.isfile(net_cfg_file)
    assert os.path.isfile(usr_cfg_file)


def test_extend_with_external_config_req_exist(default_config,
                                               default_config_file_system):
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_external_config(default_config, (
        default_config.GENERAL_CONFIG_DIR, default_config.GENERAL_CONFIG_FILE),
                                required=True)
    assert default_config.int_val == 1
    assert default_config.str_val == 'value1'


def test_extend_with_external_config_req_not_exist(default_config,
                                                   default_config_file_system):
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    with pytest.raises(FileNotFoundError):
        extend_with_external_config(default_config, (
            "/some/bad/path", "some_bad.py"), required=True)
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'


def test_extend_with_external_config_not_req_exist(default_config,
                                                   default_config_file_system):
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_external_config(default_config, (
        default_config.GENERAL_CONFIG_DIR, default_config.GENERAL_CONFIG_FILE),
                                required=False)
    assert default_config.int_val == 1
    assert default_config.str_val == 'value1'


def test_extend_with_external_config_not_req_not_exist(default_config,
                                                       default_config_file_system):
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_external_config(default_config,
                                ("/some/bad/path", "some_bad.py"), required=False)
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'


def test_extend_with_default_external_config_no_any(default_config,
                                                    default_config_file_system):
    gen_net_dir = os.path.join(default_config.GENERAL_CONFIG_DIR, default_config.NETWORK_NAME)
    gen_base_dir = os.path.join(default_config.baseDir, default_config.NETWORK_NAME)
    gen_cfg_file = os.path.join(default_config.GENERAL_CONFIG_DIR, default_config.GENERAL_CONFIG_FILE)
    net_cfg_file = os.path.join(gen_net_dir, default_config.NETWORK_CONFIG_FILE)
    usr_cfg_file = os.path.join(gen_base_dir, default_config.USER_CONFIG_FILE)
    os.remove(gen_cfg_file)
    os.remove(net_cfg_file)
    os.remove(usr_cfg_file)
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_default_external_config(default_config)
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'


def test_extend_with_default_external_config_only_gen(default_config, default_config_file_system):
    gen_net_dir = os.path.join(default_config.GENERAL_CONFIG_DIR, default_config.NETWORK_NAME)
    gen_base_dir = os.path.join(default_config.baseDir, default_config.NETWORK_NAME)
    net_cfg_file = os.path.join(gen_net_dir, default_config.NETWORK_CONFIG_FILE)
    usr_cfg_file = os.path.join(gen_base_dir, default_config.USER_CONFIG_FILE)
    os.remove(net_cfg_file)
    os.remove(usr_cfg_file)
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_default_external_config(default_config)
    assert default_config.int_val == 1
    assert default_config.str_val == 'value1'


def test_extend_with_default_external_config_gen_net(default_config, default_config_file_system):
    gen_base_dir = os.path.join(default_config.baseDir, default_config.NETWORK_NAME)
    usr_cfg_file = os.path.join(gen_base_dir, default_config.USER_CONFIG_FILE)
    os.remove(usr_cfg_file)
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_default_external_config(default_config)
    assert default_config.int_val == 2
    assert default_config.str_val == 'value2'


def test_extend_with_default_external_config_all(default_config, default_config_file_system):
    assert default_config.int_val == -1
    assert default_config.str_val == 'value-1'
    extend_with_default_external_config(default_config)
    assert default_config.int_val == 3
    assert default_config.str_val == 'value3'
