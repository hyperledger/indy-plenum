def dict_cmp(d1, d2):
    if len(d1) != len(d2):
        return False
    for k, v in d1.items():
        if v != d2[k]:
            return False
    for k, v in d2.items():
        if v != d1[k]:
            return False
    return True

def test_replicas_primary_name_by_inst_id(looper, txnPoolNodeSet):
    assert len(txnPoolNodeSet) == 4
    id_to_name = txnPoolNodeSet[0].replicas.primary_name_by_inst_id
    assert dict_cmp(id_to_name, {0: "Alpha", 1: "Beta"})


def test_replicas_inst_id_by_primary_name(looper, txnPoolNodeSet):
    assert len(txnPoolNodeSet) == 4
    name_to_id = txnPoolNodeSet[0].replicas.inst_id_by_primary_name
    assert dict_cmp(name_to_id, {"Alpha": 0, "Beta": 1})
