

def create_full_root_hash(root_hash, pool_root_hash):
    """
    Utility method for creating full root hash that then can be signed
    by multi signature
    """
    return root_hash + pool_root_hash
