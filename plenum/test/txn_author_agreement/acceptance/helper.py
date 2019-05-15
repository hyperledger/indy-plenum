from plenum.common.request import SafeRequest

from plenum.test.helper import sdk_sign_request_from_dict


def gen_signed_request(looper, sdk_wallet, operation, taa_acceptance=None):
    req = sdk_sign_request_from_dict(
        looper, sdk_wallet, operation, taa_acceptance=taa_acceptance
    )
    return SafeRequest(**req)
