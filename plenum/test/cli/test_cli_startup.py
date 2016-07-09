def testDefaultWalletAndIdentifier(cli):
    dc = cli.defaultClient
    verstr = next(iter(dc.wallet.signers.values())).verstr
    assert cli.printeds[1]['msg'] == "Current wallet set to {walletName}". \
        format(walletName=dc.name)
    assert cli.printeds[0]['msg'] == \
           "Current identifier set to {alias} ({cryptonym})". \
               format(alias=dc.name, cryptonym=verstr)
