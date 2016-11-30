import plenum.common.log

# this is to stop logging from being configured when other imported modules are
# parsed
# IT MUST BE RUN AT THE EARLIEST POSSIBLE MOMENT
if plenum.common.log.loggingConfigured:
    raise ImportError("logging cannot be configured by this point; "
                      "make sure no other imports occur above this point")
else:
    plenum.common.log.loggingConfigured = True
