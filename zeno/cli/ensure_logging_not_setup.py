from zeno.common import util

# this is to stop logging from being configured when other imported modules are
# parsed
# IT MUST BE RUN AT THE EARLIEST POSSIBLE MOMENT
if util.loggingConfigured:
    raise ImportError("logging cannot be configured by this point; "
                      "make sure no other imports occur above this point")
else:
    util.loggingConfigured = True
