from oar.lib import db

# get_date
# returns the current time in the format used by the sql database
def get_date():
    result = db.engine.execute("select EXTRACT(EPOCH FROM current_timestamp)").one()
    return int(result)

def notify_user(job, state, msg):
    #TODO see OAR::Modules::Judas::notify_user
    log.info("notify_user not yet implemented !!!!)
