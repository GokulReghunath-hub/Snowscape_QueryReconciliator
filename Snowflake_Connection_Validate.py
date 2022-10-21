#!/usr/bin/env python
import snowflake.connector
import Config

# Gets the version
ctx = snowflake.connector.connect(
    user=Config.user,
    #password=Config.password,
    authenticator='externalbrowser',
    account=Config.account,
    role = Config.role,
    database = Config.database1,
    schema = Config.schema1,
    warehouse = Config.warehouse,
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()