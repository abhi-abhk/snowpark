from snowflake.snowpark import Session

class utils:


    def __init__(self, type='', language='', source='', destination='', feed='', out=''):
        self._default_params = {
        'account': '',
        'user': '',
        'password': '',
        'role': '',
        'warehouse': '',
        'database': '',
        'schema': ''
    }

    def getSession(self):
        session = Session.builder.configs(self._default_params).create()
        return session


    def getTable(self, table='', sql='', isSql=False):
        session = utils.getSession(self)
        if isSql:
            out = session.sql(sql)
            return out
        else:
            out = session.table(table)
            return  out


    def writeTable(self, data, name='', overwrite=True):
        mode = 'overwrite'
        if overwrite == False :
            mode = 'append'
        data.write.mode(mode).save_as_table(name)
