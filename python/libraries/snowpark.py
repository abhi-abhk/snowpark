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

    # @property
    # def session(self):
    #     self._default_session = Session.builder.configs(self._default_params).create()
    #     return self._default_session
    #     # return self._language
    #
    # @session.setter
    # def session(self, value):
    #     if len(value) != 0:
    #         self._default_session = value
    #     else:
    #         raise ValueError("Mandatory Configuration")

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
