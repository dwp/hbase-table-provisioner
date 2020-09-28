package app.exception

import java.lang.Exception

class TableExistsInHbase(message: String) : Exception(message)
