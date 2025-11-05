const LOG_LEVELS = {
    ERROR: 'ERROR',
    WARN: 'WARN',
    INFO: 'INFO',
    DEBUG: 'DEBUG',
}

class Logger {
    constructor(context = 'app') {
        this.context = context
    }

    _formatMessage(level, message, data = null) {
        const timestamp = new Date().toISOString()
        const logObject = {
            timestamp,
            level,
            context: this.context,
            message,
        }

        if (data) {
            logObject.data = data
        }

        return logObject;
    }

    _log(level, message, data = null) {
        const logMessage = this._formatMessage(level, message, data)

        if (level === LOG_LEVELS.ERROR) {
            console.error(JSON.stringify(logMessage), null, 2)
        } else {
            console.log(JSON.stringify(logMessage), null, 2)
        }
    }

    info(message, data = null) {
        this._log(LOG_LEVELS.INFO, message, data)
    }

    error(message, data = null) {
        this._log(LOG_LEVELS.ERROR, message, data)
    }

    warn(message, data = null) {
        this._log(LOG_LEVELS.WARN, message, data)
    }

    debug(message, data = null) {
        this._log(LOG_LEVELS.DEBUG, message, data)
    }
}

module.exports = Logger;