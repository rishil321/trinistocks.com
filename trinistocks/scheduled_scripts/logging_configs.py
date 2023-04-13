import os.path

LOGGING_CONFIG = {
    'version': 1,
    'loggers': {
        '': {  # root logger
            'level': 'NOTSET',
            'handlers': ['rotating_file_handler', 'console_handler', 'critical_mail_handler'],
        },
    },
    'handlers': {
        'console_handler': {
            'level': 'DEBUG',
            'formatter': 'info',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'rotating_file_handler': {
            'level': 'INFO',
            'formatter': 'error',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.abspath(os.path.dirname(__file__)), '../scheduled_scripts/logs', 'scheduled_scripts.info.log'),
            'mode': 'a',
            'maxBytes': 1048576 * 10,
            'backupCount': 10
        },
        'critical_mail_handler': {
            'level': 'CRITICAL',
            'formatter': 'error',
            'class': 'logging.handlers.SMTPHandler',
            'mailhost': 'localhost',
            'fromaddr': 'trinistocks@gmail.com',
            'toaddrs': ['latchmepersad@gmail.com', ],
            'subject': 'Critical error from trinistocks.com'
        }
    },
    'formatters': {
        'info': {
            'format': '%(asctime)s-%(levelname)s-%(name)s:%(module)s|%(funcName)s|%(lineno)s-%(message)s'
        },
        'error': {
            'format': '%(asctime)s-%(levelname)s-%(module)s-%(funcName)s:%(message)s'
        },
    },

}
