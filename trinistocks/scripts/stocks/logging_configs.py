import os.path

LOGGING_CONFIG = {
    'version': 1,
    'loggers': {
        '': {  # root logger
            'level': 'NOTSET',
            'handlers': ['debug_console_handler', 'info_rotating_file_handler', 'error_file_handler',
                         'critical_mail_handler'],
        },
        'scripts.stocks.scrapettse.scraper': {
            'level': 'DEBUG',
            'propagate': False,
            'handlers': ['info_rotating_file_handler', 'error_file_handler'],
        },
    },
    'handlers': {
        'debug_console_handler': {
            'level': 'DEBUG',
            'formatter': 'info',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'debug_rotating_file_handler': {
            'level': 'DEBUG',
            'formatter': 'info',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join('scripts','debug.log'),
            'mode': 'a',
            'maxBytes': 1048576 * 10,
            'backupCount': 10
        },
        'info_rotating_file_handler': {
            'level': 'INFO',
            'formatter': 'info',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join('scripts','info.log'),
            'mode': 'a',
            'maxBytes': 1048576,
            'backupCount': 10
        },
        'error_file_handler': {
            'level': 'WARNING',
            'formatter': 'error',
            'class': 'logging.FileHandler',
            'filename': os.path.join('scripts','error.log'),
            'mode': 'a',
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
            'format': '%(asctime)s-%(levelname)s-%(name)s|%(funcName)s|%(lineno)s-%(message)s'
        },
        'error': {
            'format': '%(asctime)s-%(levelname)s-%(name)s-%(process)d::%(module)s|%(lineno)s:: %(message)s'
        },
    },

}
