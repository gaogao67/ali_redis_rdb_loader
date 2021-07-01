# -*- coding: utf-8 -*-
# =============================================================================
#     FileName:
#         Desc:
#       Author:
#        Email:
#     HomePage:
#      Version:
#   LastChange:
#      History:
# =============================================================================
import os
import logging
import logging.config


class LogHelper(object):
    @staticmethod
    def get_logger(logger_name=""):
        """
        获取Logger
        :param logger_name:
        :return:
        """
        if logger_name == "":
            logger_name = "default"
        return logging.getLogger(logger_name)

    @staticmethod
    def get_logging_config(log_file_prefix, debug_or_info="INFO"):
        """
        获取logging的配置
        :param log_file_prefix:
        :param debug_or_info:
        :return:
        """
        base_dir = os.path.dirname(os.path.dirname(__file__))
        logger_dir = os.path.join(base_dir, 'logs')
        if not os.path.exists(logger_dir):
            os.makedirs(logger_dir)
        # 'format': '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '[%(asctime)s]-[%(levelname)s]: %(message)s'
                }
            },
            'handlers': {
                'console_handler': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'standard',
                },
                'default_file_handler': {
                    'level': debug_or_info,
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': os.path.join(logger_dir, log_file_prefix + '_log.txt'),
                    'maxBytes': 1024 * 1024 * 500,
                    'backupCount': 20,
                    'formatter': 'standard',
                },
                'error_file_handler': {
                    'level': 'WARNING',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': os.path.join(logger_dir, log_file_prefix + '_error.txt'),
                    'maxBytes': 1024 * 1024 * 500,
                    'backupCount': 20,
                    'formatter': 'standard',
                },

            },
            'loggers': {
                'default': {
                    'handlers': ['default_file_handler', 'error_file_handler', 'console_handler'],
                    'level': 'INFO',
                    'propagate': False,
                },
            }
        }
        return config

    @staticmethod
    def init_logger(log_file_prefix, debug_or_info="INFO"):
        """
        初始化Logging配置
        :param log_file_prefix:
        :param debug_or_info:
        :return:
        """
        logging_config = LogHelper.get_logging_config(
            log_file_prefix=log_file_prefix,
            debug_or_info=debug_or_info
        )
        logging.config.dictConfig(logging_config)

