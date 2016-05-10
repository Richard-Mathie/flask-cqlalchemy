# -*- coding: utf-8 -*-
"""
flask_cqlalchemy

:copyright: (c) 2015-2016 by George Thomas
:license: BSD, see LICENSE for more details

"""
from __future__ import print_function
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import (
    sync_table, create_keyspace_simple, sync_type
)
from cassandra.cqlengine import columns
from cassandra.cqlengine import models
from cassandra.cqlengine import usertype
from cassandra.cqlengine.management import drop_keyspace
from cassandra.cqlengine.management import drop_table
from cassandra import OperationTimedOut

try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack


class CQLAlchemy(object):
    """The CQLAlchemy class. All CQLEngine methods are available as methods of
    Model or columns attribute in this class.
    No teardown method is available as connections are costly and once made are
    ideally not disconnected.
    """

    def __init__(self, app=None):
        """Constructor for the class"""
        self.columns = columns
        self.Model = models.Model
        self.UserType = usertype.UserType
        self.app = app
        self.sync_table = sync_table
        self.sync_type = sync_type
        self.create_keyspace_simple = create_keyspace_simple
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Bind the CQLAlchemy object to the app.

        This method set all the config options for the connection to
        the Cassandra cluster and creates a connection at startup.
        """
        self._hosts_ = app.config['CASSANDRA_HOSTS']
        self._keyspace_ = app.config['CASSANDRA_KEYSPACE']
        consistency = app.config.get('CASSANDRA_CONSISTENCY', 1)
        lazy_connect = app.config.get('CASSANDRA_LAZY_CONNECT', False)
        retry_connect = app.config.get('CASSANDRA_RETRY_CONNECT', False)
        setup_kwargs = app.config.get('CASSANDRA_SETUP_KWARGS', {})

        if not self._hosts_ and self._keyspace_:
            raise NoConfig("""No Configuration options defined.
            At least CASSANDRA_HOSTS and CASSANDRA_CONSISTENCY
            must be supplied""")
        connection.setup(self._hosts_,
                         self._keyspace_,
                         consistency=consistency,
                         lazy_connect=lazy_connect,
                         retry_connect=retry_connect,
                         **setup_kwargs)

    def sync_db(self):
        """Sync all defined tables. All defined models must be imported before
        this method is called
        """
        models = get_subclasses(self.Model)
        for model in models:
            sync_table(model)

    def set_keyspace(self, keyspace_name=None):
        """ Changes keyspace for the current session if keyspace_name is
        supplied. Ideally sessions exist for the entire duration of the
        application. So if the change in keyspace is meant to be temporary,
        this method must be called again without any arguments
        """
        if not keyspace_name:
            keyspace_name = self.app.config['CASSANDRA_KEYSPACE']
        models.DEFAULT_KEYSPACE = keyspace_name
        self._keyspace_ = keyspace_name


class NoConfig(Exception):
    """ Raised when CASSANDRA_HOSTS or CASSANDRA_KEYSPACE is not defined"""
    pass


# some helper functions for masshing the class list
def flatten(lists):
    """flatten a list of lists into a single list"""
    return [item for sublist in lists for item in sublist]


def get_subclasses(cls):
    """get all the non abstract subclasses of cls"""
    if cls.__abstract__:
        return flatten([get_subclasses(scls) for scls in cls.__subclasses__()])
    else:
        return [cls]


class handel_db_timeout:
    def __init__(self,
                 func,
                 tries=2,
                 logger=None,
                 session=None,
                 error=OperationTimedOut):
        self.func = func
        self.tries = tries
        self.logger = logger
        self.session = session
        self.error = error

    def warning(self, mesg):
        if self.logger:
            self.logger.warning(mesg)
        else:
            print(mesg)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        pass

    def execute(self, *args, **kwags):
        default_timeout = None
        for _ in range(self.tries):
            try:
                return self.func(*args, **kwags)
            except self.error:
                self.warning("Taking Longer to drop....")
                if self.session is None:
                    from cassandra.cqlengine.connection import session
                    self.session = session
                if self.session:
                    default_timeout = self.session.default_timeout
                    self.session.default_timeout = 100
                pass
            finally:
                # reset default timeout to original value
                if self.session and default_timeout:
                    self.session.default_timeout = default_timeout
        raise self.error


def drop_db(db):
    """
    Drop the keyspace

    **Take care to execute schema modifications in a single context**
    """
    with handel_db_timeout(drop_keyspace) as dks:
        dks.execute(db._keyspace_)


def drop_tables(db):
    """
    Drop all the models in a keyspace

    **Take care to execute schema modifications in a single context**
    """
    models = get_subclasses(db.Model)
    for model in models:
        with handel_db_timeout(drop_table) as dt:
            dt.execute(model)
