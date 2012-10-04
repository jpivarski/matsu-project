#!/usr/bin/env python

try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import jpype

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(["../CONFIG.ini", "CONFIG.ini"])

    JAVA_VIRTUAL_MACHINE = config.get("DEFAULT", "lib.jvm")
    ACCUMULO_INTERFACE = config.get("DEFAULT", "accumulo.interface")
    ACCUMULO_DB_NAME = config.get("DEFAULT", "accumulo.db_name")
    ZOOKEEPER_LIST = config.get("DEFAULT", "accumulo.zookeeper_list")
    ACCUMULO_USER_NAME = config.get("DEFAULT", "accumulo.user_name")
    ACCUMULO_PASSWORD = config.get("DEFAULT", "accumulo.password")
    ACCUMULO_TABLE_NAME = config.get("DEFAULT", "accumulo.table_name")
    try:
        jpype.startJVM(JAVA_VIRTUAL_MACHINE, "-Djava.class.path=%s" % ACCUMULO_INTERFACE)
        AccumuloInterface = jpype.JClass("org.occ.matsu.AccumuloInterface")
        AccumuloInterface.connectForReading(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    zoomDepthNarrowest = int(config.get("DEFAULT", "mapreduce.zoomDepthNarrowest"))

    try:
        keys = AccumuloInterface.getKeys("T%02d-" % zoomDepthNarrowest, "T%02d-" % (zoomDepthNarrowest + 1))
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    for key in keys:
        print key
