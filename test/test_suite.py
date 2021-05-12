import logging
import os
import pytest
# from sqlalchemy import create_engine
from sqlalchemy_kusto.dbapi import *


# def test_connect():
#     engine = create_engine("kusto://localhost/mydb")


def test():
    assert True


def test_connect():
    connection = connect("dododevkusto.westeurope", "deltalake_serving", True)
    assert connection is not None

def test_connect():
    connection = connect("dododevkusto.westeurope", "deltalake_serving", True)
    assert connection is not None
