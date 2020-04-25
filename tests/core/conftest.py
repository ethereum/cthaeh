import pytest

from sqlalchemy import create_engine

from cthaeh.models import Base
from cthaeh.session import Session


@pytest.fixture
def engine():
    return create_engine('sqlite:///:memory:', echo=True)


@pytest.fixture
def _Session(engine):
    Session.configure(bind=engine)
    return Session


@pytest.fixture
def _schema(engine):
    Base.metadata.create_all(engine)


@pytest.fixture
def session(_Session, _schema):
    return _Session()
