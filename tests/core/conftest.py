import pytest

from sqlalchemy import create_engine

from cthaeh.models import Base
from cthaeh.tools.session import Session


@pytest.fixture
def engine():
    return create_engine('sqlite:///:memory:', echo=True)


@pytest.fixture(autouse=True)
def _Session(engine):
    Session.configure(bind=engine)


@pytest.fixture(autouse=True)
def _schema(engine):
    Base.metadata.create_all(engine)


@pytest.fixture
def session():
    return Session()
