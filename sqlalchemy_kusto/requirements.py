# sqlalchemy_access/requirements.py

from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):

    @property
    def returning(self):
        return exclusions.open()
