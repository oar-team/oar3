# -*- coding: utf-8 -*-
from mixer.backend.sqlalchemy import Mixer


def generate_fake_data(db):
    mixer = Mixer(session=db.session, commit=True)
    job = mixer.blend('oar.models.Job')
