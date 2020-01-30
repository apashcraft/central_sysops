from app import db


class BusinessSystems(db.EmbeddedDocument):
    chef = db.ListField(db.StringField())
    vcenter = db.StringField()
    solarwinds = db.StringField()
    servicenow = db.ListField(db.StringField())


class vCenterIds(db.EmbeddedDocument):
    MoRefID = db.StringField()
    InstanceUuid = db.StringField()
    SMBiosUuid = db.StringField()


class Uuids(db.EmbeddedDocument):
    chef_guuid = db.StringField()
    solarwinds_uri = db.StringField()
    vcenter = db.EmbeddedDocumentField(vCenterIds)
    servicenow_uuid = db.StringField()


class Server(db.Document):
    meta = {'collection': 'top'}
    name = db.StringField(required=True)
    tier = db.StringField()
    environment = db.StringField()
    department = db.StringField()
    ip = db.ListField(db.StringField())
    management_group = db.StringField()
    platform = db.StringField()
    operating_system = db.StringField()
    power_state = db.StringField()
    business_system = db.EmbeddedDocumentField(BusinessSystems)
    uuids = db.EmbeddedDocumentField(Uuids)
    vcenter_tags = db.DictField()
    chef_run_list = db.ListField(db.StringField())
    chef_tags = db.ListField(db.StringField())
    application = db.StringField()
    database_management_group = db.StringField()
    last_updated = db.StringField()
