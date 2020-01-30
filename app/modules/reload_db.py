import multiprocessing

from requests.exceptions import ConnectionError

from app.modules.classes.data_loader import ChefLoader, ServiceNowLoader
from app.modules.classes.data_loader import SolarWindsLoader, vCenterLoader
from app.modules.classes.collection_splitter import SplitCollections
from app.modules.classes.top_processor import TopProcessor


def load_sw():
    sw_loader = SolarWindsLoader()
    sw_loader.solarwinds_pull()


def load_chef():
    chef_loader = ChefLoader()
    chef_loader.chef_pull()


def load_vcenter():
    vc_loader = vCenterLoader()
    vc_loader.vcenter_pull()


def load_servicenow():
    sn_loader = ServiceNowLoader()
    sn_loader.servicenow_pull()


def load_vcenter_tags():
    vc_loader = vCenterLoader()
    vc_loader.vcenter_tags()


def load_servicenow_business_systems():
    sn_loader = ServiceNowLoader()
    sn_loader.bs_servicenow()


def call_processor():
    processor = TopProcessor()
    processor.mass_update_master()


def call_splitter():
    splitter = SplitCollections('dti_servers')
    splitter.mass_update()


def reload():
    chef = multiprocessing.Process(target=load_chef)
    vc = multiprocessing.Process(target=load_vcenter)
    sn = multiprocessing.Process(target=load_servicenow)
    vc_tags = multiprocessing.Process(target=load_vcenter_tags)
    sn_bs = multiprocessing.Process(target=load_servicenow_business_systems)

    p = []
    try:
        load_sw()
    except ConnectionError:
        print("SolarWinds connection failed.")
    try:
        chef.start()
        p.append(chef)
    except ConnectionError:
        print("Chef connection failed.")
    try:
        vc.start()
        p.append(vc)
    except ConnectionError:
        print("vCenter connection failed.")
    try:
        sn.start()
        p.append(sn)
    except ConnectionError:
        print("ServiceNow connection failed.")
    try:
        vc_tags.start()
        p.append(vc_tags)
    except ConnectionError:
        print("vCenter connection failed.")
    try:
        sn_bs.start()
        p.append(sn_bs)
    except ConnectionError:
        print("ServiceNow connection failed.")

    for process in p:
        process.join()

    call_processor()
    call_splitter()
