import pandas as pd
import os
import time

from pathlib import Path

from app.modules.classes.interface import vCenterInterface


def get_server_postscript(name):
    posts = ['-old', '-ol', '-o', '-new', '-ne', '-n']
    lower_name = name.lower()

    for post in posts:
        if post in lower_name:
            return True, post[1:]

    return False, posts


def find_old_new():
    vcenter = vCenterInterface()

    root = Path('app/data/temp/vsphere_old_new')
    try:
        os.mkdir(root)
    except FileExistsError:
        pass
    now = time.strftime('%Y%m%d-%H%M')
    file = Path(f'old_new_{now}.csv')
    file_path = Path(root / file)

    vs_servers = vcenter.get_vs_servers()

    matching_servers = []
    print('Getting all servers from vSpere...')
    for vs_server in vs_servers:
        vsphere = vcenter.create_vsphere_client(vs_server)
        nodes = vcenter.get_all_nodes(vsphere)
        for node in nodes:
            name = node['config']['name']
            match, post = get_server_postscript(name)
            if match:
                node_data = {
                 'name': name,
                 'postscript': post,
                 'operating_system': node['config']['guestId']
                }
                matching_servers.append(node_data)
            else:
                pass
    frame = pd.DataFrame(matching_servers)

    frame.to_csv(file_path)
    attach = [root, str(file)]

    print('Sending.')
    return attach
