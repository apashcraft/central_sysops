import json
import sys

from app.modules.utils.tools import csv_pull_key, text_writer
from app.modules.classes.interface import ChefInterface
from app.modules.classes.interface import SolarWindsInterface


def cleaner(word):
    word = word.replace('recipe', '')
    word = word.replace('role', '')
    return word.strip("[]")


def update_targeted_groups(chef):
    chef_management_group = []
    chef_patching_role = []

    roles = chef.roles()
    for role in roles.keys():
        run_list = chef.role_run_list(role)
        run_list = [cleaner(item) for item in run_list]
        if 'chef-client' in run_list:
            chef_management_group.append(role)
        if 'dti-os-patching' in run_list or 'autopatch_ii' in run_list:
            chef_patching_role.append(role)

    text_writer('data/chef_management_group.csv', chef_management_group)
    text_writer('data/chef_patching_role.csv', chef_patching_role)


def get_node(node, chef):
    check = False
    lower = False
    upper = False

    run_list = []
    environment = ""

    while check is False:
        query = f'hostname:{node} OR hostname:{node}.state.de.us OR hostname:{node}.dti.state.de.us OR hostname:{node}.dot.state.de.us OR name:{node} OR name:{node}.state.de.us OR name:{node}.dti.state.de.us OR name:{node}.dot.state.de.us'
        response = chef.chef_search(index='node', query=query)
        response = json.loads(response)
        if response['total'] == 0:
            if upper is False:
                node = node.upper()
                upper = True
            elif lower is False:
                node = node.lower()
                lower = True
            else:
                check = True
        elif response['total'] > 1:
            step = 0
            while step <= response['total']:
                node_data = response['rows'][step]
                try:
                    if node_data['chef_environment'] == 'dti-decomm':
                        step += 1
                    else:
                        run_list = node_data['run_list']
                        environment = node_data['chef_environment']
                        step = response['total'] + 1
                except KeyError:
                    print(node)
                    step += 1
                    pass
            check = True

        else:
            node_data = response['rows'][0]
            run_list = node_data['run_list']
            try:
                environment = node_data['chef_environment']
            except (TypeError, KeyError) as e:
                environment = f'{e}'
            check = True

    return run_list, environment


def load_properties(chef, sw, nodes):
    chef_management_groups = csv_pull_key(
        'data/chef_management_group.csv', 0)
    chef_patching_roles = csv_pull_key(
        'data/chef_patching_role.csv', 0)

    count = 0

    for node in nodes:
        node_management_group = []
        node_patching_role = []
        node_environment = ""
        node_other_roles = []
        node_recipes = []

        name = node['SysName']
        uri = node['Uri']
        run_list, node_environment = get_node(name, chef)
        if run_list:
            for item in run_list:
                if item[:6] == "recipe":
                    item = cleaner(item)
                    node_recipes.append(item)
                else:
                    item = cleaner(item)
                    if item[:20] == 'autopatch_linux_prod' or item[:20] == 'autopatch_linux_test':
                        node_management_group.append('sa-managed')
                        node_patching_role.append(item[:20])
                    elif item in chef_management_groups:
                        node_management_group.append(item)
                    elif item in chef_patching_roles:
                        node_patching_role.append(item)
                    elif item != 'linux_chef_client' and item != 'linux_chef_client_TEST' and item != 'dti_golive':
                        node_other_roles.append(item)
            if len(node_management_group) != 0:
                node_management_group = ', '.join(node_management_group)
            else:
                node_management_group = "none"
            if len(node_patching_role) != 0:
                node_patching_role = ', '.join(node_patching_role)
            else:
                node_patching_role = "none"
            if len(node_other_roles) != 0:
                node_other_roles = ', '.join(node_other_roles)
            else:
                node_other_roles = "none"
            if len(node_recipes) != 0:
                node_recipes = ', '.join(node_recipes)
            else:
                node_recipes = "none"
        else:
            node_management_group = "name does not match"
            node_patching_role = "name does not match"
            node_environment = "name does not match"
            node_other_roles = "name does not match"
            node_recipes = "name does not match"

        updated_props = {'Chef_Management_Group': node_management_group,
                         'Chef_Patching_Role': node_patching_role,
                         'Chef_Environment': node_environment,
                         'Chef_Other_Roles': node_other_roles,
                         'Chef_Recipes': node_recipes}
        sw.change_custom_properties(uri, updated_props)
        count += 1
        sys.stdout.write(f"Complete: {count}/{len(nodes)}\r")


def load_all():
    sw = SolarWindsInterface()
    chef = ChefInterface()

    query_str = """SELECT n.SysName, n.NodeID, n.Uri, n.Agent.AgentID
                   FROM Orion.Nodes n
                   WHERE n.Agent.AgentID is not null and n.SysName !=''"""

    query_results = sw.query(query_str)
    nodes = query_results['results']

    update_targeted_groups(chef)
    load_properties(chef, sw, nodes)
