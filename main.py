import json
import logging

import gitlab
from typing import Dict

from gitlab.v4.objects import ProjectIssue
from pyairtable import Table


logging.getLogger().setLevel(logging.INFO)


class ConfigurationError(Exception):
    pass


with open('config.json') as f:
    config = json.load(f)

airtable_api_key = config['airtable_credentials'].get('api_key')
airtable_base_id = config['airtable_credentials'].get('base_id')
airtable_table_id = config['airtable_credentials'].get('table_id')

gitlab_private_token = config['gitlab_credentials'].get('private_token')

gitlab_projects = [project for project in config['gitlab_projects']]

gitlab_to_airtable_field_map = {
    'title': config['airtable_field_mapping'].get('title'),
    'iid': config['airtable_field_mapping'].get('ticket_number'),
    'web_url': config['airtable_field_mapping'].get('url'),
    # 'assignees': config['airtable_field_mapping'].get('assignees'), TODO
    'labels': config['airtable_field_mapping'].get('labels'),
    'weight': config['airtable_field_mapping'].get('weight'),
    'milestone': config['airtable_field_mapping'].get('milestone'),
}

primary_key_map = {'ticket_number': 'iid', 'url': 'web_url'}
primary_key_selection = config['options']['gitlab_primary_key']
gitlab_primary_key = primary_key_map.get(primary_key_selection)
airtable_primary_key = gitlab_to_airtable_field_map.get(gitlab_primary_key)

try:
    if not airtable_api_key:
        raise ConfigurationError('Missing Airtable api key in config,json')
    elif not airtable_base_id:
        raise ConfigurationError('Missing Airtable base id in config,json')
    elif not airtable_table_id:
        raise ConfigurationError('Missing Airtable table id/name in config,json.')
    elif not gitlab_private_token:
        raise ConfigurationError('Missing GitLab private access token in config.json')
    elif not gitlab_projects:
        raise ConfigurationError('Missing GitLab project ids in config.json')
    elif not gitlab_to_airtable_field_map.values():
        raise ConfigurationError('Missing GitLab to Airtable field mapping in config.json')
    elif not primary_key_selection:
        raise ConfigurationError('Missing primary key in config.json')

except ConfigurationError as e:
    logging.exception(e)


def get_airtable_records() -> Dict:
    table = Table(airtable_api_key, airtable_base_id, airtable_table_id)

    logging.info('Pulling all existing records from Airtable table.')

    records = table.all()
    records_map = {}
    for record in records:
        key = record['fields'].get(airtable_primary_key)
        if key:
            records_map[key] = record

    logging.info('Successfully pulled Airtable records.')

    return records_map


def get_gitlab_tickets() -> Dict:
    gl = gitlab.Gitlab(private_token=gitlab_private_token)

    logging.info('Pulling GitLab projects.')

    projects = {}
    for project_config in gitlab_projects:
        import_after = project_config.get('import_after')
        project = gl.projects.get(id=project_config['id'])
        projects[project.id] = {
            'project': project,
            'import_after': import_after
        }

    logging.info('Successfully pulled GitLab projects.')

    logging.info('Pulling GitLab tickets from projects.')

    issues_map = {}
    for project_id, project_data in projects.items():
        project = project_data['project']

        logging.info(f'Pulling tickets from {project.name}.')

        import_after = project_data['import_after'] or 0
        issues = project.issues.list(all=True)
        issues_map[project.id] = {}
        issues_map[project.id].update(
            {getattr(issue, gitlab_primary_key): issue for issue in issues if issue.iid > import_after}
        )

        logging.info(f'Successfully pulled tickets from {project.name}.')

    logging.info('Successfully pulled tickets from GitLab projects.')

    return issues_map


def create_airtable_records(records_to_create: [Dict]):
    table = Table(airtable_api_key, airtable_base_id, airtable_table_id)
    for record_data in records_to_create:
        table.create(fields=record_data)


def parse_ticket_to_record(gitlab_ticket: ProjectIssue) -> Dict:
    record_data = {}
    for gitlab_field, airtable_field in gitlab_to_airtable_field_map.items():
        if airtable_field:
            record_data[airtable_field] = getattr(gitlab_ticket, gitlab_field)

    return record_data


def sync():
    airtable_records_map = get_airtable_records()
    gitlab_tickets_by_project = get_gitlab_tickets()

    logging.info('Collecting missing GitLab tickets from Airtable.')

    airtable_records_to_create = []
    for tickets in gitlab_tickets_by_project.values():
        for ticket in tickets.values():
            # find GitLab tickets missing from Airtable
            if not airtable_records_map.get(getattr(ticket, gitlab_primary_key)):
                airtable_records_to_create.append(parse_ticket_to_record(ticket))

    if len(airtable_records_to_create) > 0:
        logging.info(f'{len(airtable_records_to_create)} Airtable records will be created.')

        create_airtable_records(airtable_records_to_create)

        logging.info(f'{len(airtable_records_to_create)} Airtable records were successfully created.')

    else:
        logging.info('No new Airtable records to create.')


if __name__ == '__main__':
    sync()
