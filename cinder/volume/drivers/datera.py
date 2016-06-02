# Copyright 2016 Datera
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import time
import uuid
import functools

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import units
import requests
import six
import ipaddress

from cinder import context
from cinder import exception
from cinder.i18n import _, _LE, _LI
from cinder import utils
from cinder.volume.drivers.san import san
from cinder.volume import qos_specs
from cinder.volume import volume_types

# import sys
# import logging
# logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
# LOG = logging.getLogger(__name__)
LOG = logging.getLogger()
# LOG.setLevel(logging.DEBUG)

d_opts = [
    cfg.StrOpt('datera_api_port',
               default='7717',
               help='Datera API port.'),
    cfg.StrOpt('datera_api_version',
               default='2',
               help='Datera API version.'),
    cfg.IntOpt('datera_503_timeout',
               default='120',
               help='Timeout for HTTP 503 retry messages'),
    cfg.IntOpt('datera_503_interval',
               default='5',
               help='Interval between 503 retries'),
    cfg.BoolOpt('datera_debug',
                default=False,
                help="True to set function arg and return logging")
]


CONF = cfg.CONF
CONF.import_opt('driver_use_ssl', 'cinder.volume.driver')
CONF.register_opts(d_opts)

# Recursive dict to assemble basic url structure for the most common
# API URL endpoints. Most others are constructed from these
URL_TEMPLATES = {
    'ai': lambda: 'app_instances',
    'ai_inst': lambda: (URL_TEMPLATES['ai']() + '/{}'),
    'si': lambda: (URL_TEMPLATES['ai_inst']() + '/storage_instances'),
    'si_inst': lambda storage_name: (
        (URL_TEMPLATES['si']() + '/{}').format(
            '{}', storage_name)),
    'vol': lambda storage_name: (
        (URL_TEMPLATES['si_inst'](storage_name) + '/volumes')),
    'vol_inst': lambda storage_name, volume_name: (
        (URL_TEMPLATES['vol'](storage_name) + '/{}').format(
            '{}', volume_name))}


def _authenticated(func):
    """Ensure the driver is authenticated to make a request.

    In do_setup() we fetch an auth token and store it. If that expires when
    we do API request, we'll fetch a new one.
    """
    @functools.wraps(func)
    def func_wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except exception.NotAuthorized:
            # Prevent recursion loop. After the self arg is the
            # resource_type arg from _issue_api_request(). If attempt to
            # login failed, we should just give up.
            if args[0] == 'login':
                raise

            # Token might've expired, get a new one, try again.
            self._login()
            return func(self, *args, **kwargs)
    return func_wrapper


@six.add_metaclass(utils.TraceWrapperWithABCMetaclass)
class DateraDriver(san.SanISCSIDriver):

    """The OpenStack Datera Driver

    Version history:
        1.0 - Initial driver
        1.1 - Look for lun-0 instead of lun-1.
        2.0 - Update For Datera API v2
        2.1 - Multipath, ACL and reorg
        2.2 - Capabilites List, Extended Volume-Type Support
    """
    VERSION = '2.2'

    def __init__(self, *args, **kwargs):
        super(DateraDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(d_opts)
        self.num_replicas = 3
        self.username = self.configuration.san_login
        self.password = self.configuration.san_password
        self.cluster_stats = {}
        self.datera_api_token = None
        self.interval = self.configuration.datera_503_interval
        self.retry_attempts = (self.configuration.datera_503_timeout /
                               self.interval)
        self.driver_prefix = str(uuid.uuid4())[:4]
        self.datera_debug = self.configuration.datera_debug

        if self.datera_debug:
            utils.setup_tracing(['method'])

    def do_setup(self, context):
        # If we can't authenticate through the old and new method, just fail
        # now.
        if not all([self.username, self.password]):
            msg = _("san_login and/or san_password is not set for Datera "
                    "driver in the cinder.conf. Set this information and "
                    "start the cinder-volume service again.")
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        self._login()

    @utils.retry(exception.VolumeDriverException, retries=3)
    def _wait_for_resource(self, id, resource_type, policies):
        result = self._issue_api_request(resource_type, 'get', id)
        if result['storage_instances'][
                policies['default_storage_name']]['volumes'][
                policies['default_volume_name']]['op_state'] == 'available':
            return
        else:
            raise exception.VolumeDriverException(
                message=_('Resource not ready.'))

    def _create_resource(self, resource, resource_type, body):

        result = None
        try:
            result = self._issue_api_request(resource_type, 'post', body=body)
        except exception.Invalid:
            type_id = resource.get('volume_type_id', None)
            if resource_type == 'volumes' and type_id:
                LOG.error(_LE("Creation request failed. Please verify the "
                              "extra-specs set for your volume types are "
                              "entered correctly."))
            raise
        else:
            policies = self._get_policies_for_resource(resource)
            # Handle updating QOS Policies
            if resource_type == URL_TEMPLATES['ai']():
                self._update_qos(resource, policies)
            if result['storage_instances'][policies['default_storage_name']][
                    'volumes'][policies['default_volume_name']][
                        'op_state'] == 'available':
                return
            self._wait_for_resource(resource['id'], resource_type, policies)

    def create_volume(self, volume):
        """Create a logical volume."""
        # Generate App Instance, Storage Instance and Volume
        # Volume ID will be used as the App Instance Name
        # Storage Instance and Volumes will have standard names
        policies = self._get_policies_for_resource(volume)
        num_replicas = int(policies['replica_count'])
        storage_name = policies['default_storage_name']
        volume_name = policies['default_volume_name']

        app_params = (
            {
                'create_mode': "openstack",
                'uuid': str(volume['id']),
                'name': str(volume['id']),
                'access_control_mode': 'deny_all',
                'storage_instances': {
                    storage_name: {
                        'name': storage_name,
                        'volumes': {
                            volume_name: {
                                'name': volume_name,
                                'size': volume['size'],
                                'replica_count': num_replicas,
                                'snapshot_policies': {
                                }
                            }
                        }
                    }
                }
            })
        self._create_resource(volume, URL_TEMPLATES['ai'](), body=app_params)

    def extend_volume(self, volume, new_size):
        # Offline App Instance, if necessary
        reonline = False
        app_inst = self._issue_api_request(
            URL_TEMPLATES['ai_inst']().format(volume['id']))
        if app_inst['admin_state'] == 'online':
            reonline = True
            self.detach_volume(None, volume, delete_initiator=False)
        # Change Volume Size
        app_inst = volume['id']
        data = {
            'size': new_size
        }
        policies = self._get_policies_for_resource(volume)
        self._issue_api_request(
            URL_TEMPLATES['vol_inst'](
                policies['default_storage_name'],
                policies['default_volume_name']).format(app_inst),
            method='put',
            body=data)
        # Online Volume, if it was online before
        if reonline:
            self.create_export(None, volume, None)

    def create_cloned_volume(self, volume, src_vref):
        policies = self._get_policies_for_resource(volume)
        src = "/" + URL_TEMPLATES['vol_inst'](
            policies['default_storage_name'],
            policies['default_volume_name']).format(src_vref['id'])
        data = {
            'create_mode': 'openstack',
            'name': str(volume['id']),
            'uuid': str(volume['id']),
            'clone_src': src,
        }
        self._issue_api_request(URL_TEMPLATES['ai'](), 'post', body=data)

        if volume['size'] > src_vref['size']:
            self.extend_volume(volume, volume['size'])

    def delete_volume(self, volume):
        self.detach_volume(None, volume)
        app_inst = volume['id']
        try:
            self._issue_api_request(URL_TEMPLATES['ai_inst']().format(
                app_inst),
                method='delete')
        except exception.NotFound:
            msg = _LI("Tried to delete volume %s, but it was not found in the "
                      "Datera cluster. Continuing with delete.")
            LOG.info(msg, volume['id'])

    def ensure_export(self, context, volume, connector):
        """Gets the associated account, retrieves CHAP info and updates."""
        return self.create_export(context, volume, connector)

    def initialize_connection(self, volume, connector):
        # Now online the app_instance (which will online all storage_instances)
        multipath = connector.get('multipath', False)
        url = URL_TEMPLATES['ai_inst']().format(volume['id'])
        data = {
            'admin_state': 'online'
        }
        app_inst = self._issue_api_request(url, method='put', body=data)
        storage_instances = app_inst["storage_instances"]
        si_names = storage_instances.keys()

        portal = storage_instances[si_names[0]]['access']['ips'][0] + ':3260'
        iqn = storage_instances[si_names[0]]['access']['iqn']
        if multipath:
            portals = [p + ':3260' for p in
                       storage_instances[si_names[0]]['access']['ips']]
            iqns = [iqn for _ in
                    storage_instances[si_names[0]]['access']['ips']]
            lunids = [self._get_lunid() for _ in
                      storage_instances[si_names[0]]['access']['ips']]

            return {
                'driver_volume_type': 'iscsi',
                'data': {
                    'target_discovered': False,
                    'target_iqn': iqn,
                    'target_iqns': iqns,
                    'target_portal': portal,
                    'target_portals': portals,
                    'target_lun': self._get_lunid(),
                    'target_luns': lunids,
                    'volume_id': 1,
                    'discard': False}}
        else:
            return {
                'driver_volume_type': 'iscsi',
                'data': {
                    'target_discovered': False,
                    'target_iqn': iqn,
                    'target_portal': portal,
                    'target_lun': self._get_lunid(),
                    'volume_id': 1,
                    'discard': False}}

    def create_export(self, context, volume, connector):
        # Online volume in case it hasn't been already
        url = URL_TEMPLATES['ai_inst']().format(volume['id'])
        data = {
            'admin_state': 'online'
        }
        self._issue_api_request(url, method='put', body=data)
        # Check if we've already setup everything for this volume
        url = (URL_TEMPLATES['si']().format(volume['id']))
        storage_instances = self._issue_api_request(url)
        # Handle adding initiator to product if necessary
        # Then add initiator to ACL
        policies = self._get_policies_for_resource(volume)
        if (connector and
                connector.get('initiator') and
                not policies['acl_allow_all']):
            initiator_name = "OpenStack_{}_{}".format(
                self.driver_prefix, str(uuid.uuid4())[:4])
            initiator_group = 'IG-' + volume['id']
            found = False
            initiator = connector['initiator']
            current_initiators = self._issue_api_request('initiators')
            for iqn, values in current_initiators.items():
                if initiator == iqn:
                    found = True
                    break
            # If we didn't find a matching initiator, create one
            if not found:
                data = {'id': initiator, 'name': initiator_name}
                # Try and create the initiator
                # If we get a conflict, ignore it because race conditions
                self._issue_api_request("initiators",
                                        method="post",
                                        body=data,
                                        conflict_ok=True)
            # Create initiator group with initiator in it
            initiator_path = "/initiators/{}".format(initiator)
            initiator_group_path = "/initiator_groups/{}".format(
                initiator_group)
            ig_data = {'name': initiator_group, 'members': [initiator_path]}
            self._issue_api_request("initiator_groups",
                                    method="post",
                                    body=ig_data,
                                    conflict_ok=True)
            # Create ACL with initiator group as reference for each
            # storage_instance in app_instance
            # TODO: We need to avoid changing the ACLs if the template already
            # specifies an ACL policy.
            for si_name in storage_instances.keys():
                acl_url = (URL_TEMPLATES['si']() + "/{}/acl_policy").format(
                    volume['id'], si_name)
                data = {'initiator_groups': [initiator_group_path]}
                self._issue_api_request(acl_url,
                                        method="put",
                                        body=data)

        if connector and connector.get('ip'):
            # Determine IP Pool from IP and update storage_instance
            try:
                initiator_ip_pool_path = self._get_ip_pool_for_string_ip(
                    connector['ip'])
                policies = self._get_policies_for_resource(volume)

                ip_pool_url = URL_TEMPLATES['si_inst'](
                    policies['default_storage_name']).format(
                    volume['id'])
                ip_pool_data = {'ip_pool': initiator_ip_pool_path}
                self._issue_api_request(ip_pool_url,
                                        method="put",
                                        body=ip_pool_data)
            except exception.DateraAPIException:
                # Datera product 1.0 support
                pass

    def detach_volume(self, context, volume, attachment=None):
        url = URL_TEMPLATES['ai_inst']().format(volume['id'])
        data = {
            'admin_state': 'offline',
            'force': True
        }
        try:
            self._issue_api_request(url, method='put', body=data)
        except exception.NotFound:
            msg = _LI("Tried to detach volume %s, but it was not found in the "
                      "Datera cluster. Continuing with detach.")
            LOG.info(msg, volume['id'])
        # TODO: Make acl cleaning multi-attach aware
        self._clean_acl(volume)

    def _check_for_acl(self, initiator_path):
        """ Returns True if an acl is found for initiator_path """
        # TODO when we get a /initiators/:initiator/acl_policies endpoint
        # use that instead of this monstrosity
        initiator_groups = self._issue_api_request("initiator_groups")
        for ig, igdata in initiator_groups.items():
            if initiator_path in igdata['members']:
                LOG.debug("Found initiator_group: %s for initiator: %s",
                          ig, initiator_path)
                return True
        LOG.debug("No initiator_group found for initiator: %s", initiator_path)
        return False

    def _clean_acl(self, volume):
        policies = self._get_policies_for_resource(volume)
        acl_url = (URL_TEMPLATES["si_inst"](
            policies['default_storage_name']) + "/acl_policy").format(
            volume['id'])
        try:
            initiator_group = self._issue_api_request(acl_url)[
                'initiator_groups'][0]
            initiator_iqn_path = self._issue_api_request(
                initiator_group.lstrip("/"))["members"][0]
            # Clear out ACL and delete initiator group
            self._issue_api_request(acl_url,
                                    method="put",
                                    body={'initiator_groups': []})
            self._issue_api_request(initiator_group.lstrip("/"),
                                    method="delete")
            if not self._check_for_acl(initiator_iqn_path):
                self._issue_api_request(initiator_iqn_path.lstrip("/"),
                                        method="delete")
        except (IndexError, exception.NotFound):
            LOG.debug("Did not find any initiator groups for volume: %s",
                      volume)

    def create_snapshot(self, snapshot):
        policies = self._get_policies_for_resource(snapshot)
        url_template = URL_TEMPLATES['vol_inst'](
            policies['default_storage_name'],
            policies['default_volume_name']) + '/snapshots'
        url = url_template.format(snapshot['volume_id'])

        snap_params = {
            'uuid': snapshot['id'],
        }
        self._issue_api_request(url, method='post', body=snap_params)

    def delete_snapshot(self, snapshot):
        policies = self._get_policies_for_resource(snapshot)
        snap_temp = URL_TEMPLATES['vol_inst'](
            policies['default_storage_name'],
            policies['default_volume_name']) + '/snapshots'
        snapu = snap_temp.format(snapshot['volume_id'])
        snapshots = self._issue_api_request(snapu, method='get')

        try:
            for ts, snap in snapshots.items():
                if snap['uuid'] == snapshot['id']:
                    url_template = snapu + '/{}'
                    url = url_template.format(ts)
                    self._issue_api_request(url, method='delete')
                    break
            else:
                raise exception.NotFound
        except exception.NotFound:
            msg = _LI("Tried to delete snapshot %s, but was not found in "
                      "Datera cluster. Continuing with delete.")
            LOG.info(msg, snapshot['id'])

    def create_volume_from_snapshot(self, volume, snapshot):
        policies = self._get_policies_for_resource(snapshot)
        snap_temp = URL_TEMPLATES['vol_inst'](
            policies['default_storage_name'],
            policies['default_volume_name']) + '/snapshots'
        snapu = snap_temp.format(snapshot['volume_id'])
        snapshots = self._issue_api_request(snapu, method='get')
        for ts, snap in snapshots.items():
            if snap['uuid'] == snapshot['id']:
                found_ts = ts
                break
        else:
            raise exception.NotFound

        src = "/" + (snap_temp + '/{}').format(snapshot['volume_id'], found_ts)
        app_params = (
            {
                'create_mode': 'openstack',
                'uuid': str(volume['id']),
                'name': str(volume['id']),
                'clone_src': src,
            })
        self._issue_api_request(
            URL_TEMPLATES['ai'](),
            method='post',
            body=app_params)

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update first.
        The name is a bit misleading as
        the majority of the data here is cluster
        data.
        """
        if refresh or not self.cluster_stats:
            try:
                self._update_cluster_stats()
            except exception.DateraAPIException:
                LOG.error(_LE('Failed to get updated stats from Datera '
                              'cluster.'))
        return self.cluster_stats

    def _update_cluster_stats(self):
        LOG.debug("Updating cluster stats info.")

        results = self._issue_api_request('system')

        if 'uuid' not in results:
            LOG.error(_LE('Failed to get updated stats from Datera Cluster.'))

        backend_name = self.configuration.safe_get('volume_backend_name')
        stats = {
            'volume_backend_name': backend_name or 'Datera',
            'vendor_name': 'Datera',
            'driver_version': self.VERSION,
            'storage_protocol': 'iSCSI',
            'total_capacity_gb': int(results['total_capacity']) / units.Gi,
            'free_capacity_gb': int(results['available_capacity']) / units.Gi,
            'reserved_percentage': 0,
        }

        self.cluster_stats = stats

    def _login(self):
        """Use the san_login and san_password to set token."""
        body = {
            'name': self.username,
            'password': self.password
        }

        # Unset token now, otherwise potential expired token will be sent
        # along to be used for authorization when trying to login.

        try:
            LOG.debug('Getting Datera auth token.')
            results = self._issue_api_request('login', 'put', body=body,
                                              sensitive=True)
            self.datera_api_token = results['key']
        except exception.NotAuthorized:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('Logging into the Datera cluster failed. Please '
                              'check your username and password set in the '
                              'cinder.conf and start the cinder-volume '
                              'service again.'))

    def _get_lunid(self):
        return 0

    def _init_vendor_properties(self):
        """Create a dictionary of vendor unique properties.

        This method creates a dictionary of vendor unique properties
        and returns both created dictionary and vendor name.
        Returned vendor name is used to check for name of vendor
        unique properties.

        - Vendor name shouldn't include colon(:) because of the separator
          and it is automatically replaced by underscore(_).
          ex. abc:d -> abc_d
        - Vendor prefix is equal to vendor name.
          ex. abcd
        - Vendor unique properties must start with vendor prefix + ':'.
          ex. abcd:maxIOPS

        Each backend driver needs to override this method to expose
        its own properties using _set_property() like this:

        self._set_property(
            properties,
            "vendorPrefix:specific_property",
            "Title of property",
            _("Description of property"),
            "type")

        : return dictionary of vendor unique properties
        : return vendor name

        prefix: DF --> Datera Fabric
        """

        properties = {}
        self._set_property(
            properties,
            "DF:replica_count",
            "Datera Volume Replica Count",
            _("Specifies number of replicas for each volume"),
            "integer",
            minimum=1,
            default=3)

        self._set_property(
            properties,
            "DF:acl_allow_all",
            "Datera ACL Allow All",
            _("True to set acl 'allow_all' on volumes created"),
            "boolean",
            default=False)

        # ###### QoS Settings ###### #
        self._set_property(
            properties,
            "DF:read_bandwidth_max",
            "Datera QoS Max Bandwidth Read",
            _("Max read bandwidth setting for volume qos, "
              "use 0 for unlimited"),
            "integer",
            minimum=0,
            default=0)

        self._set_property(
            properties,
            "DF:default_storage_name",
            "Datera Default Storage Instance Name",
            _("The name to use for storage instances created"),
            "string",
            default="storage-1")

        self._set_property(
            properties,
            "DF:default_volume_name",
            "Datera Default Volume Name",
            _("The name to use for volumes created"),
            "string",
            default="volume-1")

        self._set_property(
            properties,
            "DF:write_bandwidth_max",
            "Datera QoS Max Bandwidth Write",
            _("Max write bandwidth setting for volume qos, "
              "use 0 for unlimited"),
            "integer",
            minimum=0,
            default=0)

        self._set_property(
            properties,
            "DF:total_bandwidth_max",
            "Datera QoS Max Bandwidth Total",
            _("Max total bandwidth setting for volume qos, "
              "use 0 for unlimited"),
            "integer",
            minimum=0,
            default=0)

        self._set_property(
            properties,
            "DF:read_iops_max",
            "Datera QoS Max iops Read",
            _("Max read iops setting for volume qos, "
              "use 0 for unlimited"),
            "integer",
            minimum=0,
            default=0)

        self._set_property(
            properties,
            "DF:write_iops_max",
            "Datera QoS Max IOPS Write",
            _("Max write iops setting for volume qos, "
              "use 0 for unlimited"),
            "integer",
            minimum=0,
            default=0)

        self._set_property(
            properties,
            "DF:total_iops_max",
            "Datera QoS Max IOPS Total",
            _("Max total iops setting for volume qos, "
              "use 0 for unlimited"),
            "integer",
            minimum=0,
            default=0)
        # ###### End QoS Settings ###### #

        return properties, 'DF'

    def _get_policies_for_resource(self, resource):
        """Get extra_specs and qos_specs of a volume_type.

        This fetches the scoped keys from the volume type. Anything set from
         qos_specs will override key/values set from extra_specs.
        """
        type_id = resource.get('volume_type_id', None)
        # Handle case of volume with no type.  We still want the
        # specified defaults from above
        if type_id:
            ctxt = context.get_admin_context()
            volume_type = volume_types.get_volume_type(ctxt, type_id)
            specs = volume_type.get('extra_specs')
        else:
            volume_type = None
            specs = {}

        # Set defaults:
        policies = {k.lstrip('DF:'): str(v['default']) for (k, v)
                    in self._init_vendor_properties()[0].items()}

        if volume_type:
            # Populate updated value
            for key, value in specs.items():
                if ':' in key:
                    fields = key.split(':')
                    key = fields[1]
                    policies[key] = value

            qos_specs_id = volume_type.get('qos_specs_id')
            if qos_specs_id is not None:
                qos_kvs = qos_specs.get_qos_specs(ctxt, qos_specs_id)['specs']
                if qos_kvs:
                    policies.update(qos_kvs)
        # Cast everything except booleans int that can be cast
        for k, v in policies.items():
            # Handle String Boolean case
            if v == 'True' or v == 'False':
                policies[k] = policies[k] == 'True'
                continue
            # Int cast
            try:
                policies[k] = int(v)
            except ValueError:
                pass
        return policies

    def _update_qos(self, resource, policies):
        url = URL_TEMPLATES['vol_inst'](
            policies['default_storage_name'],
            policies['default_volume_name']) + '/performance_policy'
        url = url.format(resource['id'])
        type_id = resource.get('volume_type_id', None)
        if type_id is not None:
            # Filter for just QOS policies in result. All of their keys
            # should end with "max"
            fpolicies = {k: int(v) for k, v in
                         policies.items() if k.endswith("max")}
            # Filter all 0 values from being passed
            fpolicies = dict(filter(lambda (_, v): v > 0, fpolicies.items()))
            if fpolicies:
                self._issue_api_request(url, 'post', body=fpolicies)

    def _get_ip_pool_for_string_ip(self, ip):
        """ Takes a string ipaddress and return the ip_pool API object dict """
        pool = 'default'
        ip_obj = ipaddress.ip_address(six.text_type(ip))
        ip_pools = self._issue_api_request("access_network_ip_pools")
        for ip_pool, ipdata in ip_pools.items():
            for access, adata in ipdata['network_paths'].items():
                if not adata.get('start_ip'):
                    continue
                pool_if = ipaddress.ip_interface(
                    "/".join((adata['start_ip'], str(adata['netmask']))))
                if ip_obj in pool_if.network:
                    pool = ip_pool
        return self._issue_api_request(
            "access_network_ip_pools/{}".format(pool))['path']

    def _request(self, connection_string, method, payload, header, cert_data):
        LOG.debug("Endpoint for Datera API call: %s", connection_string)
        try:
            response = getattr(requests, method)(connection_string,
                                                 data=payload, headers=header,
                                                 verify=False, cert=cert_data)
            return response
        except requests.exceptions.RequestException as ex:
            msg = _(
                'Failed to make a request to Datera cluster endpoint due '
                'to the following reason: %s') % six.text_type(
                ex.message)
            LOG.error(msg)
            raise exception.DateraAPIException(msg)

    def _raise_response(self, response):
        msg = _('Request to Datera cluster returned bad status:'
                ' %(status)s | %(reason)s') % {
                    'status': response.status_code,
                    'reason': response.reason}
        LOG.error(msg)
        raise exception.DateraAPIException(msg)

    def _handle_bad_status(self,
                           response,
                           connection_string,
                           method,
                           payload,
                           header,
                           cert_data,
                           sensitive=False,
                           conflict_ok=False):
        if not sensitive:
            LOG.debug(("Datera Response URL: %s\n"
                       "Datera Response Payload: %s\n"
                       "Response Object: %s\n"),
                      response.url,
                      payload,
                      vars(response))
        if response.status_code == 404:
            raise exception.NotFound(response.json()['message'])
        elif response.status_code in [403, 401]:
            raise exception.NotAuthorized()
        elif response.status_code == 409 and conflict_ok:
            # Don't raise, because we're expecting a conflict
            pass
        elif response.status_code == 503:
            # TODO Try again here
            current_retry = 0
            while current_retry <= self.retry_attempts:
                LOG.debug("Datera 503 response, trying request again")
                time.sleep(self.interval)
                resp = self._request(connection_string,
                                     method,
                                     payload,
                                     header,
                                     cert_data)
                if resp.ok:
                    return response.json()
                elif resp.status_code != 503:
                    self._raise_response(resp)
        else:
            self._raise_response(response)

    @_authenticated
    def _issue_api_request(self, resource_url, method='get', body=None,
                           sensitive=False, conflict_ok=False):
        """All API requests to Datera cluster go through this method.

        :param resource_url: the url of the resource
        :param method: the request verb
        :param body: a dict with options for the action_type
        :returns: a dict of the response from the Datera cluster
        """
        host = self.configuration.san_ip
        port = self.configuration.datera_api_port
        api_token = self.datera_api_token
        api_version = self.configuration.datera_api_version

        payload = json.dumps(body, ensure_ascii=False)
        payload.encode('utf-8')

        header = {'Content-Type': 'application/json; charset=utf-8'}

        protocol = 'http'
        if self.configuration.driver_use_ssl:
            protocol = 'https'

        if api_token:
            header['Auth-Token'] = api_token

        client_cert = self.configuration.driver_client_cert
        client_cert_key = self.configuration.driver_client_cert_key
        cert_data = None

        if client_cert:
            protocol = 'https'
            cert_data = (client_cert, client_cert_key)

        connection_string = '%s://%s:%s/v%s/%s' % (protocol, host, port,
                                                   api_version, resource_url)

        response = self._request(connection_string,
                                 method,
                                 payload,
                                 header,
                                 cert_data)

        data = response.json()

        if not response.ok:
            self._handle_bad_status(response,
                                    connection_string,
                                    method,
                                    payload,
                                    header,
                                    cert_data,
                                    conflict_ok=conflict_ok)

        return data
