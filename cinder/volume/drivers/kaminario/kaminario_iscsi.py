# Copyright (c) 2016 by Kaminario Technologies, Ltd.
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
"""Volume driver for Kaminario K2 all-flash arrays."""
import six

from cinder import exception
from cinder.i18n import _, _LW
from cinder import interface
from cinder.volume.drivers.kaminario import kaminario_common as common
from oslo_log import log as logging

ISCSI_TCP_PORT = "3260"
LOG = logging.getLogger(__name__)
kaminario_logger = common.kaminario_logger


@interface.volumedriver
class KaminarioISCSIDriver(common.KaminarioCinderDriver):
    """Kaminario K2 iSCSI Volume Driver."""

    @kaminario_logger
    def __init__(self, *args, **kwargs):
        super(KaminarioISCSIDriver, self).__init__(*args, **kwargs)
        self._protocol = 'iSCSI'

    @kaminario_logger
    def initialize_connection(self, volume, connector):
        """Get volume object and map to initiator host."""
        if type(volume).__name__ != 'RestObject':
            vol_name = self.get_volume_name(volume.id)
            LOG.debug("Searching volume : %s in K2.", vol_name)
            vol_rs = self.client.search("volumes", name=vol_name)
            if not hasattr(vol_rs, 'hits') or vol_rs.total == 0:
                msg = _("Unable to find volume: %s from K2.") % vol_name
                LOG.error(msg)
                raise exception.KaminarioCinderDriverException(reason=msg)
            vol = vol_rs.hits[0]
        else:
            vol = volume
        """Get target_portal"""
        LOG.debug("Searching first iscsi port ip  without wan in K2.")
        iscsi_ip_rs = self.client.search("system/net_ips", wan_port="")
        iscsi_ip = target_iqn = None
        if hasattr(iscsi_ip_rs, 'hits') and iscsi_ip_rs.total != 0:
            iscsi_ip = iscsi_ip_rs.hits[0].ip_address
        if not iscsi_ip:
            msg = _("Unable to get ISCSI IP addres from K2.")
            LOG.error(msg)
            raise exception.KaminarioCinderDriverException(reason=msg)
        iscsi_portal = "{0}:{1}".format(iscsi_ip, ISCSI_TCP_PORT)
        LOG.debug("Searching system state for target iqn in K2.")
        sys_state_rs = self.client.search("system/state")

        if hasattr(sys_state_rs, 'hits') and sys_state_rs.total != 0:
            target_iqn = sys_state_rs.hits[0].iscsi_qualified_target_name

        if not target_iqn:
            msg = _("Unable to get target iqn from K2.")
            LOG.error(msg)
            raise exception.KaminarioCinderDriverException(reason=msg)
        host_name = self.get_initiator_host_name(connector)
        LOG.debug("Searching initiator hostname: %s in K2.", host_name)
        host_rs = self.client.search("hosts", name=host_name)
        """Create a host if not exists."""
        if host_rs.total == 0:
            try:
                LOG.debug("Creating initiator hostname: %s in K2.", host_name)
                host = self.client.new("hosts", name=host_name,
                                       type="Linux").save()
                LOG.debug("Adding iqn: %(iqn)s to host: %(host)s in K2.",
                          {'iqn': connector['initiator'], 'host': host_name})
                iqn = self.client.new("host_iqns", iqn=connector['initiator'],
                                      host=host)
                iqn.save()
            except Exception as ex:
                LOG.debug("Unable to create host : %s in K2.", host_name)
                self.delete_host_by_name(host_name)
                raise exception.KaminarioCinderDriverException(
                    reason=six.text_type(ex.message))
        else:
            LOG.debug("Use existing initiator hostname: %s in K2.", host_name)
            host = host_rs.hits[0]
        try:
            LOG.debug("Mapping volume: %(vol)s to host: %(host)s",
                      {'host': host_name, 'vol': vol.name})
            mapping = self.client.new("mappings", volume=vol, host=host).save()
        except Exception as ex:
            if host_rs.total == 0:
                LOG.debug("Unable to mapping volume:%(vol)s to host: %(host)s",
                          {'host': host_name, 'vol': vol.name})
                self.delete_host_by_name(host_name)
            raise exception.KaminarioCinderDriverException(
                reason=six.text_type(ex.message))
        if type(volume).__name__ == 'RestObject':
            volsnap = None
            LOG.debug("Searching volsnaps in K2.")
            volsnaps = self.client.search("volsnaps")
            for v in volsnaps.hits:
                if v.snapshot.id == vol.id:
                    volsnap = v
                    break
            LOG.debug("Searching mapping of volsnap in K2.")
            rv = self.client.search("mappings", volume=volsnap)
            lun = rv.hits[0].lun

        else:
            lun = mapping.lun
        return {"driver_volume_type": "iscsi",
                "data": {"target_iqn": target_iqn,
                         "target_portal": iscsi_portal,
                         "target_lun": lun,
                         "target_discovered": True}}

    @kaminario_logger
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate connection of volume from host."""
        # Get volume object
        if type(volume).__name__ != 'RestObject':
            vol_name = self.get_volume_name(volume.id)
            LOG.debug("Searching volume: %s in K2.", vol_name)
            volume_rs = self.client.search("volumes", name=vol_name)
            if hasattr(volume_rs, "hits") and volume_rs.total != 0:
                volume = volume_rs.hits[0]
        else:
            vol_name = volume.name

        # Get host object.
        host_name = self.get_initiator_host_name(connector)
        host_rs = self.client.search("hosts", name=host_name)
        if hasattr(host_rs, "hits") and host_rs.total != 0 and volume:
            host = host_rs.hits[0]
            LOG.debug("Searching and deleting mapping of volume: %(name)s to "
                      "host: %(host)s", {'host': host_name, 'name': vol_name})
            map_rs = self.client.search("mappings", volume=volume, host=host)
            if hasattr(map_rs, "hits") and map_rs.total != 0:
                map_rs.hits[0].delete()
            if self.client.search("mappings", host=host).total == 0:
                LOG.debug("Deleting initiator hostname: %s in K2.", host_name)
                host.delete()
        else:
            LOG.warning(_LW("Host: %s not found on K2."), host_name)
