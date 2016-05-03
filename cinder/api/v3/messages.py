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

"""The messages API."""


from oslo_config import cfg
from oslo_log import log as logging
import webob
from webob import exc

from cinder.api.openstack import wsgi
from cinder.api.v3.views import messages as messages_view
from cinder import exception
from cinder.message import api as message_api
from cinder.message import defined_messages
import cinder.policy

CONF = cfg.CONF

LOG = logging.getLogger(__name__)
MESSAGES_BASE_MICRO_VERSION = '3.3'


def check_policy(context, action, target_obj=None):
    target = {
        'project_id': context.project_id,
        'user_id': context.user_id,
    }
    target.update(target_obj or {})

    _action = 'message:%s' % action
    cinder.policy.enforce(context, _action, target)


class MessagesController(wsgi.Controller):
    """The User Messages API controller for the OpenStack API."""

    _view_builder_class = messages_view.ViewBuilder

    def __init__(self, ext_mgr):
        self.message_api = message_api.API()
        self.ext_mgr = ext_mgr
        super(MessagesController, self).__init__()

    @wsgi.Controller.api_version(MESSAGES_BASE_MICRO_VERSION)
    def show(self, req, id):
        """Return the given message."""
        context = req.environ['cinder.context']

        try:
            message = self.message_api.get(context, id)
        except exception.MessageNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)

        check_policy(context, 'get', message)

        # Fetches message text based on event id passed to it.
        message['user_message'] = defined_messages.get_message_text(
            message['event_id'])

        return self._view_builder.detail(req, message)

    @wsgi.Controller.api_version(MESSAGES_BASE_MICRO_VERSION)
    def delete(self, req, id):
        """Delete a message."""
        context = req.environ['cinder.context']

        try:
            message = self.message_api.get(context, id)
            check_policy(context, 'delete', message)
            self.message_api.delete(context, message)
        except exception.MessageNotFound as error:
            raise exc.HTTPNotFound(explanation=error.msg)

        return webob.Response(status_int=204)

    @wsgi.Controller.api_version(MESSAGES_BASE_MICRO_VERSION)
    def index(self, req):
        """Returns a list of messages, transformed through view builder."""
        context = req.environ['cinder.context']
        check_policy(context, 'get_all')

        messages = self.message_api.get_all(context)

        for message in messages:
            # Fetches message text based on event id passed to it.
            user_message = defined_messages.get_message_text(
                message['event_id'])
            message['user_message'] = user_message

        messages = self._view_builder.index(req, messages)
        return messages


def create_resource(ext_mgr):
    return wsgi.Resource(MessagesController(ext_mgr))
