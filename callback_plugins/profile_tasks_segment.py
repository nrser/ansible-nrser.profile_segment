# -*- coding: utf-8 -*-
# (C) 2016, Joel, http://github.com/jjshoe
# (C) 2015, Tom Paine, <github@aioue.net>
# (C) 2014, Jharrod LaFon, @JharrodLaFon
# (C) 2012-2013, Michael DeHaan, <michael.dehaan@gmail.com>
#
# This file is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# File is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# See <http://www.gnu.org/licenses/> for a copy of the
# GNU General Public License

# Provides per-task timing, ongoing playbook elapsed time and
# ordered list of top 20 longest running tasks at end

# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import collections
import os
import time
import datetime
import analytics
import inspect
import yaml
import re

from ansible.module_utils.six.moves import reduce
from ansible.plugins.callback import CallbackBase
from ansible.errors import AnsibleError
from ansible.parsing.yaml.dumper import AnsibleDumper


# define start time
t0 = tn = time.time()


def to_yaml(obj, allow_unicode=True, default_flow_style=False, **kwds):
    return yaml.dump(
        obj,
        Dumper=AnsibleDumper,
        allow_unicode=allow_unicode,
        default_flow_style=default_flow_style,
        **kwds
    )


def secondsToStr(t):
    # http://bytes.com/topic/python/answers/635958-handy-short-cut-formatting-elapsed-time-floating-point-seconds
    def rediv(ll, b):
        return list(divmod(ll[0], b)) + ll[1:]

    return "%d:%02d:%02d.%03d" % tuple(reduce(rediv, [[t * 1000, ], 1000, 60, 60]))


def filled(msg, fchar="*"):
    if len(msg) == 0:
        width = 79
    else:
        msg = "%s " % msg
        width = 79 - len(msg)
    if width < 3:
        width = 3
    filler = fchar * width
    return "%s%s " % (msg, filler)


def timestamp(self):
    if self.current is not None:
        self.stats[self.current]['time'] = (
            time.time() - self.stats[self.current]['time']
        )


def tasktime():
    global tn
    time_current = time.strftime('%A %d %B %Y  %H:%M:%S %z')
    time_elapsed = secondsToStr(time.time() - tn)
    time_total_elapsed = secondsToStr(time.time() - t0)
    tn = time.time()
    return filled('%s (%s)%s%s' % (time_current, time_elapsed, ' ' * 7, time_total_elapsed))


# Tracking
# =====================================================================
# 
# I'm not sure why `ansible-profile` hommie declared these functions outside
# of CallbackModule, but I'll stick with it for now.
# 

def track_config(self, track_task):
    """
    Configure tracking.
    """
    
    analytics.write_key = track_task.args['write_key']
    
    if 'user' in track_task.args:
        self.track_config['user'] = track_task.args['user']
    
    if 'context' in track_task.args:
        self.track_config['context'].update(track_task.args['context'])
    
    if 'properties' in track_task.args:
        self.track_config['properties'].update(track_task.args['properties'])
    
    self.log("TRACKING CONFIGURED\n{0}", to_yaml(self.track_config))
# track_config


def track(self, event, properties):
    """
    Actually send data to Segment.
    """
    
    props = self.track_config['properties'].copy()
    props.update(properties)
    
    payload = {
        'properties': props,
        'context': self.track_config['context'],
    }
    
    analytics.track(self.track_config['user'], event, payload)
    
    self.log("TRACKED\n{}", to_yaml({
        'user': self.track_config['user'],
        'event': event,
        'payload': payload,
    }))
# track


def track_last_task(self, track_task):
    stats = self.stats[self.current]
    
    # self._display.display("ARGS: {0}".format(track_task.args))
    
    # vars = track_task.get_vars()
    # self._display.display("VARS KEYS: {0}".format(vars.keys()))
    
    properties = {
        # no bueno:
        # 'web_version': vars['web_version']
    }
    
    if 'properties' in track_task.args:
        properties.update(track_task.args['properties'])
    
    properties.update({
        'role': track_task._role.get_name(),
        'seconds': stats['time'],
    })
    
    analytics.track(
        os.getenv('USER'),
        track_task.args['event'],
        properties
    )
# track_last_task


def track_start(self, track_task):
    """
    Start timing an event to track.
    """
    
    # Get the event from the args
    event = track_task.args['event']
    
    # Make sure we haven't started this event already
    if event in self.track_blocks:
        raise AnsibleError("Already started event {0}".format(event))
    
    properties = {
        # Though the role *could* be different at the start and end of the
        # tracking, to keep things simple I'm going to pick one, and this seems
        # like the one to pick.
        'role': track_task._role.get_name(),
    }
    
    if 'properties' in track_task.args:
        properties.update(track_task.args['properties'])
    
    # A "track block" is just a simple dict
    self.track_blocks[event] = {
        # *I* like datetime / timedelta because they naturally print nice,
        # but for now I'll stick with `time` like the rest of this...
        # 
        # We get called *after* the track_start task has returned, so we want
        # to use the current time to start the timer.
        'start': time.time(),
        
        # Add any properties passed to the task, defaulting to an empty dict.
        'properties': properties,
    }
    
    self.log(
        "TRACK START {0}\n{1}",
        event,
        to_yaml(self.track_blocks[event])
    )
# track_start


def track_end(self, track_task):
    """
    Stop timing an event.
    """
    
    # Get the event from the args
    event = track_task.args['event']
    
    if event not in self.track_blocks:
        raise AnsibleError("Never started tracking event {0}!".format(event))
    
    # Add the end and delta info to the block
    
    track_block = self.track_blocks[event]
    
    # `tn` is the time that the last task started, which would be the time
    # the `task_end` task started, since we're getting called in the "ok"
    # return phase, so take that as the end time.
    # 
    # Seems like it only adds ~100-200ms thought so not a huge deal either way
    track_block['end'] = tn
    
    track_block['properties']['seconds'] = (
        track_block['end'] - track_block['start']
    )
    
    self._display.display(
        "TRACK END {0}: {1}".format(
            event,
            datetime.timedelta(seconds=track_block['properties']['seconds'])
        )
    )
    
    track(self, event, track_block['properties'])
# track_end


class CallbackModule(CallbackBase):
    """
    This callback module provides per-task timing, ongoing playbook elapsed time
    and ordered list of top 20 longest running tasks at end.
    """
    
    
    # Constants
    # =====================================================================
    
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'profile_tasks_segment'
    CALLBACK_NEEDS_WHITELIST = True
    
    
    # Constructor
    # =====================================================================
    
    def __init__(self):
        self.stats = collections.OrderedDict()
        self.current = None
        
        self.sort_order = os.getenv(
            'PROFILE_TASKS_SORT_ORDER',
            True
        )
        
        self.task_output_limit = os.getenv(
            'PROFILE_TASKS_TASK_OUTPUT_LIMIT',
            20
        )

        if self.sort_order == 'ascending':
            self.sort_order = False

        if self.task_output_limit == 'all':
            self.task_output_limit = None
        else:
            self.task_output_limit = int(self.task_output_limit)
        
        # Tracking
        # ========
        
        self.track_config = {
            # User to track events as, defaults to system user.
            # Update via `track_config` tasks.
            'user': os.getenv('USER'),
        
            # Context for events. Update via `track_config` tasks. Strict, see:
            # 
            # https://segment.com/docs/spec/common/#context
            'context': {},
            
            # Default properties for events. Proprties passed in tasks override
            # (shallow merge). Update via `track_config` tasks. Anything goes
            # here.
            # 
            # https://segment.com/docs/spec/track/#properties
            'properties': {},
        }
        
        # The things that are started by `track_start` and ended with
        # `track_end` actions.
        self.track_blocks = collections.OrderedDict()

        super(CallbackModule, self).__init__()
    
    
    
    # Instance Methods
    # =====================================================================
    
    # Custom Extenions
    # ---------------------------------------------------------------------
    # 
    # Things added in this plugin (by myself or from ansible-profile)
    # 
    
    def log(self, tpl, *args):
        """
        Display a formatted message.
        """
        self._display.display(tpl.format(*args))
    
    
    def _record_task(self, task):
        """
        Logs the start of each task
        """
        self._display.display(tasktime())
        timestamp(self)

        # Record the start time of the current task
        self.current = task._uuid
        self.stats[self.current] = {
            'time': time.time(),
            'name': task.get_name()
        }
        if self._display.verbosity >= 2:
            self.stats[self.current]['path'] = task.get_path()
            
    
    
    # Ansible Callbacks
    # ---------------------------------------------------------------------
    
    def v2_runner_on_ok(self, result):
        task = result._task
        
        if task.action == u'track_config':
            track_config(self, task)
            
        elif task.action == u'track_start':
            track_start(self, task)
            
        elif task.action == u'track_end':
            track_end(self, task)
        
        elif task.action == u'track_last_task':
            track_last_task(self, task)

    def v2_playbook_on_task_start(self, task, is_conditional):
        self._record_task(task)

    def v2_playbook_on_handler_task_start(self, task):
        self._record_task(task)

    def playbook_on_setup(self):
        self._display.display(tasktime())

    def playbook_on_stats(self, stats):
        self._display.display(tasktime())
        self._display.display(filled("", fchar="="))

        timestamp(self)

        results = self.stats.items()

        # Sort the tasks by the specified sort
        if self.sort_order != 'none':
            results = sorted(
                self.stats.items(),
                key=lambda x: x[1]['time'],
                reverse=self.sort_order,
            )

        # Display the number of tasks specified or the default of 20
        results = results[:self.task_output_limit]

        # Print the timings
        for uuid, result in results:
            name = re.sub(r'\ +', ' ', result['name'].replace(u"\n", u'␤'))
            
            if len(name) > 65:
                name = name[:62] + u'...'
            
            msg = u"{0:-<{2}}{1:->9}".format(
                name + u' ',
                u' {0}'.format(datetime.timedelta(seconds=result['time'])),
                self._display.columns - 9
            )
            
            if 'path' in result:
                msg += u"\n{0:-<{1}}".format(
                    result['path'] + u' ',
                    self._display.columns
                )
            self._display.display(msg)
        
        # *** NEED THIS *** or you lose events?!
        if analytics.write_key:
            analytics.flush()
