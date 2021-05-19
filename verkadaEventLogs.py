#!/usr/bin/env python3

import sys,os,getopt
import traceback
import os
import fcntl
import json
import requests
import time
from datetime import datetime
from datetime import timedelta
import base64

from six import PY2

if PY2:
    get_unicode_string = unicode
else:
    get_unicode_string = str

sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

from html.parser import HTMLParser

class integration(object):

    JSON_field_mappings = {
        'created' : 'timestamp'
    }

    def verkada_getCameras(self):
        response = self.verkada_request('/cameras')
        r_json = response.json()
        return r_json['cameras']

    def verkada_getEvents(self):
        pagesize = 10
        total_events = []
        params = {
                'start_time': self.last_run,
                'end_time': self.current_run,
                'per_page': pagesize

            }
        response = self.verkada_request('/notifications', params = params)
        r_json = response.json()
        page_cursor = r_json['page_cursor']
        events = r_json['notifications']
        if page_cursor == None:
            return events
        total_events += events
        while page_cursor != None:
            params = {
                'start_time': self.last_run,
                'end_time': self.current_run,
                'per_page': pagesize,
                'page_cursor': page_cursor
                }
            response = self.verkada_request('/notifications', params = params)
            r_json = response.json()
            events = r_json['notifications']
            total_events += events
            page_cursor = r_json['page_cursor']
        return total_events


    def verkada_request(self, path, params = None, verify=False, proxies=None):
        url = self.api_url + '/orgs/' + self.org_id + path
        headers = {
                'Content-Type': 'applicaiton/json',
                'x-api-key': self.api_key
            }
        self.ds.log('INFO', "Attempting to connect to url: " + url + " with params: " + json.dumps(params))
        try:
            response = requests.get(url, headers=headers, params = params, timeout=15,
                                    verify=verify, proxies=proxies)
        except Exception as e:
            self.ds.log('ERROR', "Exception in verkada_request: {0}".format(str(e)))
            return None
        if not response or response.status_code != 200:
            self.ds.log('ERROR', "Received unexpected " + str(response.text) + " response from Brivo Server {0}.".format(url))
            self.ds.log('ERROR', "Exiting due to unexpected response.")
            sys.exit(0)
        return response



    def verkada_main(self): 

        self.api_url = self.ds.config_get('verkada', 'api_url')
        self.state_dir = self.ds.config_get('verkada', 'state_dir')
        self.org_id = self.ds.config_get('verkada', 'org_id')
        self.api_key = self.ds.config_get('verkada', 'api_key')
        self.last_run = self.ds.get_state(self.state_dir)
        self.current_run = int(time.time())
        if self.last_run == None:
            self.last_run = self.current_run - (86400 * 30)

        cameras_list = self.verkada_getCameras()
        cameras = {} 
        for camera in cameras_list:
            cameras[camera['camera_id']] = camera

        events = self.verkada_getEvents()

        if events == None:
            self.ds.log('INFO', "There are no event logs to send")
        else:
            self.ds.log('INFO', "Sending {0} event logs".format(len(events)))
            for log in events:
                log['name'] = cameras[log['camera_id']]['name']
                log['site'] = cameras[log['camera_id']]['site']
                log['message'] = log['name'] + " - " + log['notification_type']
                self.ds.writeJSONEvent(log, JSON_field_mappings = self.JSON_field_mappings, flatten = False)

        self.ds.set_state(self.state_dir, self.current_run)
        self.ds.log('INFO', "Done Sending Notifications")


    def run(self):
        try:
            pid_file = self.ds.config_get('verkada', 'pid_file')
            fp = open(pid_file, 'w')
            try:
                fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                self.ds.log('ERROR', "An instance of cb defense syslog connector is already running")
                # another instance is running
                sys.exit(0)
            self.verkada_main()
        except Exception as e:
            traceback.print_exc()
            self.ds.log('ERROR', "Exception {0}".format(str(e)))
            return
    
    def usage(self):
        print
        print(os.path.basename(__file__))
        print
        print('  No Options: Run a normal cycle')
        print
        print('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print('        in the current directory')
        print
        print('  -l    Log to stdout instead of syslog Local6')
        print
        print('  -g    Authenticate to Get Token then exit')
        print
    
        print
    
    def __init__(self, argv):

        self.testing = False
        self.send_syslog = True
        self.ds = None
        self.get_token = None
    
        try:
            opts, args = getopt.getopt(argv,"htlg")
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-l"):
                self.send_syslog = False
            elif opt in ("-g"):
                self.get_token = True
    
        try:
            self.ds = DefenseStorm('verkadaEventLogs', testing=self.testing, send_syslog = self.send_syslog)
        except Exception as e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
