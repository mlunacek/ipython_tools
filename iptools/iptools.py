#!/usr/bin/env python
'''
An abstraction of the IPython Parallel task interface.

Given a PBS_NODEFILE, this class launches the controller and engines via ssh
using a temporary profile.

Author: Monte Lunacek, monte.lunacek@colorado.edu

'''
import os
import subprocess
import time
import socket
import signal
import shutil
import sys
import argparse
import logging
import uuid
import jinja2 as jin
import datetime
import json

from IPython import parallel

# Template constants
ipcontroller = jin.Template('''
c = get_config()
c.HubFactory.ip = '*'

''')

ipengine = jin.Template('''
c = get_config()
c.EngineFactory.timeout = 300
c.IPEngineApp.log_to_file = True
c.IPEngineApp.log_level = 30
c.EngineFactory.ip = '*'

''')

class ClusterFormatter(logging.Formatter):
    def format(self, record):
        a = "{0}: {1}".format(datetime.date.today(), str(record.lineno).rjust(4))
        return "{0} {1}".format(a, record.msg)

def get_logger(debug):
    logger = logging.getLogger('ipcluster')
    logger.setLevel(logging.CRITICAL)
    if debug == True:
        logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    formatter = ClusterFormatter()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

class Cluster:

    def __init__(self, **kwargs):
        """Creates a profile, logger, starts engines and controllers"""
        self.args = {}
        self.args['ppn'] = kwargs.get('ppn', 12)
        self.args['debug'] = kwargs.get('debug', False)
        self.args['terminate'] = kwargs.get('terminate', True)
        self.args['profile'] = kwargs.get('profile', None)
        self.cwd = os.getcwd()

        self.directory = os.getcwd()
        self.set_ppn(self.args['ppn'])
        self.node_list = self.pbs_nodes()
        self.logger = get_logger(self.args['debug'])

        # Create the profile
        self.profile = 'temp_' + str(uuid.uuid1())
        if self.args['profile'] is not None:
            self.profile = self.args['profile']
        self.logger.debug(self.profile)
        self.logger.debug(self.args['profile'])

        self.ipengine_path()
        self.create_profile()
        self.start_controller()
        self.start_engines()
        self.save()
        self.logger.debug('Engines have started')

    def set_ppn(self,ppn):
        """Environment variable override"""
        try:
            ppn = os.environ['PPN']
        except KeyError, e:
            pass

        self.ppn = int(ppn)

    def ipengine_path(self):
        """Find the full path for ipengine"""
        p = subprocess.Popen(['which','ipengine'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        res = p.stdout.readlines()
        if len(res) == 0:
            exit(1)

        self.ipengine = res[0].strip('\n')

    def pbs_nodes(self):
        """Returns an array of nodes from the PBS_NODEFILE"""
        nodes = []
        try:
            filename = os.environ['PBS_NODEFILE']
        except KeyError, e:
            exit(1)

        with open(filename,'r') as file:
            for line in file:
                node_name = line.split()[0]
                if node_name not in nodes:
                    nodes.append(node_name)

        #TODO add self.args['nodes'] as an option
        return nodes

    def create_profile(self):
        """Calls the ipython profile create command"""
        msg = 'creating profile {0}'.format(self.profile)
        self.logger.debug(msg)
        cmd = subprocess.Popen(['ipython','profile','create','--parallel','--profile='+self.profile],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=os.setsid)
        cmd.wait()

        # Append settings
        self.profile_directory = os.path.join(os.path.join(os.environ['HOME'],'.ipython'),'profile_'+ self.profile)

        tmp = ipcontroller.render({})
        with open(os.path.join(self.profile_directory,'ipcontroller_config.py'),'w') as f:
            f.write(tmp)

        tmp = ipengine.render({})
        with open(os.path.join(self.profile_directory,'ipengine_config.py'),'w') as f:
            f.write(tmp)

    def start_controller(self):
        """Starts the ipcontroller"""
        self.logger.debug('starting controller')
        cmd = ['ipcontroller']
        cmd.append('--profile='+self.profile)
        cmd.append('--log-to-file')
        cmd.append('--log-level=50')
        cmd.append("--ip='*'")
        self.controller = subprocess.Popen(cmd,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        preexec_fn=os.setsid)
        time.sleep(1)
        self.wait_for_controller()

    def wait_for_controller(self):
        """Loops until the controller is ready"""
        tic = time.time()
        while True:
            if  time.time() - tic > 30:
                break
            try:
                rc = parallel.Client(profile=self.profile)
                return True
            except ValueError, e:
                time.sleep(2)
            except IOError, e:
                time.sleep(2)
            except:
                time.sleep(2)

    def start_engines(self):
        msg = 'starting {0} engines'.format(len(self.node_list)*self.ppn)
        self.logger.debug(msg)
        """Starts and waits for the engines"""
        self.engines = []
        self.hostname = socket.gethostname()
        for node in self.node_list:
            for i in xrange(self.ppn):
                if self.hostname != node:
                    cmd = ['ssh']
                    cmd.append(node)
                    cmd.append(self.ipengine)
                else:
                    cmd = [self.ipengine]

                cmd.append('--profile='+self.profile)
                cmd.append('--log-to-file')
                cmd.append('--log-level=20')
                cmd.append('--work-dir={0}'.format(self.cwd))
                # print ' '.join(cmd)
                tmp = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=os.setsid)
                self.engines.append(tmp)
                time.sleep(0.1)

        self.wait_for_engines()

    def wait_for_engines(self):
        """Loops until engies have started"""
        tic = time.time()
        while True and time.time() - tic < 120:
            try:
                rc = parallel.Client(profile=self.profile)
                msg = 'Waiting for engines: {0} or {1}'
                msg = msg.format(len(rc), len(self.engines))
                self.logger.debug(msg)
                if len(rc.ids) == len(self.engines):
                    return True
                else:
                    time.sleep(2)
            except ValueError, e:
                time.sleep(2)
            except IOError, e:
                time.sleep(2)

    def remove_profile(self):
        """Removes the profile directory"""
        count = 0
        while True and count < 20:
            try:
                shutil.rmtree(self.profile_directory)
                count += 1
                return True
            except OSError, e:
                time.sleep(1)
        return False

    def client(self):
        return parallel.Client(profile=self.profile)

    @staticmethod
    def remove_profile(tmp):
        """Removes the profile directory"""
        count = 0
        while True and count < 20:
            try:
                shutil.rmtree(tmp['profile_directory'])
                count += 1
                return True
            except OSError:
                time.sleep(1)
        return False

    @staticmethod
    def terminate_cluster(tmp):

        try:
            for engine in tmp['engines']:
                os.killpg( engine, signal.SIGINT)
        except OSError:
            pass

        try:
            os.killpg( tmp['controller'], signal.SIGINT)
        except AttributeError, OSError:
            pass

    def save(self):

        tmp = {}
        tmp['profile'] = self.profile
        tmp['profile_directory'] = self.profile_directory
        tmp['engines'] = [ x.pid for x in self.engines]
        tmp['controller'] = self.controller.pid

        with open('profile.json','w') as outfile:
            outfile.write(json.dumps(tmp))

    def __del__(self):
        ''' Either delete the cluster or write the profile
            information to profile.json'''
        tmp = {}
        tmp['profile'] = self.profile
        tmp['profile_directory'] = self.profile_directory
        tmp['engines'] = [ x.pid for x in self.engines]
        tmp['controller'] = self.controller.pid

        if self.args['terminate'] == True:
            self.logger.debug('terminating cluster')
            self.terminate_cluster(tmp)
            self.remove_profile(tmp)

def read_profile():
    with open('profile.json','r') as infile:
        data = json.loads(infile.read())
    return data

def client():
    '''return the client using profile.json'''
    data = read_profile()
    return parallel.Client(profile=data['profile'])

def delete():
    '''Delete cluster using profile.json'''
    data = read_profile()
    Cluster.terminate_cluster(data)
    Cluster.remove_profile(data)

def get_args(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help=None, dest='command')
    start = subparsers.add_parser('start', help='start a cluster.\n')
    start.add_argument('--ppn', help='processors per node', dest='ppn')
    start.add_argument('--debug', help='print debug messages', dest='debug')
    start.add_argument('--profile', help='name of profile', dest='profile')
    start.set_defaults(ppn=12)
    start.set_defaults(debug=True)
    start.set_defaults(profile=None)
    stop = subparsers.add_parser('stop', help='stop the cluster.\n')

    return parser.parse_args(argv)

if __name__ == '__main__':
    args = get_args(sys.argv[1:])

    if args.command == 'start':
        opts = dict()
        opts['ppn'] = args.ppn
        opts['debug'] = args.debug
        opts['terminate'] = False
        opts['profile'] = args.profile
        c = Cluster(**opts)
    elif args.command == 'stop':
    	delete()
    else:
    	print 'Not a valid command', args.command
