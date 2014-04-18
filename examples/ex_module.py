#!/usr/bin/env python

import iptools as ip

if __name__ == '__main__':

    opts = dict()
    opts['ppn'] = 4
    opts['debug'] = True
    opts['nodes'] = ['node1661', 'node1569']

    cluster = ip.Cluster(**opts)
    rc = cluster.client()
    print 'There are {0} engines'.format(len(rc))
    print rc.ids
