#!/usr/bin/env python

from IPython.parallel import Client

rc = Client(profile='iptools')
print len(rc.ids)
print rc.ids
