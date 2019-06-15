from __future__ import absolute_import

import logging

from meetuplytics import meetuplytics

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    meetuplytics.run()
