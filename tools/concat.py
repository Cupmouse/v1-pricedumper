import gzip
import logging
import os
import sys

if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print('Please specify directory to process and output directory.')
        exit(1)

    logger = logging.getLogger('Main')

    files = os.listdir(sys.argv[1])
    files.sort()
    for file in files:
        path = os.path.join(sys.argv[1], file)
        fo = gzip.open(path, 'rt')
        try:
            for line in fo:
                print(line)
        except EOFError as e:
            logger.exception('Unexpected EOF\n %s' % e)
