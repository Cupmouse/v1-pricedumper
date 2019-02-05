import sys
import logging
import gzip
import re

import processor

if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print('Please specify file to process, and a file name of datadase to write the result')
        exit(1)

    # Set format for logger
    logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
    # Initialize logger
    logger = logging.getLogger('Main')

    # Open compressed file with gzip with read, text option, using handy with statement
    with gzip.open(sys.argv[1], 'rt') as file:
        # Check if opened file has content
        line = file.readline()

        if not line:
            logger.error('Blank file')
            exit(1)
        elif not line.startswith('head'):
            logger.error('Head line missing')
            exit(1)

        # Get appropriate line processor for a head
        try:
            line_processor = processor.get_appropriate_processor(line)
            line_processor.initialize_database()
            line_processor.process_head()
        except processor.ProcessingError as e:
            logger.exception('Head is invalid\n%s' % e)
            exit(1)

        logger.info('Head is valid')

        # Processing begin
        logger.info('Starting processing lines from file...')
        try:
            line = file.readline()

            while line:
                try:
                    line_processor.process_line(line)
                except processor.ProcessingError as e:
                    logger.exception('Error occurred during processing line\n%s' % e)
                    exit(1)

                line = file.readline()
        except EOFError as e:
            logger.exception('Reached EOF before explicit file terminal:\n%s' % e)
            logger.warn('Simulating EOF instead...')
            try:
                line_processor.simulate_eos()
            except processor.ProcessingError as e2:
                logger.exception('Simulating EOF failed\n%s' % e2)
            exit(1)