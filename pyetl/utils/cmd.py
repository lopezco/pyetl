import subprocess
import os
import logging

logger = logging.getLogger(__name__)


def subprocess_cmd(command):
    command = '{cmd} > tmp_cmd_out.log 2> tmp_cmd_err.log'.format(cmd=command)
    logger.debug('Executing: {}'.format(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    _ = process.communicate()  # proc_stdout

    with open("tmp_cmd_out.log") as f:
        output = f.read()

    with open("tmp_cmd_err.log") as f:
        errors = f.read()

    os.remove("tmp_cmd_out.log")
    os.remove("tmp_cmd_err.log")

    return {'output': output, 'errors': errors}


def handle_cmd_output(output):
    if output['errors']:
        raise Exception(output['errors'])
    else:
        print(output['output'])
