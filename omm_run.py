import logging
import os
import omm2caom2


def run_by_file():
    root_dir = '/usr/src/app'
    work_fqn = os.path.join(root_dir, 'todo.txt')
    netrc_fqn = os.path.join(root_dir, 'test_netrc')
    with open(work_fqn) as f:
        for line in f:
            obs_id = line.strip()
            logging.info('Process {}'.format(obs_id))
            meta = omm2caom2.Omm2Caom2Meta(obs_id, root_dir, 'OMM', netrc_fqn)
            meta.execute(context=None)
            data = Omm2Caom2Data(obs_id, root_dir, 'OMM', netrc_fqn)
            data.execute(context=None)


if __name__ == "__main__":
    run_by_file()
