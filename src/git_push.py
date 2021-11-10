import subprocess
import os

def main(logger):
    ''' run the shell script that
    - adds all new files 
    - commits
    - pushes
    '''
    logger.error('Git Commit')

    # in shell, remove any existing dockers
    shell_commands = [
                    ['git', 'add', '.'],
                    ['git', 'commit', '-m', '"updating app data"'],
                    ['git', 'push', '-u', 'origin', 'master']
                    ]
    for com in shell_commands:
        subprocess.run(com)

    logger.error('Git Pushed')
    
