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
                    'git add .',
                    'git status',
                    "git commit -m 'updating app data'",
                    'git push -u origin master'
                    ]
    for com in shell_commands:
        subprocess.run(com.split())

    logger.error('Git Pushed')
    
