#!/bin/bash
set -ex

# ensure that pip is available
function ensurePipExists()
{
    if ! [[ -x "$(command -v pip)" ]]; then
        curl -s https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py
        python get-pip.py --user
        rm get-pip.py
    fi
}

# check default python version
PYVERS=$(python -c 'import platform; major, minor, patch = platform.python_version_tuple(); print(major)')

# create venv with highest available python version
if [[ $PYVERS -eq 2 ]]; then
    # see if they have python 3 available
    if [[ -x "$(command -v python3)" ]]; then
        python3 -m venv .venv
    else
        # if they only have python 2, locally install virtualenv
        ensurePipExists
        export PATH="$PATH:/var/lib/jenkins/.local/bin"
        pip install virtualenv --user
        virtualenv .venv
    fi
elif [[ $PYVERS -eq 3 ]]; then
    python -m venv .venv
fi

# activate venv
source .venv/bin/activate

# install AWS CLI and OKTA Processor
ensurePipExists
pip install --upgrade pip
pip install awscli
pip install aws-okta-processor

# exit venv 
deactivate

# venv should be ready to use :)
echo VirtualEnv created in local directory, to activate run \"source .venv/bin/activate\".
