# emap-setup

Code to initialise, update and run EMAP. Includes ability to:
1. Clone repos if they don't exist
2. Set up configuration
3. Run pipeline (live and validation runs)
4. Update code (`git pull`)


## Usage: GAE

There should be one installation of `emap-setup` for each deployment of emap, each installed inside a venv named
`venv` at the top level of the source directory for the EMAP deployment.

Initial creation of the venv:
```shell
cd /gae/emap-deployment-name  # eg /gae/emap-dev
python -m venv venv --prompt "emap-deployment-name"  # prompt so we can tell which venv we're in

# install setup script in editable mode
cd /gae/emap-deployment-name/emap/emap-setup
pip install -e . -r requirements.txt
```

For an existing deployment it should already exist, so just activate it:
```shell
cd /gae/emap-deployment-name  # eg /gae/emap-dev
source venv/bin/activate
```

The required `global-configuration.yaml` file exists in the top level directory:
e.g. `/gae/emap-<schema name>`.

## Usage: Local

1. Create your top-level working directory 
1. Clone emap into that directory
1. Install the emap setup package to a virtual environment
1. Copy global-configuration-EXAMPLE.yaml as global-configuration.yaml to the top top-level working directory  
   and adjust for your own requirements

For example, create and activate a virtual environment first with either:

<details><summary>Conda</summary>

```bash
conda create python=3.9 -n emap --yes &&\
conda activate emap
```

</details>
<details><summary>venv</summary>

```bash
mkdir -p ~/.local/venvs/emap &&\
python -m venv ~/.local/venvs/emap &&\
source ~/.local/venvs/emap/bin/activate
```

</details>

then clone and install 
```bash
git clone https://github.com/UCLH-DHCT/emap
cd emap-setup
pip install -e . -r requirements.txt
cp global-configuration-EXAMPLE.yaml ../../global-configuration.yaml
```

***
## Command line options

To see the top level options:
```bash
emap --help
```

and for the setup subcommands e.g.:
```bash
emap setup --help
```

### Examples

Update all the repositories for the specified branches:
```bash
emap setup -u --branch test_branch
```

> **Note**
> If a branch has not been specified the runner defaults to the _develop_ branch.

Run a docker `ps` command:
```bash
emap docker ps
```

Run a validation
```bash
emap validation
```
