# Deploying an Emap instance

How to deploy an instance of Emap on
- your own machine; or
- the UCLH GAE, with access to real patient data.

To be read by Emap developers or deployers.

These instructions use `/gae/emap-instance-name` as an example of the Emap instance root.
If you're not on the GAE you can create your own empty directory.

The [emap script](../../emap-setup) from the emap-setup module is used to manage the
multiple repositories and configuration files.

> [!TIP]
> You can deploy on docker containers on your dev machine
> straight from your dev source tree(s).
> See [here for IntelliJ setup](intellij.md)

## Per-person one-off tasks

The `emap` repo is public and doesn't need a github login, but if you want to fetch from hoover or other repos you
need to [create a personal access token (PAT)](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) .

Give it the most limited scope possible: read-only and hoover (and/or other repos) access only, because
for your convenience we are going to allow your username and access token to be saved in plain text in your home directory:

```shell
git config --global credential.https://github.com.helper store
```

> [!CAUTION]
> This config option will store any git credentials you type in plaintext in your home directory.
> Never type your github username and password into the GAE (I think github no longer supports that anyway).
> Use PATs as above.
> 
> Check out your platform-specific options (eg. Keychain) here: https://git-scm.com/doc/credential-helpers
> 
> If you are particularly concerned about storing the PAT on disk you can instead use:
> 
> `git config --global credential.https://github.com.helper "cache --timeout=3600"`
> 
> To store the PAT temporarily in memory. Or do none of the above and paste it in every time.


You can also set your github username with:
`git config --global credential.https://github.com.username YOUR_GITHUB_USERNAME`

## Per-machine tasks

For the GAE, see [GAE shared env doc](https://uclh.slab.com/posts/shared-virtual-python-environments-with-uv-u7pa2fv4#hpkxd-per-gae-setup-tasks)
and follow instructions on setting up `uv`.

For any other machine, it's also recommended to use `uv`, but you could instead use conda or venv.

## Per-Emap instance setup tasks
> [!NOTE]
> If the Emap instance already exists, see instead [how to switch between Emap instances](#switching-emap-instances)

### Create a directory with the correct permissions (GAE only)
(If not on GAE just create an empty dir in the normal way)

See [main instructions for creating a directory that will inherit permissions correctly](https://uclh.slab.com/posts/shared-virtual-python-environments-with-uv-u7pa2fv4#hizbb-per-project-setup-tasks)

You need to run the `create_shared_dir` function defined in that doc as shown below:

`create_shared_dir /gae/emap-instance-name`

### Clone this repo
Clone this repo
```bash
cd /gae/emap-instance-name
git clone https://github.com/SAFEHR-data/emap
```

> [!IMPORTANT]
> On the GAE you must avoid pushing to the remote, as an extra layer of defence against leaking secrets.
> ```
> cd /gae/emap-instance-name/emap
> git remote set-url --push origin no_push.example.com
> ```
> Verify that this has worked:
> ```
> $ git remote -vv
> origin  https://github.com/SAFEHR-data/emap (fetch)
> origin  no_push.example.com (push)
> ```

### Install <b>emap-setup</b>

On the GAE you must use `uv`:
```shell
cd /gae/emap-instance-name  # eg /gae/emap-dev
uv venv --python 3.11 .venv-emap-instance-name   # venv name will go in the prompt so good to name it clearly
source .venv-emap-instance-name/bin/activate

# install setup script in editable mode
cd emap/emap-setup
uv pip install -e . -r requirements.txt
```

On other computers you should probably also use `uv`, but see the [emap-setup README](../../emap-setup/README.md)
for instructions for other virtual env managers.

### Modify configuration
The first time you must copy the `global-configuration.yaml` file from the template file:

`cp emap/emap-setup/global-configuration-EXAMPLE.yaml global-configuration.yaml`

Now modify `global-configuration.yaml`, adding passwords, usernames and URLs for your setup.

Then run `emap setup -g` to propagate the config into the individual `config/xxx-config-envs` configuration files.

Config tips:
- On the GAE we use an external postgres server (the "UDS"). See `fake_uds` for enabling postgres in a docker container for non-GAE setups.
- Be sure to set `UDS_SCHEMA` to match the name/purpose of the instance that you are deploying. This schema must already exist.
- Use the database user that has the minimum permissions necessary for your task. Ie. only the live user can write to live, so important to reserve use of this user for this purpose only. (See Lastpass for details)
- If you're running on your own machine, you can set `EMAP_PROJECT_NAME` to whatever you like. If running on the GAE it should be the same as the current directory (i.e. `emap-test` if in `/gae/emap-test`)
- All passwords must be strong. Remember that Emap needs to expose certain ports outside the GAE to operate.
- All config must stay local and not be committed to git.

### Clone the other repositories

> [!NOTE]
> Since moving to the monorepo, the importance of the `emap` script in managing repos
> has decreased. Especially during development, you may just want to manually manipulate your git repos.

If you have access to hoover and want to use it in your deployment:
```bash
cd /gae/emap-instance-name
git clone https://github.com/SAFEHR-data/hoover
```

> [!IMPORTANT]
> See [earlier cloning instructions](#clone-this-repo) and do the same trick to avoid pushing to this repo.
> ( `git remote set-url --push origin no_push.example.com` )

A GAE instance would typically be deployed from main or another well-known branch.
Make sure the branches are set correctly in the global config file, and run:
`emap setup --update`
Git repos will be checked out accordingly, and the config files will be updated.

Or you can override the configured branches with `emap setup --update --branch my_feature_branch`

The `--init` option to the above command is not recommended as it can overwrite existing data.

<details>
    <summary> This should result in the following directory structure</summary>

```bash
$ ls -la /gae/emap-instance-name
total 20
drwxrws---+  8 tomyoung docker 4096 Jan 16 09:27 .
drwxrwx---. 11 root     docker  179 Jan 13 16:26 ..
drwxrws---+  2 tomyoung docker  173 Feb 10  2022 config
drwxrws---+  8 tomyoung docker 4096 Jan 13 11:15 emap
-rwxrwx---.  1 tomyoung docker 2638 Jan 13 11:05 global-configuration.yaml
drwxrws---+  8 tomyoung docker 4096 Jan 13 11:08 hoover
```

If files already exist in the top-level directory, you might want to 
remove the `S` from the group permissions of each file, e.g. `chmod g-s global.configuration.yaml`

```bash
$ tree -L 2
.
.
├── config
│     ├── ...
├── emap
│     ├── README.md
│     ├── core
│     ├── docs
│     ├── emap-checker.xml
│     ├── emap-interchange
│     ├── emap-setup
│     ├── emap-star
│     ├── global-config-envs.EXAMPLE
│     ├── glowroot-config-envs.EXAMPLE
│     └── hl7-reader
├── global-configuration.yaml
├── hoover
      ├── ...
```

</details>

### Actually bring it up!
```bash
cd /gae/emap-instance-name
emap docker build
emap docker up -d
```


## Day-to-day Emap instance tasks

### Switching emap instances
```bash
cd /gae/emap-instance-name
source .venv-emap-instance-name/bin/activate  # or the equivalent for your virtual environment manager
```

### Changing config
> [!IMPORTANT]
> Config options may be added or removed from the global configuration file as new versions of Emap are released.
> It's recommended to perform a diff against the template file periodically and especially after updating
> to a new version of Emap, to see if you need to add/remove any options from the actual config file.
> ```bash
> vimdiff global-configuration.yaml emap/emap-setup/global-configuration-EXAMPLE.yaml
> ```
> Take great care not to edit the EXAMPLE file by mistake, as the config file will contain secrets.

```bash
vim global-configuration.yaml 
# [..make edits...]

# updates the files in config/ from the global config file
emap setup -g
```

### Bringing up an instance
```bash
emap docker up -d
```

### Check the status of an instance
```bash
emap docker ps
```

For example, this may give
```
$ emap docker ps
Name                    Command                State                                               Ports                                           
---------------------------------------------------------------------------------------------------------------------------------------------------------
jes1_core_1         /usr/local/bin/mvn-entrypo ...   Up                                                                                                   
jes1_fakeuds_1      docker-entrypoint.sh postgres    Up         0.0.0.0:5433->5432/tcp                                                                    
jes1_hl7-reader_1   /usr/local/bin/mvn-entrypo ...   Up                                                                                                   
jes1_rabbitmq_1     docker-entrypoint.sh rabbi ...   Up         15671/tcp, 0.0.0.0:15972->15672/tcp, 25672/tcp, 4369/tcp, 5671/tcp, 0.0.0.0:5972->5672/tcp
```

# Miscellaneous

Ports which are allocated per project are listed on the [GAE port log](https://liveuclac.sharepoint.com/sites/RITS-EMAP/_layouts/OneNote.aspx?id=%2Fsites%2FRITS-EMAP%2FSiteAssets%2FInform%20-%20Emap%20Notebook&wd=target%28_Collaboration%20Space%2FOrganisation%20Notes.one%7C3BDBA82E-CB01-45FF-B073-479542EA6D7E%2FGAE%20Port%20Log%7C1C87DFDC-7FCF-4B63-BC51-2BA497BA8DBF%2F%29)

Reserve an Emap DB schema on the GAE using the [load times spreadsheet](https://liveuclac.sharepoint.com/:x:/r/sites/RITS-EMAP-EmapDevChatter/Shared%20Documents/Emap%20Dev%20Chatter/load_times.xlsx?d=w20bdbe908b0f4e309caeb62590e890a0&csf=1&web=1&e=ZiUVZB):
