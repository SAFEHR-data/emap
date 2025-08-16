# Emap Core Processor (`core`)

This service takes messages from a queue and compares this data to the current data in the EMAP database.
Generally, if a message has newer information that is different, then the message will update the database data,
otherwise the message will have no effect. This is important because the HL7 messages can be received out of order.

## IntelliJ setup

See [here for IntelliJ setup](intellij.md)

## Deploying a live version

How to deploy an instance of Emap on the UCLH GAE, to be run on real patient data. [emap-setup](https://github.com/inform-health-informatics/emap-setup)
manages the multiple repositories and configuration files.


1. <details>
    <summary>Create a directory with the correct permissions</summary>

    > **Note**
    > These folders probably already exist in `/gae`. Create a new one only if a new schema is availible


    Find a place to put the source code. If this instance is not attached to a person, a directory in `/gae` is a good place. For example, `/gae/emap-live/`, and this will be the example used in these instructions.
    e.g.
    
    ```bash
    mkdir /gae/emap-live
    chgrp -R docker /gae/emap-live
    chmod -R g+rws /gae/emap-live  # ensures that the group will be inherited for any new directories or files
    setfacl -R -m d:g::rwX /gae/emap-live
    ```
    <!-- Changed back from chmod -R g+rwx as permissions weren't transferred as in the readme. If this is a problem again then we should think about it
    <img width="590" alt="image" src="https://user-images.githubusercontent.com/8124189/210367021-32ac429f-950e-4acb-a1f8-b095eb4616cd.png">
    -->
    
    to create, modify the group, change ownership and inherit permissions.
    
    When you then create directories and files in this directory they should look like this:
    
    ```bash
    $ ls -la /gae/emap-live
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
   
</details>

2. <details>
    <summary>Set the git configuration</summary>

    Create a [personal access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) 
    for the next step and allow your username and access token to be saved with

    ```shell
    git config --global credential.helper store
    ```

    **Note**: this will allow storage of the connection information in plain text in your home directory. We use https 
    as a default but SSH is also possible.
</details>

3. <details>
    <summary>Install <b>emap-setup</b></summary>
   
    See the emap-setup [README](https://github.com/inform-health-informatics/emap-setup/blob/main/README.md) for details

</details>


4. <details>
    <summary>Modify configuration</summary>
   
    Modify `global-configuration.yaml` with any passwords, usernames and URLs that need to be changed for a live version.
    these will propagate into the individual `xxx-config-envs` configuration files, which in turn are used 
    by the`application.properties`.
    
    - For example, make sure `UDS_SCHEMA` is set to what it needs to be, in this example `live` is used. If you're writing to the UDS, use the `emap_core` user (password in lastpass).
    - If you're running locally, you can set `EMAP_PROJECT_NAME` to whatever you like. If running on the GAE it should be the same as the current directory (i.e. `emap-test` if in `/gae/emap-test`)
    - All passwords should be strong to help prevent a user/malware outside the GAE from accessing the queue.
    
</details>

5. <details>
    <summary>Clone the repositories</summary>

    Repositories must be checked out to the correct branches. "Correct" will depend on what you're trying to do.
    Conventionally a live instance would all be deployed from main/master, but during the development phase `develop`
    or a feature branch is more likely to be the correct. Clone all the master branches with:

    ```bash
    emap setup --init --branch master
    ```

    This will result in the following directory structure

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

6. <details>
    <summary>Creating an instance</summary>
   
    ```bash
    emap docker up -d
    ```

    Check the status with 
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
   
</details>


## Miscellaneous

Ports which are allocated per project are listed on the [GAE port log](https://liveuclac.sharepoint.com/sites/RITS-EMAP/_layouts/OneNote.aspx?id=%2Fsites%2FRITS-EMAP%2FSiteAssets%2FInform%20-%20Emap%20Notebook&wd=target%28_Collaboration%20Space%2FOrganisation%20Notes.one%7C3BDBA82E-CB01-45FF-B073-479542EA6D7E%2FGAE%20Port%20Log%7C1C87DFDC-7FCF-4B63-BC51-2BA497BA8DBF%2F%29)

Reserve an Emap DB schema on the GAE using the [load times spreadsheet](https://liveuclac.sharepoint.com/:x:/r/sites/RITS-EMAP-EmapDevChatter/Shared%20Documents/Emap%20Dev%20Chatter/load_times.xlsx?d=w20bdbe908b0f4e309caeb62590e890a0&csf=1&web=1&e=ZiUVZB):
