## Why a monorepo for Emap?

Allows atomic changes across the whole project. No more awkward branch naming conventions for what code works with what other version of another repo's code.

Reduce duplication of config (eg. CI scripts, docker-compose files).

Easier local environment setup (eg. less IDE config)

## Repo scope

Repos to include:
- git@github.com:inform-health-informatics/Inform-DB.git
- git@github.com:inform-health-informatics/Emap-Interchange.git
- git@github.com:inform-health-informatics/Emap-Core.git
- git@github.com:inform-health-informatics/emap-hl7-processor.git
- git@github.com:inform-health-informatics/emap-setup.git
- git@github.com:inform-health-informatics/emap_documentation.git (public docs)

Exclude:
- git@github.com:inform-health-informatics/hoover.git (temporarily exclude)
- git@github.com:inform-health-informatics/internal_emap_docs.git (is internal)

Any others I've forgotten?

### Future code and migrations

After the migration, further code that is "core" to Emap should go straight into the monorepo, even if it's incomplete or experimental (that's what branches are for).

Non-core projects should go in their own separate repos - they can always be merged in if we decide they have become core.

But what is core Emap code? More or less it's code that is essential in order to have a meaningful instance of Emap.

So that includes anything (plus dependencies) that writes directly to Emap-Star tables. And Interchange, since you can't send data to Emap without knowing what format it should be in.

Excluded would be anything that only reads from Emap, even if it creates its own mini-db for some project-specific purpose, eg HOCI or Inform-us.

What about datasources that feed data into Emap? hoover does this and we'd consider it core (this is unrelated to the reason we're not merging it in right now). But some of its (meta)data is quite fundamental to an Emap instance - eg. locations are fundamental to ADT being useful.

But what about a datasource reader for a very old ADT database that only one user cares about? Probably not core.

## Tasks/Requirements

### Phase 1a (essential tasks)

(Eg. code will fail to compile/run/deploy if we don't do them, or they're so integral to the merge it would almost be harder *not* to do them in the first phase)

- &check; Enable branch protection
- Check repo permissions
- Keep PRs small and/or manageable. The enormous PR(s) that bringing in each repo will generate should contain no other code changes.
- &check; Merge in `Inform-DB`
- &check; Merge in `Emap-Interchange`
- &check; Merge in `Emap-Core`
- &check; Merge in `emap-hl7-processor`
- &check;? Preserve history - ensure git log and git blame give expected results
- Make names match (and follow the same capitalisation+punctuation) between:
    - &check; docker service names
    - &check; module directories
    - &check; maven modules
    - &check; output jar
- Merge Emap config files
- Merge/simplify docker-compose files
- &check; Fix/simplify GHA scripts
- Hoover: changes to adapt to changes in this repo
- Hoover: convert Circle to GHA
- Fix/simplify glowroot config
- &check; Merge `inform-checker.xml` files (`hl7-reader` may need its own)
- &check; Bring in any changes to old repos that have happened since this migration started, and update instructions for how to do it

### Phase 1b (delayable tasks, but will attempt as phase 1 stretch goals)

- &check; Bring in pre-existing tags, prefix them to avoid clashes and signify oldness
- Rename package names `rits.inform` -> `arc.emap`
- Merge in `emap_documentation` to docs subfolder
- Ensure PR templates are read from docs repo

### Phase 2 (delayed tasks)

(Within scope of the project but things will work well enough without them)

- Make sure auto doc generation for former `emap_documentation` repo is working
- Merge in `emap-setup`

## Limitations

Old PRs/issues won't get migrated

## Naming

New, merged repo will be called `emap`  and live under the new organisation

The new name column will be used for ALL purposes; that is, the docker service name, the subdirectory, the maven project name, and the output jar name.

| current repo name | NEW name | current docker service name  | current maven project name | current jar name  |
| --- | --- | --- | --- | --- |
| Inform-DB        | emap-star (maven project = emap-star-parent) | n/a | emap-star | n/a |
| Inform-DB        | emap-star/emap-star (maven project = emap-star) | n/a | inform-db | `inform-db*.jar` |
| Inform-DB     |   emap-star/emap-star-annotations (maven project = emap-star-annotations) | n/a  |  inform-annotations | `inform-annotations*.jar`  |
| Emap-Interchange | emap-interchange | n/a | emap-interchange | `emap-interchange*.jar` |
| Emap-Core       | core | emapstar  | EmapCore | `Emap-Core*.jar`  |
| emap-hl7-processor | hl7-reader | hl7source | HL7Processor | `HL7Processor*.jar` |
| hoover §     | hoover   | hoover  | hoover | `Hoover*.jar` |
| emap-setup     | emap-setup   | n/a  | n/a | n/a |


§  not merging right now but can still rename things 

## Using `git filter-repo` to prepare repos for merging into this monorepo

Since `git filter-repo` works on an entire repo, it's necessary to do the main rewriting work on a clone of each incoming repository.

Create like so:
```
# clone the new repo (this repo) if not already done
cd ~/YOUR_PROJECTS_DIR/emap
git clone git@github.com:UCLH-DHCT/emap.git

# Recommended to make a dated directory to put all these repos in,
# in case you need to repeat the process to bring in subsequent changes to these repos
mkdir ~/emap.filter.repo.2023TODAYSDATE
cd ~/emap.filter.repo.2023TODAYSDATE

# clone the old repos using their new names
git clone git@github.com:inform-health-informatics/Inform-DB.git emap-star
git clone git@github.com:inform-health-informatics/Emap-Interchange.git emap-interchange
git clone git@github.com:inform-health-informatics/Emap-Core.git core
git clone git@github.com:inform-health-informatics/emap-hl7-processor.git hl7-reader
git clone git@github.com:inform-health-informatics/emap-setup.git emap-setup
```

Run the actual filter-repo command. Moves everything to a subdir and gives all tags a prefix to avoid clashes.
```
for repo in emap-star emap-interchange core hl7-reader emap-setup; do
    ( cd $repo &&
      git filter-repo --path-rename :${repo}/ --tag-rename :${repo}- )
done
```
This runs in only a few seconds! (Compare to `git filter-branch` which takes forever).

We don't push the modified versions back up to the original repos. In fact, `filter-repo` discourages you from doing so by deleting the `origin` remote.

Therefore, in the new repo, instead of creating remotes that point to the incoming repos on github, we will point to the modified repos on the local disk. Then we can merge them in.

Now switch back to the new repo and create remotes, fetch and merge:
```
# (you should manually delete any pre-existing remotes from previous attempts, likely everything except origin)
incoming_dir=~/emap.filter.repo.2023TODAYSDATE
cd ~/YOUR_PROJECTS_DIR/emap/emap
for repo in emap-star emap-interchange core hl7-reader; do
    # emap-setup doesn't have a develop branch
    git remote add -t develop -m develop --no-tags "$repo" $incoming_dir/"$repo"
    git fetch --prune "$repo"
    # --allow-unrelated-histories only needed the first time you're bringing this repo in
    git merge --allow-unrelated-histories "$repo"/develop
done
```
Merge conflicts are possible here, not due to the mass renaming that filter-repo performs, but
due to the changes that were done as part of the migration (consolidating CI, for example).
Just fix them in the normal way.

Check that your merge commit looks like you expected it to look! If this is a "top-up" merge,
it should include just the changes done since last time. Protect yourself from errors by not
using the `--allow-unrelated-histories` option beyond the first merge.

Bring in our renamed tags from earlier, then push them to the monorepo:
```
git fetch --all --tags
git push --tags origin
```

### What if we change the old repo after we've done the monorepo merge?

This migration won't be instantaneous; a few changes will be going in to the old repos while it's still underway.

I'll put detailed instructions here when I've actually done it, but for now
we can note that you get the same results every time you repeat the `git filter-repo` process, as long as you're
using the same parameters. That is, you get a new commit history with the same (new) hashes as before.
This may not have been the case if eg. `filter-repo` was updating the CommitDate with the current time.

Given this, it should be easy to repeat the same merging process, the only difference being that the `git merge` step won't be merging in as many commits, and you
won't need the `--allow-unrelated-histories` option.

### Abandoned `git mv` + `git merge` method

- Create new, empty, monorepo (this repo)
- For each repo you want to bring in:
    - Define a git remote for that repo and fetch it
    - Add a commit to that tree that just does a `git mv` into a subdirectory
    - From main, `git merge` the new branch

This worked ok, but causes problems with `git log`. The `--follow` option is necessary for traversing the commit history past a move, *however* it can only be used on individual files, not directories.

### Why not do a `git filter-branch` instead of `git mv`?

You could do, but it's slow and strongly discouraged in favour of git filter-repo, which is a better way to rewrite history, which it turns out is necessary to make `git log` work properly.

### Why aren't we using `git subtree`

It breaks the output of `git blame` and `git log` (even with `--follow` option).

It would look like whoever did the merge had written all the code on the date the merge happened.

### What about `git submodule`?

This relies on the referenced repos continuing to exist. However, we want to create a monorepo and eventually delete the original repos.
