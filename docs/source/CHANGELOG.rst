*********
Changelog
*********

0.8.0
=====
:Date: October 13, 2021


* `2 <https://github.com/insitro/redun/pull/2>`_ - Add buildspec
* `1 <https://github.com/insitro/redun/pull/1>`_ - Adding new examples and LICENSE
* Begining of public repo
* `312`_ - Update html links to markdown
* `28`_ - Example of making postgres optional
* `305`_ - Compute hash of ShardedS3Dataset after it's returned
* `304`_ - Remove unnecessary DO NOT USE warning that contained core references
* `300`_ - adding insitro redun.ini examples
* `303`_ - DE-3485 clean up design doc
* `301`_ - Fixes from library cleanup
* `298`_ - DE-3490 Clean examples
* `299`_ - Small lib cleanups
* `297`_ - Remove unused variables in AWS glue code
* `293`_ - add scripts for making a public version of the redun repo


0.7.4
=====
:Date: September 30, 2021

* `294`_ - More redun examples
* `292`_ - Add git commit and origin_url tags to execution
* `291`_ - Add more debugging logs for executors [WIP]


0.7.3
=====
:Date: September 21, 2021

* `286`_ - Revert "Update latest setuptools-conda with dependency version fix "
* `288`_ - Move AWS batch statuses to constants

0.7.2
=====
:Date: September 20, 2021

* `286`_ - Update latest setuptools-conda with dependency version fix
* `285`_ - DE-3421 Add executor-based job tags
* `250`_ - DE-3433 Add `--rerun` option to `redun run`

0.7.1
=====
:Date: September 16, 2021

* `282`_ - DE-3399 Add tag APIs for redun_server
* `272`_ - DE-2651 Add Spark executor
* `277`_ - DE-2483 value store
* `278`_ - Update postgres example config
* `283`_ - Ignore self and cls variables for docstring test

0.7.0
=====
:Date: September 09, 2021

* `209`_ - DE-1592 Tag system v2
* `253`_ - DE-2945 Use ProcessPoolExecutor through a separate executor
* `280`_ - DE-3342 refactor CallGraphQuery
* Merge branch 'master' of github.com:insitro/redun
* update journal
* `276`_ - small tutorial fixes
* `274`_ - Test for numpy docstring adherence
* `273`_ - Fix minor typos in docs
* `271`_ - Update redun server's ecs cluster & setup CD for redun-dev
* `260`_ - DE-3094 Implement a fast Files search page
* `270`_ - add sys.exit(1)
* Merge branch 'master' of github.com:insitro/redun
* update journal
* `268`_ - remove recursive query for job executions
* `267`_ - fix migration with a missing commit

0.6.1
=====
:Date: August 04, 2021

* `265`_ - avoid Session as context use
* `264`_ - Abandon release if failures are encountered during install or build

0.6.0
=====
:Date: August 02, 2021

* `261`_ - DE-3105 Add Job.execution_id migration
* `259`_ - DE-3091 Add indexes to commonly queries columns
* `258`_ - Small improvements to File such as file size and staging/copy defaults

0.5.1
=====
:Date: July 16, 2021

* `256`_ - Quote the DB password when creating DB URIs
* `255`_ - Add support for Python 3.9
* `245`_ - DE-2923 Extending the tutorial
* `252`_ - DE-2939 Improve sort of sections in dataflow
* `244`_ - Allow for non-python function as Task.source in dataflow visualization
* `251`_ - DE-2922 Small fixes and improvements to batch executor and File
* `249`_ - Update redun server to handle new task serialization format
* `248`_ - DE-2900 Show db too new message

0.5.0
=====
:Date: June 28, 2021

* `246`_ - tee stdout and stderr
* `229`_ - Backfill lonely Tasks, and update Task/Value serialization
* `241`_ - DE-2001 Add File support for http, https, and ftp
* `240`_ - DE-2850 Guided tutorial through workflow examples
* `242`_ - add missing use_tempdir
* `238`_ - Remove rogue . in alembic version info
* `237`_ - use python3 in Makefile

0.4.15
======
:Date: June 15, 2021

* `235`_ - fix: bump boto3 floor version for required botocore functionality
* `232`_ - [DE-2761] Make tee tolerant of write errors for script batch jobs
* `233`_ - DE-2632 -- Handle case where non-redun jobs have matching prefix
* `234`_ - DE-2711 -- Fix optional cli args
* `228`_ - Every time a db.Task is recorded, also record it as a db.Value

0.4.14
======
:Date: June 07, 2021

* `224`_ - DE-2713 Add batch_tags option
* `230`_ - Fix job status
* `222`_ - upstream useful changes from tags-flag
* `219`_ - DE-2660 Use a default root task for non-TaskExpressions

0.4.13
======
:Date: May 18, 2021

* `220`_ - DE-2637 fix hashing of task_options_update
* `204`_ - DE-2619 Use O(1) queries to speedup record serialization
* `218`_ - DE-2635 Show unknown CallNodes for unfinished jobs
* `217`_ - show keyword arguments
* `216`_ - Fix isort line length
* `215`_ - DE-2623 Dont use recursive for getting execution jobs
* `213`_ - fix path term parsing
* `212`_ - fix: redun server ECS service name in merge spec
* `208`_ - Scope redun_server DB sessions at the request level
* `210`_ - Cleanup logging of migrations
* `211`_ - DE-2599 Use wait_until in aws batch tests to fix flaky tests

0.4.12
======
:Date: May 07, 2021

* `206`_ - Add method to clone RedunBackendDB with connection pool sharing
* `196`_ - DE-2325 Add database versioning commands
* `201`_ - Add quick script to generate release notes

0.4.11
======
:Date: April 22th, 2021

* `198`_ - Add support for configuration only task args
* `197`_ - [DE-2428] Fix typed list check
* `192`_ - DE-2434 Add more common tasks to functools
* `194`_ - decouple scheduler from oneshot
* `186`_ - Dockerize redun server, update directory layout and utils, add specs for prod deployment
* `190`_ - DE-2464 Add postmortem debugging

0.4.10
======
:Date: April 12th, 2021

* `188`_ - Don't let docker change terminal to raw mode
* `187`_ - Tasks should allow novel kwargs
* `180`_ - Use amazonlinux default pythons
* `185`_ - Support job timeouts on batch
* `182`_ - Lazy operators for redun Expressions

0.4.9
=====
:Date: March 23rd, 2021

* `183`_ - add py.typed
* `177`_ - Support list args from cli
* `178`_ - Fix settrace monkeypatch to restore debugging ability
* `179`_ - DE-2370 Give array jobs a unique uuid
* `181`_ - sqlalchemy 1.4.0 no longer allows postgres:// gotta be postgresql://
* `176`_ - Improve pickle preview for constructor and __new__
* `173`_ - Allow pycharm's debugger to work with redun
* `175`_ - Set choices on parser for enum args
* `174`_ - Allow use of id prefixes with push/pull commands
* `171`_ - Make S3 repositories work
* `172`_ - Match python 3.7 and 3.8 micro versions to match codebuild image


0.4.8
=====
:Date: March 10th, 2021

* `111`_ - Add concept of remote repos
* `169`_ - Remove invalid positional arg in get_or_create_job_definition call
* `147`_ - Dir should have File as subvalues for better dataflow recording
* `165`_ - Fix lack of caching for catch expressions
* `164`_ - Fix PartialTask's options() and partial() calls so that they interact correctly
* `163`_ - Imports executors in the __init__
* `155`_ - Use config_dir with redun_server

0.4.7
=====
:Date: February 24th, 2021

**WARNING:** This version contains a bug in the `get_or_create_job_defintion` call in `batch_submit`. Do not use this version.

* `156`_ - Automatic publishing of packages and docs
* `153`_ - Use existing job def
* `116`_ - Display dataflow
* `154`_ - Fix data provenance recording for seq scheduler task
* `152`_ - Fix pickling expression upstreams
* `136`_ - Add redux to redun_server
* `151`_ - Record stderr from scripts on batch
* `149`_ - Add support for generating DB URI from AWS secret
* `150`_ - Document max value size
* `146`_ - Cryptic error for large falues
* `148`_ - Simplify Scheduler.run() to take expressions
* `145`_ - Add nout task option for tuples
* `144`_ - Increase sqlalchemy requirement to 1.3.17
* `143`_ - Package on submit not start

0.4.6
=====
:Date: February 3rd, 2021

* `141`_ - Only gather inflight jobs on batch on first submission

0.4.5
=====
:Date: January 28th, 2021

* `139`_ - Propagate batch script errors
* `137`_ - Override CannotInspectContainerError batch errors
* `138`_ - Fix pickle preview for classes where the module can't be found
* `133`_ - Small fixes from demo talk
* `132`_ - Small improvements to File.copy_to and self-stagin

0.4.4
=====
:Date: January 15th, 2021

* `131`_ - Fix catch dataflow
* `134`_ - Add notebook example of redun scheduler evaluation
* `128`_ - Make redun compatible with sqlalchemy-1.4.0b1
* `129`_ - Add pickle_preview for unknown classes
* `130`_ - Fix catch dataflow
* `127`_ - Add FAQ page to docs
* `126`_ - Require sorted imports

0.4.3
======
:Date: January 5th, 2021

* `122`_ - Stronger type checking for task calls
* `101`_ - Record CallNodes when an exception is raised
* `86`_ - Scheduler tasks

0.4.2
======
:Date: January 4th, 2021

* `121`_ - Array job reuniting fix

0.4.1
======
:Date: December 23rd, 2020

* `119`_ - Bugfix to correctly restart job array monitor thread

0.4.0
======
:Date: December 15th, 2020

* `83`_ - Detect and submit job arrays to AWS batch
* `114`_ - Adds job definition option to run container in privileged mode

0.3.12
======
:Date: December 10th, 2020

* `76`_ - Improve querying of logs

0.3.11
======
:Date: December 8th, 2020

* `109`_ - Permalink update in README
* `108`_ - Automated release

0.3.10
======
:Date: December 3rd, 2020

* `104`_ - use ECR for postgres image
* `95`_ - Hard fail on script errors
* `100`_ - Show more information in logs and traceback
* `102`_ - Fix check-valid=shallow to use the original call node
* `98`_ - Skip license check when building conda packages
* `105`_ - Typecheck map_nested_value
* `103`_ - Fix script reactivity to inputs and outputs
* `106`_ - Small clean up of batch logs

0.3.9
=====
:Date: November 25th, 2020

* `96`_ - Default to interactive debugging
* `81`_ - Allow REDUN_CONFIG environment variable to specify config directory
* `92`_ - DE-1922 tolerate missing logs for failed jobs

0.3.8
=====
:Date: November 18th, 2020

* `89`_ - Respect no-cache for job reuniting.
* `88`_ - Assume batch output after completion is valid.
* `87`_ - Fix filesystem caching and Dir hashing caching.
* `85`_ - Add step to publish pypi package in publish script.
* `84`_ - Fix package name in dependencies notes in README.

0.3.7
=====
:Date: November 12th, 2020

* `80`_ - redun import paths should take precedence over system imports.
* `79`_ - fix default arg parsing and prefix args.

0.3.6
=====
:Date: November 10th, 2020

* `73`_ - Allow users to customize `setup_scheduler()`.

0.3.5
=====
:Date: November 10, 2020

* `77`_ - Check version of redun cli in docker container.

0.3.4
=====
:Date: October 29th, 2020

* `72`_ - Use current working directory when importing a module.
* `64`_ - Some optimizations for AWS Batch large fanout.

0.3.3
=====
:Date: October 28th, 2020

* `#71`_ - Don't fetch batch logs when debug=True

0.3.2
=====
:Date: October 27th, 2020

* `#66`_ - Fix import_script to properly support module-style

0.3.1
=====

* Fix bug with using s3fs >= 0.5

0.3
=====
:Date: October 20th, 2020

* Improve display of errors and logs for AWS Batch jobs.

0.2.5
=====
:Date: October 14th, 2020

* `#57`_ - Improve redun traceback for failed jobs.
* `#56`_ - Fix local shell error propogation.
* `#54`_ - Add documentation on required dependencies.

0.2.4
=====
:Date: October 6, 2020

* Encourage defining task namespaces by raising a warning. The warning can be ignored using a [configuration option](config.html#ignore-warnings).


0.2.3
=====
:Date: September 25, 2020

* Fixes FileNotFoundError occuring when using AWS Batch tasks, by avoiding the s3fs cache.


0.2.2
=====
:Date: August 27, 2020

* Require database credentials to be specified by environment variables


0.2.1
=====

:Date: August 9, 2020

 * Fix duplicate upstream bug.


0.2.0
=====

:Date: August 7, 2020

 * Add support for Python 3.8


0.1.1
=====

:Date: July 29, 2020

 * Drop dependency on bcode as it has no conda package and the repo appears abandoned.


0.1
===

 * Initial release.
