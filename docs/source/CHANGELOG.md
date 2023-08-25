# Changelog

## 0.16.2
August 25, 2023

* `#319` - avoid Screen.query method
* `#317` - Add DockerExecutor volumes config
* `#315` - Efficiently compute Exec and Job status from result type
* `#314` - add missing repl hotkey for search screen
* `#313` - Remove duplicate federated tasks

## 0.16.1
July 27, 2023

* `#311` - update MANIFEST.in for console/style.css
* `#310` - skip test since shutdown doesnt actually run in 3.8
* `#309` - Test task _and_ scheduler level catch cache busting
* `#307` - Add Console docs

## 0.16.0
June 21, 2023

* `#303` - Fix boto thread safe
* `#300` - Console extras
* `#301` - Fix scrapping typo
* `#299` - Add batch job tags at subission time
* `#297` - broaden the scope of console executions --find to include args
* `#294` - Add TUI Console to redun cli
* `#295` - Update stale example output
* `#293` - Example of running conda in a docker container
* `#292` - Allow setting no role for aws batch jobs
* `#291` - Removed pinned boto version for doc requirements
* `#73` - split out version into it's own module
* `#68` - GCP batch support v2
* `#72` - Fix link on the "Scheduling redun workflows" docs page
* use legacy solver for docs publishing
* fix sphinx docs building
* `#71` - Update design.md

## 0.15.0
May 01, 2023

* `#286` - Uncap botocore to allow better dependency solving
* `#285` - DE-8667 ensure to call record_job_start even when executor is not defiend
* `#275` - Drop python 3.6
* `#284` - DE-8613 make Handle.is_valid() more graceful to missing recordings
* `#283` - Add lambda_federated_task scheduler task with tests
* `#282` - small fix for graph reduction docs
* `#281` - Update TaskExpression names to match code snippet
* `#280` - [DE-8465] Fair Share Scheduling support.
* `#278` - Release 0.14.0
* `#261` - Provide integration code for redun and a proxy
* `#277` - Add task_def option to federated_task config
* `#247` - Publish federated tasks
* `#270` - DE-8068 Pass through missing aws batch options

## 0.14.0
March 13, 2023

This release primarily introduces "federated tasks", a mechanism for working with tasks 
where you do not have to import the implementation code.

* `#261` - Provide integration code for redun and a proxy
* `#277` - Add task_def option to federated_task config
* `#247` - Publish federated tasks
* `#270` - DE-8068 Pass through missing aws batch options

## 0.13.0
February 27, 2023

* `#272` - DE-7556: Update job definition sanitizing to be recursive 
* `#182` - Check CSE even for completed jobs 
* `#269` - Bump SQLAlchemy to 2.0, use future=True flag 
* `#268` - Follow-up on SQLAlchemy 2.0 compatibility 
* `#266` - DE-7776 Upgrade sqlalchemy to 1.4 and fix deprecation warnings 
* `#22` - Add k8s executor 
* `#253` - Try to clarify and document Value and Handle 
* `#63` - remove develop mode for installing redun during docs build 
* `#254` - DE-7282 Simplify job arguments and JobArrayer 
* `#255` - Clarify role arn and fix for sphinx 6.0 
* `#61` - Pass AWS_DEFAULT_REGION env variable to container
* `#244` - DE-6763 Add IFile and ContentFile
* `#60` - Fix import statements with lowercase config 

## 0.12.0
November 29, 2022

* `#241` - Remove length restriction on redun type names
* `#242` - Use previews for value with no loaded class
* `#243` - DE-6625 Catch and update eval_hash on race condition
* `#240` - Specify user and password in example readme
* `#232` - DE-6585 Raise a more informative error message for File errors

## 0.11.1
November 01, 2022

* `#234` - Temporarily cap botocore version due to upstream bug

## 0.11.0
November 01, 2022

* `#227` - Add --wait option to redun launch
* `#221` - Add launch to run to allow remote runs
* `#207` - DE-6252 Upgrade mypy and remove python pin
* `#217` - DE-6397 Add pre commit for all linting/formatting

## 0.10.1
October 20, 2022

* `#219` - Fix nested value iteration for dataclasses instantiated from subscripted generics

## 0.10.0
October 17, 2022

* `#218` - Add alias executors
* `#216` - DE-6353 Add env var parsing to config files
* `#215` - Update package version to 0.9.1, skip 0.9.0 in changelog
* `#213` - Backport fixes

## 0.9.1
September 30, 2022

* `#206` - Add missing get_hash to ShardedS3Dataset
* `#203` - Update docs to match Expression __repr__ changes
* `#205` - Temporarily pin Python 3.10.6 to avoid mypy bug on 3.10.7
* `#195` - Smarter job def
* `#173` - Run tox in parallel(up to CPU count)
* `#202` - Remove PR buildspec, codebuild was made redundant by GH actions
* `#201` - DE-6221 - Make GH actions match codebuild
* `#200` - fix-handle-arg-serialization
* `#199` - implement an easier to read repr for Expressions
* `#198` - Fix handling of multi-node job statuses and logs
* `#197` - fix bug with mark_dup staging

## 0.8.16
September 08, 2022

* `#194` - Increase default `ulimits` for `nofile` (number of open file descriptors) for multi-node aws batch jobs
* `#193` - handle dataclass types correctly
* `#189` - Support nested iteration and mapping for dataclasses
* `#192` - A few small bug fixes
* `#191` - DE-6030 Fix expression cycle detection for cached expressions
* `#188` - DE-5961 Allow functions decoratored by @task and @scheduler_task to processed by autodoc

## 0.8.15
August 18, 2022

* `#179` - Add job stitching to subrun
* `#185` - include api reference in sphinx docs
* `#181` - DE-5801 Add interactive config for docker executor
* `#180` - [DE-5731] Make vizualization functionality optional when pygraphviz is unavailable
* `#178` - Update docs to describe db migrations
* `#175` - DE-5591 Fix Common Subexpression Elimination (CSE)
* `#47` - pass self.region to submit_command() from AWSBatchExecutor._submit_single_job()
* `#174` - Update test_pull_request.yml
* `#160` - Call graph visualizer
* `#126` - Improved display of job status
* `#125` - Better support for previewing large values
* `#170` - add support for parsing datetime
* `#38` - Bump pyspark from 3.1.1 to 3.1.3
* `#37` - working on fixing docs build

## 0.8.14
July 5, 2022
* `#165` - Switch conda package to noarch
* `#168` - Bump version of `black` in `redun_server`

## 0.8.13
June 16, 2022

* `#162` - DE-5345 Fx multi-node doing caching 
* `#155` - Update linting tools 
* `#161` - DE-5312 Expand the try-catch to include input parsing 
* `#157` - [DE-3475] Fix UDF type arg being ignored, add some Spark tests 
* `#156` - De4653 hash code alt 
* `#149` - Generalize file staging 
* `#159` - Fix aws batch shared memory 
* `#152` - Added docstrings to key Glue job functions 

## 0.8.12
June 01, 2022

* `#153` - DE-5198 Don't drop containerProperties when sanitizing job defs
* `#151` - remove unused and broken extract_tar import

## 0.8.11
May 29, 2022

* `#30` - Add missing data.tsv and fix cleanup default

## 0.8.10
May 26, 2022

* `#146` - Update moto to 3.1.10
* `#145` - DE-5109 Fix missing `--array-job` argument for `redun oneshot`
* `#144` - Add more docs to docker executor functions/classes
* `#137` - DE-4915 Breakout DockerExecutor and consolidate scratch and code packaging

## 0.8.9
May 17, 2022

* `#131` - [DE-4809] Use AWS job run insights to get better tracebacks for glue jobs
* Merge remote-tracking branch 'origin/main'
* `#138` - resolve conflicts with public and private main
* `#33` - Fixed typos and minor grammatical stuff like missing commas, etc. in the documentation and README.md
* `#32` - Update README.md
* `#136` - Ignore flake8 F401 errors in `__init__.py` files

## 0.8.8
April 20, 2022

* `#129` - AWS Batch multi-node executor
* `#130` - Update README.md because compositiblity is not a word.
* `#128` - DE-4761 fix script reuniting in aws batch executor
* `#127` - Keep 'redun log' stable tag sorting even for complex tag values
* `#124` - DE-4689 File encoding v2
* `#119` - DE-4636 Fix for S3FileSystem threading issues
* `#120` - Fix job clear bug
* `#80` - DE-4600 Working example of catch_all
* `#116` - Bump version to 0.8.7

## 0.8.7

March 11, 2022

* `#115` - DE-4596 Handle case where containerProperties is missing
* `#113` - simplify apply_tags task
* `#111` - Include restored traceback when restoring from pickled data
* `#112` - Add apply_job_tags and apply_execution_tags

## 0.8.6
February 16, 2022

* `#103` - `make setup` should be runnable more than once in Docker examples
* `#102` - Minor usability improvements to ShardedS3Dataset
* `#93` - Implement subrun()
* `#100` - Suggestion for shared database usage
* `#97` - Add lint job to github actions. Upgrade black. Ignore F811.
* `#92` - Streamline local executor exec methods
* `#95` - Implement scheduler.get_job_status_report()
* `#94` - Implement Config.get_config_dict()
* `#96` - Advertize `max_value_size` and `value_store_path` when values are too large for db
* `#90` - Better error mesg if task is missing in registry
* `#91` - Fix import order and line spacing
* Merge pull request #87 from insitro/DE-4210_db_aws_region_from_env
* `#87` - Use AWS_REGION env variable for DB secret lookup

## 0.8.5
January 11, 2022

* `#66` - Treat functions as a valid redun Value
* Merge pull request #65 from insitro/DE4020-shardeddataset-save
* Remove superfluous type hint
* `#70` - improve module not found error
* `#76` - Link docs
* `#77` - Use a separate boto session per thread
* `#74` - Fix aws batch test warnings: DescribeJobs operation: The security token included in the request is invalid
* `#75` - DE-4104 Proposed fix for the s3fs dep issues
* `#54` - DE-3969 Evaluate default args in case they have expressions
* `#73` - DE-4102 Add inhert_cache=True to Column to suppress sqlalchemy warning
* Merge pull request #72 from insitro/typos_and_cleanups
* Correct misspellings of executor
* Remove superfluous parens
* Merge pull request #71 from insitro/add_build_timeout
* Add gh-actions build timeout
* `#67` - Generate public documents
* Add SQL query helper function for glue
* Add from_data constructor for ShardedS3Dataset
* `#13` - no need to use editable mode for redun install in examples
* Merge pull request #62 from insitro/DE-3993-override-source
* Clarifying comments on task.source during unpickling

## 0.8.4
December 02, 2021

* Merge remote-tracking branch 'pub/main'
* Merge pull request #58 from insitro/DE-3970-empty-string-vs-none-namespace
* Merge pull request #45 from insitro/DE3937-glue-imports
* `#11` - Fixes for several problems davidek saw
* `#55` - DE-3967 Fix case where subpromise is resolved first for Promise.all()
* `#46` - DE-3940 Variadic arguments should contribute to the eval_hash
* `#48` - Show a better error for workflow script not found
* `#43` - DE-3900 Handle missing logstream
* `#5` - Fix typos discovered by codespell

## 0.8.3
November 10, 2021

* `#32` - Fix version cli arg conflict
* `#38` - Add python 3.10 to codebuild
* `#36` - Clarify release triggering documentation
* Merge pull request #33 from insitro/prevent-double-ci-runs - Prevent double github actions runs

## 0.8.2
November 10, 2021

* `#23` - Add support for task wrappers
* `#4` - GitHub Action to run tox on every pull request
* `#3` - Import Executor for lines 36 and 38
* `#28` - Add more influence text for bioinfo and ML frameworks
* `#26` - Separate the various concepts of options
* `#25` - Document development installation and testing
* `#27` - DE-3811 Allow naked task decorator
* Merge pull request #21 from insitro/glue_py_bugfix
* Add clarifying comments
* Correctly format extra_py_files arg

## 0.8.1
October 24, 2021

* `#11` - improve job and promise memory usage

## 0.8.0
October 13, 2021

* `#7` - Try relaxing s3fs version range
* `#6` - DE-3650 set Task.source when deserializing
* `#5` - DE-3641 Add testing example
* `#4` - [DE-3531] Make additional python files work for glue jobs
* `#3` - Use public docker images
* `#2` - Update postgres install instructions
* `#1` - Add testing buildspec.
* Beginning of public repo
* `#312` - Update html links to markdown
* `#28` - Example of making postgres optional
* `#305` - Compute hash of ShardedS3Dataset after it's returned
* `#304` - Remove unnecessary DO NOT USE warning that contained core references
* `#300` - adding insitro redun.ini examples
* `#303` - DE-3485 clean up design doc
* `#301` - Fixes from library cleanup
* `#298` - DE-3490 Clean examples
* `#299` - Small lib cleanups
* `#297` - Remove unused variables in AWS glue code
* `#293` - add scripts for making a public version of the redun repo


## 0.7.4
September 30, 2021

* `#294` - More redun examples
* `#292` - Add git commit and origin_url tags to execution
* `#291` - Add more debugging logs for executors [WIP]


## 0.7.3
September 21, 2021

* `#286` - Revert "Update latest setuptools-conda with dependency version fix "
* `#288` - Move AWS batch statuses to constants

## 0.7.2
September 20, 2021

* `#286` - Update latest setuptools-conda with dependency version fix
* `#285` - DE-3421 Add executor-based job tags
* `#250` - DE-3433 Add `--rerun` option to `redun run`

## 0.7.1
September 16, 2021

* `#282` - DE-3399 Add tag APIs for redun_server
* `#272` - DE-2651 Add Spark executor
* `#277` - DE-2483 value store
* `#278` - Update postgres example config
* `#283` - Ignore self and cls variables for docstring test

## 0.7.0
September 09, 2021

* `#209` - DE-1592 Tag system v2
* `#253` - DE-2945 Use ProcessPoolExecutor through a separate executor
* `#280` - DE-3342 refactor CallGraphQuery
* Merge branch 'master' of github.com:insitro/redun
* update journal
* `#276` - small tutorial fixes
* `#274` - Test for numpy docstring adherence
* `#273` - Fix minor typos in docs
* `#271` - Update redun server's ecs cluster & setup CD for redun-dev
* `#260` - DE-3094 Implement a fast Files search page
* `#270` - add sys.exit(1)
* Merge branch 'master' of github.com:insitro/redun
* update journal
* `#268` - remove recursive query for job executions
* `#267` - fix migration with a missing commit

## 0.6.1
August 04, 2021

* `#265` - avoid Session as context use
* `#264` - Abandon release if failures are encountered during install or build

## 0.6.0
August 02, 2021

* `#261` - DE-3105 Add Job.execution_id migration
* `#259` - DE-3091 Add indexes to commonly queries columns
* `#258` - Small improvements to File such as file size and staging/copy defaults

## 0.5.1
July 16, 2021

* `#256` - Quote the DB password when creating DB URIs
* `#255` - Add support for Python 3.9
* `#245` - DE-2923 Extending the tutorial
* `#252` - DE-2939 Improve sort of sections in dataflow
* `#244` - Allow for non-python function as Task.source in dataflow visualization
* `#251` - DE-2922 Small fixes and improvements to batch executor and File
* `#249` - Update redun server to handle new task serialization format
* `#248` - DE-2900 Show db too new message

## 0.5.0
June 28, 2021

* `#246` - tee stdout and stderr
* `#229` - Backfill lonely Tasks, and update Task/Value serialization
* `#241` - DE-2001 Add File support for http, https, and ftp
* `#240` - DE-2850 Guided tutorial through workflow examples
* `#242` - add missing use_tempdir
* `#238` - Remove rogue . in alembic version info
* `#237` - use python3 in Makefile

## 0.4.15
June 15, 2021

* `#235` - fix: bump boto3 floor version for required botocore functionality
* `#232` - [DE-2761] Make tee tolerant of write errors for script batch jobs
* `#233` - DE-2632 -- Handle case where non-redun jobs have matching prefix
* `#234` - DE-2711 -- Fix optional cli args
* `#228` - Every time a db.Task is recorded, also record it as a db.Value

## 0.4.14
June 07, 2021

* `#224` - DE-2713 Add batch_tags option
* `#230` - Fix job status
* `#222` - upstream useful changes from tags-flag
* `#219` - DE-2660 Use a default root task for non-TaskExpressions

## 0.4.13
May 18, 2021

* `#220` - DE-2637 fix hashing of task_options_update
* `#204` - DE-2619 Use O(1) queries to speedup record serialization
* `#218` - DE-2635 Show unknown CallNodes for unfinished jobs
* `#217` - show keyword arguments
* `#216` - Fix isort line length
* `#215` - DE-2623 Dont use recursive for getting execution jobs
* `#213` - fix path term parsing
* `#212` - fix: redun server ECS service name in merge spec
* `#208` - Scope redun_server DB sessions at the request level
* `#210` - Cleanup logging of migrations
* `#211` - DE-2599 Use wait_until in aws batch tests to fix flaky tests

## 0.4.12
May 07, 2021

* `#206` - Add method to clone RedunBackendDB with connection pool sharing
* `#196` - DE-2325 Add database versioning commands
* `#201` - Add quick script to generate release notes

## 0.4.11
April 22th, 2021

* `#198` - Add support for configuration only task args
* `#197` - [DE-2428] Fix typed list check
* `#192` - DE-2434 Add more common tasks to functools
* `#194` - decouple scheduler from oneshot
* `#186` - Dockerize redun server, update directory layout and utils, add specs for prod deployment
* `#190` - DE-2464 Add postmortem debugging

## 0.4.10
April 12th, 2021

* `#188` - Don't let docker change terminal to raw mode
* `#187` - Tasks should allow novel kwargs
* `#180` - Use amazonlinux default pythons
* `#185` - Support job timeouts on batch
* `#182` - Lazy operators for redun Expressions

## 0.4.9
March 23rd, 2021

* `#183` - add py.typed
* `#177` - Support list args from cli
* `#178` - Fix settrace monkeypatch to restore debugging ability
* `#179` - DE-2370 Give array jobs a unique uuid
* `#181` - sqlalchemy 1.4.0 no longer allows postgres:// gotta be postgresql://
* `#176` - Improve pickle preview for constructor and __new__
* `#173` - Allow pycharm's debugger to work with redun
* `#175` - Set choices on parser for enum args
* `#174` - Allow use of id prefixes with push/pull commands
* `#171` - Make S3 repositories work
* `#172` - Match python 3.7 and 3.8 micro versions to match codebuild image


## 0.4.8
March 10th, 2021

* `#111` - Add concept of remote repos
* `#169` - Remove invalid positional arg in get_or_create_job_definition call
* `#147` - Dir should have File as subvalues for better dataflow recording
* `#165` - Fix lack of caching for catch expressions
* `#164` - Fix PartialTask's options() and partial() calls so that they interact correctly
* `#163` - Imports executors in the __init__
* `#155` - Use config_dir with redun_server

## 0.4.7
February 24th, 2021

**WARNING:** This version contains a bug in the `get_or_create_job_defintion` call in `batch_submit`. Do not use this version.

* `#156` - Automatic publishing of packages and docs
* `#153` - Use existing job def
* `#116` - Display dataflow
* `#154` - Fix data provenance recording for seq scheduler task
* `#152` - Fix pickling expression upstreams
* `#136` - Add redux to redun_server
* `#151` - Record stderr from scripts on batch
* `#149` - Add support for generating DB URI from AWS secret
* `#150` - Document max value size
* `#146` - Cryptic error for large falues
* `#148` - Simplify Scheduler.run() to take expressions
* `#145` - Add nout task option for tuples
* `#144` - Increase sqlalchemy requirement to 1.3.17
* `#143` - Package on submit not start

## 0.4.6
February 3rd, 2021

* `#141` - Only gather inflight jobs on batch on first submission

## 0.4.5
January 28th, 2021

* `#139` - Propagate batch script errors
* `#137` - Override CannotInspectContainerError batch errors
* `#138` - Fix pickle preview for classes where the module can't be found
* `#133` - Small fixes from demo talk
* `#132` - Small improvements to File.copy_to and self-stagin

## 0.4.4
January 15th, 2021

* `#131` - Fix catch dataflow
* `#134` - Add notebook example of redun scheduler evaluation
* `#128` - Make redun compatible with sqlalchemy-1.4.0b1
* `#129` - Add pickle_preview for unknown classes
* `#130` - Fix catch dataflow
* `#127` - Add FAQ page to docs
* `#126` - Require sorted imports

## 0.4.3
January 5th, 2021

* `#122` - Stronger type checking for task calls
* `#101` - Record CallNodes when an exception is raised
* `#86` - Scheduler tasks

## 0.4.2
January 4th, 2021

* `#121` - Array job reuniting fix

## 0.4.1
December 23rd, 2020

* `#119` - Bugfix to correctly restart job array monitor thread

## 0.4.0
December 15th, 2020

* `#83` - Detect and submit job arrays to AWS batch
* `#114` - Adds job definition option to run container in privileged mode

## 0.3.12
December 10th, 2020

* `#76` - Improve querying of logs

## 0.3.11
December 8th, 2020

* `#109` - Permalink update in README
* `#108` - Automated release

## 0.3.10
December 3rd, 2020

* `#104` - use ECR for postgres image
* `#95` - Hard fail on script errors
* `#100` - Show more information in logs and traceback
* `#102` - Fix check-valid=shallow to use the original call node
* `#98` - Skip license check when building conda packages
* `#105` - Typecheck map_nested_value
* `#103` - Fix script reactivity to inputs and outputs
* `#106` - Small clean up of batch logs

## 0.3.9
November 25th, 2020

* `#96` - Default to interactive debugging
* `#81` - Allow REDUN_CONFIG environment variable to specify config directory
* `#92` - DE-1922 tolerate missing logs for failed jobs

## 0.3.8
November 18th, 2020

* `#89` - Respect no-cache for job reuniting.
* `#88` - Assume batch output after completion is valid.
* `#87` - Fix filesystem caching and Dir hashing caching.
* `#85` - Add step to publish pypi package in publish script.
* `#84` - Fix package name in dependencies notes in README.

## 0.3.7
November 12th, 2020

* `#80` - redun import paths should take precedence over system imports.
* `#79` - fix default arg parsing and prefix args.

## 0.3.6
November 10th, 2020

* `#73` - Allow users to customize `setup_scheduler()`.

## 0.3.5
November 10, 2020

* `#77` - Check version of redun cli in docker container.

## 0.3.4
October 29th, 2020

* `#72` - Use current working directory when importing a module.
* `#64` - Some optimizations for AWS Batch large fanout.

## 0.3.3
October 28th, 2020

* `#71` - Don't fetch batch logs when debug=True

## 0.3.2
October 27th, 2020

* `#66` - Fix import_script to properly support module-style

## 0.3.1

* Fix bug with using s3fs >= 0.5

## 0.3
October 20th, 2020

* Improve display of errors and logs for AWS Batch jobs.

## 0.2.5
October 14th, 2020

* `#57` - Improve redun traceback for failed jobs.
* `#56` - Fix local shell error propagation.
* `#54` - Add documentation on required dependencies.

## 0.2.4
October 6, 2020

* Encourage defining task namespaces by raising a warning. The warning can be ignored using a [configuration option](config.md#ignore-warnings).


## 0.2.3
September 25, 2020

* Fixes FileNotFoundError occurring when using AWS Batch tasks, by avoiding the s3fs cache.


## 0.2.2
August 27, 2020

* Require database credentials to be specified by environment variables


## 0.2.1

August 9, 2020

 * Fix duplicate upstream bug.


## 0.2.0

August 7, 2020

 * Add support for Python 3.8


## 0.1.1

July 29, 2020

 * Drop dependency on bcode as it has no conda package and the repo appears abandoned.


## 0.1

 * Initial release.
