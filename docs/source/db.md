---
tocpdeth: 3
---

# Database repo

redun stores data provenance and cached values in *repositories*, (repos for short), similar to how [git stores commit graphs in repos](https://git-scm.com/book/en/v2/Git-Basics-Getting-a-Git-Repository).
redun repos are currently implemented using either sqlite (the default) or PostgreSQL.
Additionally, users may specify an [external key-value store](#optional-value-store) to avoid bloating the primary repo with large binary objects.

Once your workflows mature to the point where you want to share them or their results with collaborators, we recommend configuring a persistent database repo for provenance tracking.
See the [backend configuration](config.md#backend) section for details.

## Database migration

The redun CLI will automatically initialize and upgrade the redun database in most cases, so users will oftentimes not need to manage database versioning themselves. However, there are certain situations where users may want or need to manage database upgrades explicitly, such as upgrading a central shared PostgreSQL database. Here, we review common commands for inspecting and managing database version upgrades.

When the redun CLI is run for the first on a workflow script, by default a new sqlite database is created at `.redun/redun.db`.

```sh
redun run workflow.py main

# .redun/redun.db sqlite database is created and upgraded to latest version known to the CLI.
```

The version of the database can be queried using `redun db info`, which should produce output something like:

```sh
redun :: version 0.4.11
config dir: /Users/rasmus/projects/redun/.redun

db version: 2.0
CLI requires db versions: >=2,<3
CLI compatible with db: True
```

The line `CLI requires db versions: >=2,<3` specifies what range of database versions the currently install CLI can work with. If the CLI requires a newer database version, the database will need to be upgraded before use.

To see what database versions are available to the currently installed redun CLI, use the following:

```
$ redun db versions

Version Migration    Description

1.0     806f5dcb11bf Prototype schema.
2.0     647c510a77b1 Initial production schema.
```

To upgrade the database to a new version, use the following:

```
$ redun db upgrade 3.0

redun :: version 0.4.11
config dir: /Users/rasmus/projects/redun/.redun

Initial db version: 2.0
[redun] Upgrading db from version 2.0 to 3.0...
Final db version: 3.0
```

Downgrades can be done with `redun db downgrade <version>`.


### Automatic database upgrading

For the most common use case, the redun CLI uses a local sqlite database file (e.g. `.redun/redun.db`).
Since such a database is used by only one client, it is typically safe to automatically upgrade the database if needed.
Such an upgrade will happen automatically on the next `redun run ...` command.
If you would like to disable automatic upgrades, it can be turned off with the [`automigrate`](config.md#automigrate) configuration option.
Automigration is not used for non-sqlite databases, since a centrally used database will likely need more coordination to not disrupt clients.


### Database version capabilities

Each version of the redun library requires the redun database to be within a specific version range. To understand what database version is required for your library or database, consult the table below.

| redun lib version range | required database version range |
|-------------------------|---------------------------------|
| <=0.4.10                | >=1 <2                          |
| >=0.4.11 <=0.5.0        | >=2 <3                          |
| >=0.5.1 <=0.6.0         | >=3.0 <4                        |
| >=0.6.1                 | >=3.1 <4                        |


### No downtime migrations

The compatibility ranges between the library and database are designed to allow gradual upgrades, so that redun clients never need to fully stop access.

- Migrations are designed to be *pre-deploy*, that is, they are always applied to the database (e.g. `redun db upgrade`) before upgrading clients.
- Minor database versions (e.g `3.0 --> 3.1`) can be upgraded to without disruption of current clients, because they typically contain non-breaking changes such as adding indexes or changing constraints.
- To upgrade to a major database version (e.g. `3.0 --> 4.0`) without downtime, all clients need to allow the new major version (see table above for details) before the migration is applied.


## Optional value store

By default, values are stored in the database as BLOB columns.
For most usage patterns, these BLOBs represent the fastest source of growth in the repo.
Users can specify an external key-value store and a minimum size cutoff beyond which values are written to the value store instead of the primary repo.
See `ValueStore` for implementation details.
