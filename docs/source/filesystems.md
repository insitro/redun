# Filesystems

redun supports reading and writing to various filesystems, including local, S3, GCS, and Azure Blob Storage.

The filesystem to be used is determined by the protocol in the file URL provided to `redun.file` constructor.
In most cases, users don't need to interact with `Filesystem` objects directly.

## Azure Blob Storage filesystem

This filesystem is considered experimental - please read the documentation carefully before using it.

### Usage

If you use `redun.File` or `Dir`, initialize it with `az` path: 
`az://container@accountname.blob.core.windows.net/path/to/file`. Other paths format (like HTTPS blob URI) are not
supported.

### Thread safety

By design, `fsspec` operations are not stateless, and there are potential issues with thread safety:
 - listings cache (dircache) - for results of ls-like commands. `glob` calls *likely* don't affect the cache, 
unlike `listdir` which we don't call currently, so all redun calls like `Dir.files()` are safe 
 - file access (see https://filesystem-spec.readthedocs.io/en/latest/features.html#file-buffering-and-random-access) -
users should assume IO file operations are unsafe and avoid reads/writes to the same file from multiple redun tasks running
on the same executor.

### Credentials lookup

For most cases, Azure SDKs should work without any issues with the default credentials, and that's what
`adlfs` (`fsspec` implementation for Azure storage) does when not prompted otherwise. 

Note that it **wasn't** tested on Azure Batch nodes, but `DefaultClientCredential` is expected to work inside
any Azure compute with properly configured managed identity.

However, inside AzureML compute that uses on-behalf-of authentication, DefaultClientCredential will not work.
There is a special credential class that works, but works only in synchronous mode. `adlfs` works with both sync 
and async credential classes, but it assumes custom credential classes are async. We do a small hack in the code 
to assign credential class to a correct class property.

Currently use a simple try-catch to determine if we can retrieve auth token from DefaultAzureCredential and use
AML credential as fallback option if AML env variables are set (`AZUREML_WORKSPACE_ID`). If both options fail,
we also check if `DEFAULT_IDENTITY_CLIENT_ID` variable is set - in AzureML, it holds the client id of the cluster's
managed identity. If it is, we try to create `ManagedIdentityCredential` with it.
