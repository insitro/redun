# Custom values

redun allows customization of how values are processed, such as argument parsing, serialization, hashing, etc.

This example ([workflow.py](workflow.py)) shows how to parse JSON from the command line into a pydantic model.

```sh
pip install -r requirements.txt
```

```sh
redun run workflow.py main -- --user '{"id": 10, "name": "Alice"}'
```

Note, the extra `--` is used to force `--user` to be interpreted as task argument.

You can inspect for yourself that the `user` was serialized to the redun database using JSON (instead of the default pickling) using the sqlite CLI:

```sh
sqlite3 .redun/redun.db 'select * from value;'

c62d194c06eb29126fef2d2b5d04dcced229c68f|redun.examples.custom_values.User|application/json|{"id": 10, "name": "Alice"}
```
