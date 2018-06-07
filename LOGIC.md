```
Deletes are writes!

start_transaction:
    check: transaction should not be started

    changes: create transaction id
    effects: monitor client
    data: transaction id

read:
    check: transaction should be started

    changes: aquire locks
    effects: []
    data: read data
    optimisation: read from delayed writes

write:
    check: transaction should be started

    changes: aquire locks
    effects: []
    data: delayed writes

commit:
    check: transaction should be started

    changes: clean locks, remove transaction id
    side-effects: write to mnesia
    effects: demonitor client
    data: ok/error

monitor DOWN:
    changes: clean locks, remove transaction id
    effects: demonitor client

On error: clean locks, demonitor the client and reply with error
All errors abort the transaction.

Do we need a timeout?

Transaction state:
    transaction id
    locks
    writes

Transaction started:
    transaction state exists.

```