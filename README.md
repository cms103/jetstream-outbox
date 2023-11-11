# jetstream-outbox
Implementation of the outbox message publication pattern using Postgres and NATS Jetstream

## Background
When a process needs to write to a database and then send an event to a message system on success, there's a timing problem because it's not an atomic transaction:

```
    insert / update / delete
    commit
    <- If the process dies here the event is not sent
    send event <- If the send event fails the DB and event bus recipients are out of sync
```

The solution for this is to use the outbox pattern:

```
    insert / update / delete
    insert the event into the outbox table
    delete the event from the outbox table
    commit
```

The insert and then immediate delete of an event in the outbox table means that event data isn't being stored in the database. It does however push the outbox insert data into the database's replication system, which means that it can be reliably picked up and pushed into a messaging system. If something goes wrong with the transaction it is rolled back, and nothing comes through replication.

## Setup steps

### Database
Firstly Postgres needs to be setup to enable it's replication system. This means editing the server config file and setting the following parameters:
```
wal_level = logical
max_replication_slots = 5
max_wal_senders = 10
```

The `user` that will be used for jetstream-outbox to connect to Postgres will need to have the replication permission: `alter user <username> with replication;`

Our final database step is to create the outbox, start publication on it and create a replication slot that will track what data we've seen and what is new:

```
CREATE TABLE outbox (
    id uuid NOT NULL PRIMARY KEY,
    aggregatetype character varying(255) NOT NULL,
    aggregateid character varying(255) NOT NULL,
    payload jsonb
);

AlTER TABLE outbox REPLICA IDENTITY DEFAULT;

CREATE PUBLICATION jetstream_outbox FOR TABLE outbox;

SELECT pg_create_logical_replication_slot('jetstream_outbox', 'pgoutput');
```

### NATS Jetstream
We now need to decide where our messages will be deposited in NATS. The simplest configuration is a new JetStream store that covers all event types:

```
nats stream add jetstream_outbox --subjects=events.>
```

jetstream-outbox sends NATS messages with a subject of `prefix.aggregatetype.aggregateid` where:

 * `prefix` defaults to "events" and can be set on the commandline.
 * `aggregatetype` comes from the value inserted into the outbox table.
 * `aggregateid` comes from the value inserted into the outbox table.

### Running jetstream-outbox
To start the outbox process run jetstream-outbox passing in appropriate connection details:
```
Usage of ./jetstream-outbox:
  -creds string
    	NATS credentials file location
  -db string
    	Database connection details (default "postgres://user:password@127.0.0.1/dbname?replication=database")
  -logging string
    	Set the minimum logging level to debug,info,warn,error (default "info")
  -nats string
    	NATS server address (default "localhost")
  -prefix string
    	The top level prefix for events published to NATS (default "events")

```

## Example

Using the psql client we can see how events are generated and flow into NATS Jetstream:

```
dbname=> begin;
BEGIN
dbname=*> insert into outbox (id, aggregatetype, aggregateid, payload) values ('3e106c1f-5a02-4918-91fa-cfde386103b5', 'account', 'account_id_1', '{"msg": "A test account creation event"}');
INSERT 0 1
dbname=*> delete from outbox where id = '3e106c1f-5a02-4918-91fa-cfde386103b5';
DELETE 1
dbname=*> commit;
COMMIT
```

Using `nat stream view` we can see the event captured in JetStream:
```
nats stream view
? Select a Stream jetstream_outbox
[31] Subject: events.account.account_id_1 Received: 2023-11-11T16:42:28+01:00

  Nats-Msg-Id: 3e106c1f-5a02-4918-91fa-cfde386103b5

{"msg":"A test account creation event"}

16:42:32 Reached apparent end of data

```

## Notes
A few notes:

 * As the events are published into NATS with the `aggregatetype` as part of the subject, it's possible to create multiple JetStreams and have them handle different event types.
 * The `aggregateid` is intended to contain the unique ID of the entity for which the event is being created. As this forms part of the subject it can be used for subject mapping in NATS.
 * The `id` field in the outbox is placed into the `Nats-Msg-Id` header of each message. If JetStream de-duplication is used this will be used as a unique key.
 * The progress of which events have been placed into NATS is maintained within Postgres and is updated every 10s. When the `jetstream-outbox` process is restarted it may re-publish some messages and it will catch up with any missed messages.

This is intended to be a very lightweight solution. If you are looking for something that can handle more general change data capture requirements, target event systems other than NATS Jetstream or work with databases other than Postgres it's probably worth looking at https://debezium.io/.

