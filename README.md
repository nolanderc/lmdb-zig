# lmdb-zig

LMDB using the Zig build system. Includes Ziggified bindings.

## Versions

- The `latest` branch tracks the latest release in the upstream LMDB repo.
- We use tags named `zA.B.C`, corresponding to the upstream version numbers.


## Example

```zig
const std = @import("std");
const lmdb = @import("lmdb");

/// 
const std = @import("std");
const lmdb = @import("lmdb");

pub fn main() !void {
    const db_path = "/tmp/example-db";

    try std.fs.cwd().makePath(db_path);

    const env = try lmdb.Environment.init(.{
        .map_size = 1 << 32,
        .max_databases = 2,
        .open = .{ .path = db_path },
    });
    defer env.close();

    {
        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        const db = try txn.openDatabase(.{ .name = "people", .flags = .{ .create = true } });
        defer db.close(env);
        try txn.clearDatabase(db);

        // prints 'null':
        std.debug.print("value: {?s}\n", .{try txn.get(db, "James T. Kirk")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Jean-Luc Picard")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Luke Skywalker")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Darth Vader")});

        // insert values into the database:
        try txn.put(db, "James T. Kirk", "Star Trek");
        try txn.put(db, "Luke Skywalker", "Star Wars");

        // conditionally insert
        if (try txn.getOrPut(db, "Luke Skywalker", "Star Wars IV")) |previous| {
            std.debug.print("found existing: {s}\n", .{previous});
        }

        // reserve space before inserting:
        const reserved = try txn.reserve(db, "Jean-Luc Picard", 9);
        @memcpy(reserved, "Star Trek");

        // read back the inserted values:
        std.debug.print("value: {?s}\n", .{try txn.get(db, "James T. Kirk")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Jean-Luc Picard")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Luke Skywalker")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Darth Vader")});

        // commit the changes to the database
        try txn.commit();
    }
}
```

It is also possible to use the raw C bindings under `@import("lmdb").c`
