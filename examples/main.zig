const std = @import("std");
const lmdb = @import("lmdb");

pub fn main() !void {
    std.testing.refAllDeclsRecursive(lmdb);

    const db_path = "/tmp/ramdisk/example-db";

    try std.fs.cwd().makePath(db_path);

    const env = try lmdb.Environment.init(.{
        .map_size = 1 << 32,
        .max_databases = 2,
        .open = .{ .path = db_path },
    });
    defer env.close();

    const db = try createDatabase(env, .{
        .name = "people",
        .flags = .{ .create = true },
    });
    defer db.close(env);

    const random = try createDatabase(env, .{
        .name = "random",
        .flags = .{ .create = true },
    });
    defer random.close(env);

    {
        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        std.debug.print("value: {?s}\n", .{try txn.get(db, "James T. Kirk")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Jean-Luc Picard")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Luke Skywalker")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Darth Vader")});

        try txn.put(db, "James T. Kirk", "Star Trek");
        try txn.put(db, "Luke Skywalker", "Star Wars");

        if (try txn.getOrPut(db, "Luke Skywalker", "Star Wars IV")) |previous| {
            std.debug.print("found existing: {s}\n", .{previous});
        }

        const reserved = try txn.reserve(db, "Jean-Luc Picard", 9);
        @memcpy(reserved, "Star Trek");

        std.debug.print("value: {?s}\n", .{try txn.get(db, "James T. Kirk")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Jean-Luc Picard")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Luke Skywalker")});
        std.debug.print("value: {?s}\n", .{try txn.get(db, "Darth Vader")});

        try txn.commit();
    }

    {
        const inserts: u64 = 100_000;

        std.debug.print("\nwriting {} values ...\n", .{inserts});

        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        {
            var timer = try std.time.Timer.start();
            var nanos: u64 = 0;
            defer std.debug.print("ns/insert: {}\n", .{nanos / inserts});
            defer std.debug.print("inserts/s: {}\n", .{std.time.ns_per_s * inserts / nanos});
            defer nanos = timer.read();

            for (0..inserts) |index| {
                var hasher = std.hash.Fnv1a_128.init();
                std.hash.autoHash(&hasher, index);
                const hash = hasher.final();
                try txn.put(random, std.mem.asBytes(&hash), "this is a value");
            }
        }

        try txn.commit();
    }

    {
        const reads: u64 = 100_000;

        std.debug.print("\nreading {} values ...\n", .{reads});

        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        {
            var timer = try std.time.Timer.start();
            var nanos: u64 = 0;
            defer std.debug.print("ns/read: {}\n", .{nanos / reads});
            defer std.debug.print("reads/s: {}\n", .{std.time.ns_per_s * reads / nanos});
            defer nanos = timer.read();

            for (0..reads) |index| {
                var hasher = std.hash.Fnv1a_128.init();
                std.hash.autoHash(&hasher, index);
                const hash = hasher.final();
                _ = try txn.get(random, std.mem.asBytes(&hash));
            }
        }

        try txn.commit();
    }

    {
        var deletes: u64 = 0;

        std.debug.print("\nclearing the database ...\n", .{});

        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        {
            var timer = try std.time.Timer.start();
            var nanos: u64 = 0;
            defer std.debug.print("ns/delete: {}\n", .{nanos / deletes});
            defer std.debug.print("deletes/s: {}\n", .{std.time.ns_per_s * deletes / nanos});
            defer nanos = timer.read();

            const cursor = try txn.openCursor(random);
            defer cursor.close();

            while (cursor.seek(.next)) {
                try cursor.remove();
                deletes += 1;
            } else |err| switch (err) {
                error.KeyNotFound => {},
                else => return err,
            }

            std.debug.print("removed {} items\n", .{deletes});
        }

        try txn.commit();
    }

    {
        const inserts: u64 = 100_000;

        std.debug.print("\nwriting {} values using a cursor ...\n", .{inserts});

        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        {
            var timer = try std.time.Timer.start();
            var nanos: u64 = 0;
            defer std.debug.print("ns/insert: {}\n", .{nanos / inserts});
            defer std.debug.print("inserts/s: {}\n", .{std.time.ns_per_s * inserts / nanos});
            defer nanos = timer.read();

            const cursor = try txn.openCursor(random);
            defer cursor.close();

            for (0..inserts) |index| {
                var hasher = std.hash.Fnv1a_128.init();
                std.hash.autoHash(&hasher, index);
                const hash = hasher.final();
                try cursor.put(std.mem.asBytes(&hash), "this is a value");
            }
        }

        try txn.commit();
    }

    {
        const reads: u64 = 100_000;

        std.debug.print("\nreading {} values using a cursor ...\n", .{reads});

        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        {
            var timer = try std.time.Timer.start();
            var nanos: u64 = 0;
            defer std.debug.print("ns/read: {}\n", .{nanos / reads});
            defer std.debug.print("reads/s: {}\n", .{std.time.ns_per_s * reads / nanos});
            defer nanos = timer.read();

            const cursor = try txn.openCursor(random);
            defer cursor.close();

            for (0..reads) |index| {
                var hasher = std.hash.Fnv1a_128.init();
                std.hash.autoHash(&hasher, index);
                const hash = hasher.final();
                _ = try cursor.get(.{ .key = std.mem.asBytes(&hash) });
            }
        }

        try txn.commit();
    }

    {
        var reads: u64 = 0;

        std.debug.print("\niterating all values using a cursor ...\n", .{});

        var txn = try env.beginTransaction(.{});
        defer txn.abort();

        {
            var timer = try std.time.Timer.start();
            var nanos: u64 = 0;
            defer std.debug.print("ns/read: {}\n", .{nanos / reads});
            defer std.debug.print("reads/s: {}\n", .{std.time.ns_per_s * reads / nanos});
            defer nanos = timer.read();

            const cursor = try txn.openCursor(random);
            defer cursor.close();

            while (try cursor.get(.next)) |_| {
                reads += 1;
            }

            std.debug.print("found {} items\n", .{reads});
        }

        try txn.commit();
    }

    std.debug.print("\ndone\n", .{});
}

fn createDatabase(env: lmdb.Environment, options: lmdb.Database.OpenOptions) !lmdb.Database {
    var txn = try env.beginTransaction(.{});
    defer txn.abort();

    const db = try txn.openDatabase(options);
    errdefer db.close(env);

    try txn.commit();

    return db;
}
