const std = @import("std");

pub const c = @cImport({
    @cInclude("lmdb.h");
    @cInclude("midl.h");
});

pub const max_key_size_default = 511;

pub const Environment = struct {
    handle: *c.MDB_env,

    pub const InitOptions = struct {
        map_size: ?usize = null,
        max_readers: ?u32 = null,
        max_databases: ?u32 = null,

        open: OpenOptions,
    };

    pub const OpenOptions = struct {
        /// Path to the directory where the database files should be stored.
        path: [:0]const u8,

        /// Special options for the environment
        flags: OpenFlags = .{},

        /// File permissions. Unix only.
        mode: c.mdb_mode_t = 0o664,
    };

    pub const OpenFlags = Bitflags(enum(c_uint) {
        /// use a fixed address for the mmap region. This flag must be
        /// specified when creating the environment, and is stored persistently
        /// in the environment. If successful, the memory map will always
        /// reside at the same virtual address and pointers used to reference
        /// data items in the database will be constant across multiple
        /// invocations. This option may not always work, depending on how the
        /// operating system has allocated memory to shared libraries and other
        /// uses. The feature is highly experimental.
        fixed_map = c.MDB_FIXEDMAP,

        /// By default, LMDB creates its environment in a directory whose
        /// pathname is given in path, and creates its data and lock files
        /// under that directory. With this option, path is used as-is for the
        /// database main data file. The database lock file is the path with
        /// "-lock" appended.
        no_sub_dir = c.MDB_NOSUBDIR,

        /// Open the environment in read-only mode. No write operations will be
        /// allowed. LMDB will still modify the lock file - except on read-only
        /// filesystems, where LMDB does not use locks.
        read_only = c.MDB_RDONLY,

        /// Use a writeable memory map unless MDB_RDONLY is set. This uses
        /// fewer mallocs but loses protection from application bugs like wild
        /// pointer writes and other bad updates into the database. This may be
        /// slightly faster for DBs that fit entirely in RAM, but is slower for
        /// DBs larger than RAM. Incompatible with nested transactions. Do not
        /// mix processes with and without MDB_WRITEMAP on the same
        /// environment. This can defeat durability (mdb_env_sync etc).
        write_map = c.MDB_WRITEMAP,

        /// Flush system buffers to disk only once per transaction, omit the
        /// metadata flush. Defer that until the system flushes files to disk,
        /// or next non-MDB_RDONLY commit or mdb_env_sync(). This optimization
        /// maintains database integrity, but a system crash may undo the last
        /// committed transaction. I.e. it preserves the ACI (atomicity,
        /// consistency, isolation) but not D (durability) database property.
        /// This flag may be changed at any time using mdb_env_set_flags().
        no_meta_sync = c.MDB_NOMETASYNC,

        /// Don't flush system buffers to disk when committing a transaction.
        /// This optimization means a system crash can corrupt the database or
        /// lose the last transactions if buffers are not yet flushed to disk.
        /// The risk is governed by how often the system flushes dirty buffers
        /// to disk and how often mdb_env_sync() is called. However, if the
        /// filesystem preserves write order and the MDB_WRITEMAP flag is not
        /// used, transactions exhibit ACI (atomicity, consistency, isolation)
        /// properties and only lose D (durability). I.e. database integrity is
        /// maintained, but a system crash may undo the final transactions.
        /// Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves the system with no
        /// hint for when to write transactions to disk, unless mdb_env_sync()
        /// is called. (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable. This
        /// flag may be changed at any time using mdb_env_set_flags().
        no_sync = c.MDB_NOSYNC,

        /// When using MDB_WRITEMAP, use asynchronous flushes to disk. As with
        /// MDB_NOSYNC, a system crash can then corrupt the database or lose
        /// the last transactions. Calling mdb_env_sync() ensures on-disk
        /// database integrity until next commit. This flag may be changed at
        /// any time using mdb_env_set_flags().
        map_async = c.MDB_MAPASYNC,

        /// Don't use Thread-Local Storage. Tie reader locktable slots to
        /// MDB_txn objects instead of to threads. I.e. mdb_txn_reset() keeps
        /// the slot reserved for the MDB_txn object. A thread may use parallel
        /// read-only transactions. A read-only transaction may span threads if
        /// the user synchronizes its use. Applications that multiplex many
        /// user threads over individual OS threads need this option. Such an
        /// application must also serialize the write transactions in an OS
        /// thread, since LMDB's write locking is unaware of the user threads.
        no_tls = c.MDB_NOTLS,

        /// Don't do any locking. If concurrent access is anticipated, the
        /// caller must manage all concurrency itself. For proper operation the
        /// caller must enforce single-writer semantics, and must ensure that
        /// no readers are using old transactions while a writer is active. The
        /// simplest approach is to use an exclusive lock so that no readers
        /// may be active at all when a writer begins.
        no_lock = c.MDB_NOLOCK,

        /// Turn off readahead. Most operating systems perform readahead on
        /// read requests by default. This option turns it off if the OS
        /// supports it. Turning it off may help random read performance when
        /// the DB is larger than RAM and system RAM is full. The option is not
        /// implemented on Windows.
        no_read_ahead = c.MDB_NORDAHEAD,

        /// Don't initialize malloc'd memory before writing to unused spaces in
        /// the data file. By default, memory for pages written to the data
        /// file is obtained using malloc. While these pages may be reused in
        /// subsequent transactions, freshly malloc'd pages will be initialized
        /// to zeroes before use. This avoids persisting leftover data from
        /// other code (that used the heap and subsequently freed the memory)
        /// into the data file. Note that many other system libraries may
        /// allocate and free memory from the heap for arbitrary uses. E.g.,
        /// stdio may use the heap for file I/O buffers. This initialization
        /// step has a modest performance cost so some applications may want to
        /// disable it using this flag. This option can be a problem for
        /// applications which handle sensitive data like passwords, and it
        /// makes memory checkers like Valgrind noisy. This flag is not needed
        /// with MDB_WRITEMAP, which writes directly to the mmap instead of
        /// using malloc for pages. The initialization is also skipped if
        /// MDB_RESERVE is used; the caller is expected to overwrite all of the
        /// memory that was reserved in that case. This flag may be changed at
        /// any time using mdb_env_set_flags().
        no_mem_init = c.MDB_NOMEMINIT,
    });

    /// Helper function which fully initializes an environment: calls `create`,
    /// sets any options, and finally calls `open`.
    pub fn init(options: InitOptions) !Environment {
        const env = try create();
        errdefer env.close();
        if (options.map_size) |size| try env.setMapSize(size);
        if (options.max_readers) |count| try env.setMaxReaders(count);
        if (options.max_databases) |count| try env.setMaxDatabases(count);
        try env.open(options.open);
        return env;
    }

    /// Creates a new, empty, environment. Must be `open`ed before use.
    pub fn create() !Environment {
        var handle: ?*c.MDB_env = null;
        try check(c.mdb_env_create(&handle));
        return .{ .handle = handle.? };
    }

    pub fn setMapSize(env: Environment, size: usize) !void {
        try check(c.mdb_env_set_mapsize(env.handle, size));
    }

    pub fn setMaxReaders(env: Environment, count: u32) !void {
        try check(c.mdb_env_set_maxreaders(env.handle, count));
    }

    pub fn setMaxDatabases(env: Environment, count: u32) !void {
        try check(c.mdb_env_set_maxdbs(env.handle, count));
    }

    pub fn open(env: Environment, options: OpenOptions) !void {
        try check(c.mdb_env_open(
            env.handle,
            options.path,
            @bitCast(options.flags),
            options.mode,
        ));
    }

    pub fn close(env: Environment) void {
        c.mdb_env_close(env.handle);
    }

    pub fn getMaxKeySize(env: Environment) usize {
        return @intCast(c.mdb_env_get_maxkeysize(env.handle));
    }

    /// Checks for stale entries in the reader lock table, returning the number
    /// of dead readers that were cleared.
    pub fn checkDeadReaders(env: Environment) !usize {
        var dead: c_int = 0;
        try check(c.mdb_reader_check(env.handle, &dead));
        return @intCast(dead);
    }

    pub const beginTransaction = Transaction.begin;
};

/// A transaction within an environment.
/// May only be used on one thread at a time.
pub const Transaction = struct {
    handle: ?*c.MDB_txn,

    const BeginOptions = struct {
        parent: ?Transaction = null,
        flags: Flags = .{},
    };

    const Flags = Bitflags(enum(c_uint) {
        /// This transaction will not perform any write operations.
        read_only = c.MDB_RDONLY,
    });

    /// Begin a new transaction.
    pub fn begin(env: Environment, options: BeginOptions) !Transaction {
        var handle: ?*c.MDB_txn = null;
        try check(c.mdb_txn_begin(
            env.handle,
            if (options.parent) |parent| parent.handle else null,
            @bitCast(options.flags),
            &handle,
        ));
        return .{ .handle = handle.? };
    }

    /// Cancel all operations performed within the transaction.
    /// Frees the transaction handle, freeing all associated resources.
    ///
    /// Aborting an already comitted/aborted transaction is a no-op.
    pub fn abort(txn: *Transaction) void {
        if (txn.handle == null) return;
        defer txn.handle = null;
        c.mdb_txn_abort(txn.handle);
    }

    /// Commit all the operations of the transaction into the database.
    /// Frees the transaction handle, freeing all associated resources.
    pub fn commit(txn: *Transaction) !void {
        defer txn.handle = null;
        try check(c.mdb_txn_commit(txn.handle));
    }

    /// Abort the transaction like `abort`, but keep the transaction handle alive.
    /// Use `renew` before using the transaction again.
    pub fn reset(txn: Transaction) !void {
        try check(c.mdb_txn_reset(txn.handle));
    }

    /// Restarts a `reset` read-only  transaction, allowing it to be used again.
    pub fn renew(txn: Transaction) !void {
        try check(c.mdb_txn_renew(txn.handle));
    }

    pub const Id = usize;

    /// This returns the identifier associated with this transaction. For a
    /// read-only transaction, this corresponds to the snapshot being read;
    /// concurrent readers will frequently have the same transaction ID.
    pub fn id(txn: Transaction) Id {
        return c.mdb_txn_id(txn.handle);
    }

    /// Get the environment the transaction is part of.
    pub fn getEnv(txn: Transaction) Environment {
        return .{ .handle = c.mdb_txn_env(txn.handle).? };
    }

    pub const openDatabase = Database.open;

    /// Delete all pairs in the database.
    pub fn clearDatabase(txn: Transaction, db: Database) !void {
        try check(c.mdb_drop(txn.handle, db.handle, 0));
    }

    /// Drop and close the database.
    pub fn dropDatabase(txn: Transaction, db: Database) !void {
        try check(c.mdb_drop(txn.handle, db.handle, 1));
    }

    /// Get a value stored in the database.
    pub fn get(txn: Transaction, db: Database, key: []const u8) !?[]const u8 {
        var key_val = c.MDB_val{
            .mv_size = key.len,
            .mv_data = @ptrCast(@constCast(key.ptr)),
        };

        var data_val: c.MDB_val = undefined;

        check(c.mdb_get(txn.handle, db.handle, &key_val, &data_val)) catch |err| switch (err) {
            error.KeyNotFound => return null,
            else => return err,
        };

        return @as([*]u8, @ptrCast(data_val.mv_data))[0..data_val.mv_size];
    }

    pub const PutFlags = Bitflags(enum(c_uint) {
        /// Enter the new key/data pair only if it does not already appear in
        /// the database. This flag may only be specified if the database was
        /// opened with MDB_DUPSORT. The function will return MDB_KEYEXIST if
        /// the key/data pair already appears in the database.
        no_dup_data = c.MDB_NODUPDATA,

        /// Enter the new key/data pair only if the key does not already appear
        /// in the database. The function will return MDB_KEYEXIST if the key
        /// already appears in the database, even if the database supports
        /// duplicates (MDB_DUPSORT). The data parameter will be set to point
        /// to the existing item.
        no_overwrite = c.MDB_NOOVERWRITE,

        /// Reserve space for data of the given size, but don't copy the given
        /// data. Instead, return a pointer to the reserved space, which the
        /// caller can fill in later - before the next update operation or the
        /// transaction ends. This saves an extra memcpy if the data is being
        /// generated later. LMDB does nothing else with this memory, the
        /// caller is expected to modify all of the space requested. This flag
        /// must not be specified if the database was opened with MDB_DUPSORT.
        reserve = c.MDB_RESERVE,

        /// Append the given key/data pair to the end of the database. This
        /// option allows fast bulk loading when keys are already known to be
        /// in the correct order. Loading unsorted keys with this flag will
        /// cause a MDB_KEYEXIST error.
        append = c.MDB_APPEND,

        /// As above, but for sorted dup data.
        append_dup = c.MDB_APPENDDUP,
    });

    pub const PutValue = struct {
        len: usize,
        ptr: ?[*]const u8,

        pub fn fromSlice(slice: []const u8) PutValue {
            return .{ .len = slice.len, .ptr = slice.ptr };
        }
    };

    pub const PutResult = union(enum) {
        ok,
        existing: []const u8,
        reserved: []u8,
    };

    /// Get a value stored in the database.
    ///
    /// In case the key already exists and the `no_overwrite` flag is set,
    /// retruns a pointer to the previous value.
    pub fn putRaw(
        txn: Transaction,
        db: Database,
        key: []const u8,
        value: PutValue,
        flags: PutFlags,
    ) !PutResult {
        var key_val = c.MDB_val{
            .mv_size = key.len,
            .mv_data = @ptrCast(@constCast(key.ptr)),
        };

        var data_val = c.MDB_val{
            .mv_size = value.len,
            .mv_data = @ptrCast(@constCast(value.ptr)),
        };

        const result = c.mdb_put(txn.handle, db.handle, &key_val, &data_val, @bitCast(flags));

        if (result == 0 and flags.reserve) {
            return .{ .reserved = @as([*]u8, @ptrCast(data_val.mv_data))[0..data_val.mv_size] };
        }

        if (result == c.MDB_KEYEXIST and flags.no_overwrite) {
            return .{ .existing = @as([*]const u8, @ptrCast(data_val.mv_data))[0..data_val.mv_size] };
        }

        try check(result);

        return .ok;
    }

    /// Insert a key-value pair into the database, replacing any previous value.
    pub fn put(txn: Transaction, db: Database, key: []const u8, data: []const u8) !void {
        _ = try txn.putRaw(db, key, PutValue.fromSlice(data), .{});
    }

    /// Attempts to insert a key-value pair into the database. If a value
    /// already exists for that key, returns the exsiting data without
    /// modifying it.
    pub fn getOrPut(txn: Transaction, db: Database, key: []const u8, data: []const u8) !?[]const u8 {
        const result = try txn.putRaw(db, key, PutValue.fromSlice(data), .{ .no_overwrite = true });
        return if (result == .existing) result.existing else null;
    }

    /// Reserve space for some data, returning a pointer to the value which
    /// should be filled by the caller before the transaction is committed.
    pub fn reserve(txn: Transaction, db: Database, key: []const u8, size: usize) ![]u8 {
        const result = try txn.putRaw(db, key, .{ .len = size, .ptr = null }, .{ .reserve = true });
        return result.reserved;
    }

    pub const GetOrReserveResult = union(enum) {
        existing: []const u8,
        reserved: []u8,
    };

    /// Reserve space for some data, returning a pointer to the value which
    /// should be filled by the caller before the transaction is committed.
    pub fn getOrReserve(txn: Transaction, db: Database, key: []const u8, size: usize) !GetOrReserveResult {
        const result = try txn.putRaw(db, key, .{ .size = size, .ptr = null }, .{
            .no_overwrite = true,
            .reserve = true,
        });
        return switch (result) {
            .existing => |old| .{ .existing = old },
            .reserved => |new| .{ .reserved = new },
            .ok => unreachable,
        };
    }

    /// Remove a key-value pair from the database.
    ///
    /// In case of duplicates, removes all values with the given key.
    /// To remove a single item, see `removeSingle`.
    pub fn remove(txn: Transaction, db: Database, key: []const u8) !void {
        var key_val = c.MDB_val{
            .mv_size = key.len,
            .mv_data = key.ptr,
        };

        try check(c.mdb_del(txn.handle, db.handle, &key_val, null));
    }

    /// Remove a single key-value pair from the database.
    ///
    /// In case of duplicates, removes the one with the given data.
    /// To remove a single item, see `removeSingle`.
    pub fn removeSingle(txn: Transaction, db: Database, key: []const u8, data: []const u8) !void {
        var key_val = c.MDB_val{
            .mv_size = key.len,
            .mv_data = key.ptr,
        };

        var data_val = c.MDB_val{
            .mv_size = data.len,
            .mv_data = data.ptr,
        };

        try check(c.mdb_del(txn.handle, db.handle, &key_val, &data_val));
    }

    pub const openCursor = Cursor.open;
};

pub const Database = struct {
    handle: c.MDB_dbi,

    pub const OpenOptions = struct {
        name: ?[*:0]const u8,
        flags: OpenFlags = .{},
    };

    pub const OpenFlags = Bitflags(enum(c_uint) {
        /// Keys are strings to be compared in reverse order, from the end of
        /// the strings to the beginning. By default, Keys are treated as
        /// strings and compared from beginning to end.
        reverse_key = c.MDB_REVERSEKEY,

        /// Duplicate keys may be used in the database. (Or, from another
        /// perspective, keys may have multiple data items, stored in sorted
        /// order.) By default keys must be unique and may have only a single
        /// data item.
        dup_sort = c.MDB_DUPSORT,

        /// Keys are binary integers in native byte order, either unsigned int
        /// or size_t, and will be sorted as such. The keys must all be of the
        /// same size.
        integer_key = c.MDB_INTEGERKEY,

        /// This flag may only be used in combination with MDB_DUPSORT. This
        /// option tells the library that the data items for this database are
        /// all the same size, which allows further optimizations in storage
        /// and retrieval. When all data items are the same size, the
        /// MDB_GET_MULTIPLE, MDB_NEXT_MULTIPLE and MDB_PREV_MULTIPLE cursor
        /// operations may be used to retrieve multiple items at once.
        dup_fixed = c.MDB_DUPFIXED,

        /// This option specifies that duplicate data items are binary
        /// integers, similar to MDB_INTEGERKEY keys.
        integer_dup = c.MDB_INTEGERDUP,

        /// This option specifies that duplicate data items should be compared
        /// as strings in reverse order.
        reverse_dup = c.MDB_REVERSEDUP,

        /// Create the named database if it doesn't exist. This option is not
        /// allowed in a read-only transaction or a read-only environment.
        create = c.MDB_CREATE,
    });

    /// Open a database in the environment of the transaction.
    ///
    /// If the transaction is aborted the handle will be closed automatically.
    /// After a successful commit the handle will reside in the shared
    /// environment, and may be used by other transactions.
    ///
    /// This function must not be called from multiple concurrent transactions in the same process.
    pub fn open(txn: Transaction, options: OpenOptions) !Database {
        var db: Database = undefined;
        try check(c.mdb_dbi_open(txn.handle, options.name, @bitCast(options.flags), &db.handle));
        return db;
    }

    pub fn close(db: Database, env: Environment) void {
        c.mdb_dbi_close(env.handle, db.handle);
    }
};

pub const Cursor = struct {
    handle: *c.MDB_cursor,

    pub fn open(txn: Transaction, db: Database) !Cursor {
        var handle: ?*c.MDB_cursor = null;
        try check(c.mdb_cursor_open(txn.handle, db.handle, &handle));
        return .{ .handle = handle.? };
    }

    pub fn close(cursor: Cursor) void {
        c.mdb_cursor_close(cursor.handle);
    }

    pub const Position = union(enum(c_uint)) {
        /// Position at first key/data item
        first = c.MDB_FIRST,

        /// Position at first data item of current key. Only for MDB_DUPSORT
        first_dup = c.MDB_FIRST_DUP,

        /// Position at key/data pair. Only for MDB_DUPSORT
        pair: [2][]const u8 = c.MDB_GET_BOTH,

        /// position at key, nearest data. Only for MDB_DUPSORT
        pair_range: [2][]const u8 = c.MDB_GET_BOTH_RANGE,

        /// Return key/data at current cursor position
        current = c.MDB_CURRENT,

        /// Position at last key/data item
        last = c.MDB_LAST,

        /// Position at last data item of current key. Only for MDB_DUPSORT
        last_dup = c.MDB_LAST_DUP,

        /// Position at next data item
        next = c.MDB_NEXT,

        /// Position at next data item of current key. Only for MDB_DUPSORT
        next_dup = c.MDB_NEXT_DUP,

        /// Position at first data item of next key
        next_nodup = c.MDB_NEXT_NODUP,

        /// Position at previous data item
        prev = c.MDB_PREV,

        /// Position at previous data item of current key. Only for MDB_DUPSORT
        prev_dup = c.MDB_PREV_DUP,

        /// Position at last data item of previous key
        prev_nodup = c.MDB_PREV_NODUP,

        /// Position at specified key
        key: []const u8 = c.MDB_SET_KEY,

        /// Position at first key greater than or equal to specified key.
        key_range: []const u8 = c.MDB_SET_RANGE,
    };

    pub const PositionMultiple = union(enum(c_int)) {
        /// Return key and up to a page of duplicate data items from
        /// current cursor position. Move cursor to prepare for MDB_NEXT_MULTIPLE.
        /// Only for MDB_DUPFIXED
        get_multiple = c.MDB_GET_MULTIPLE,

        /// Return key and up to a page of duplicate data items from next
        /// cursor position. Move cursor to prepare for MDB_NEXT_MULTIPLE. Only for
        /// MDB_DUPFIXED
        next_multiple = c.MDB_NEXT_MULTIPLE,
    };

    pub fn seek(cursor: Cursor, position: Position) !void {
        try check(c.mdb_cursor_get(cursor.handle, null, null, @intFromEnum(position)));
    }

    pub fn getPair(cursor: Cursor, position: Position) !?[2][]const u8 {
        var key_val: c.MDB_val = .{ .mv_size = 0, .mv_data = null };
        var data_val: c.MDB_val = .{ .mv_size = 0, .mv_data = null };

        switch (position) {
            .key, .key_range => |key| key_val = .{
                .mv_size = key.len,
                .mv_data = @ptrCast(@constCast(key.ptr)),
            },
            .pair, .pair_range => |pair| {
                key_val = .{
                    .mv_size = pair[0].len,
                    .mv_data = @ptrCast(@constCast(pair[0].ptr)),
                };
                data_val = .{
                    .mv_size = pair[1].len,
                    .mv_data = @ptrCast(@constCast(pair[1].ptr)),
                };
            },
            inline else => |data| comptime std.debug.assert(@TypeOf(data) == void),
        }

        check(c.mdb_cursor_get(
            cursor.handle,
            &key_val,
            &data_val,
            @intFromEnum(position),
        )) catch |err| switch (err) {
            error.KeyNotFound => return null,
            else => return err,
        };

        return .{
            @as([*]const u8, @ptrCast(key_val.mv_data))[0..key_val.mv_size],
            @as([*]const u8, @ptrCast(data_val.mv_data))[0..data_val.mv_size],
        };
    }

    pub fn get(cursor: Cursor, position: Position) !?[]const u8 {
        _, const value = try cursor.getPair(position) orelse return null;
        return value;
    }

    pub const PutFlags = Bitflags(enum(c_uint) {
        /// Enter the new key/data pair only if it does not already appear in
        /// the database. This flag may only be specified if the database was
        /// opened with MDB_DUPSORT. The function will return MDB_KEYEXIST if
        /// the key/data pair already appears in the database. The cursor will
        /// be positioned at the existing pair.
        no_dup_data = c.MDB_NODUPDATA,

        /// Enter the new key/data pair only if the key does not already appear
        /// in the database. The function will return MDB_KEYEXIST if the key
        /// already appears in the database, even if the database supports
        /// duplicates (MDB_DUPSORT). The cursor will be set to point
        /// to the existing item.
        no_overwrite = c.MDB_NOOVERWRITE,

        /// Reserve space for data of the given size, but don't copy the given
        /// data. Instead, return a pointer to the reserved space, which the
        /// caller can fill in later - before the next update operation or the
        /// transaction ends. This saves an extra memcpy if the data is being
        /// generated later. LMDB does nothing else with this memory, the
        /// caller is expected to modify all of the space requested. This flag
        /// must not be specified if the database was opened with MDB_DUPSORT.
        reserve = c.MDB_RESERVE,

        /// Append the given key/data pair to the end of the database. This
        /// option allows fast bulk loading when keys are already known to be
        /// in the correct order. Loading unsorted keys with this flag will
        /// cause a MDB_KEYEXIST error.
        append = c.MDB_APPEND,

        /// As above, but for sorted dup data.
        append_dup = c.MDB_APPENDDUP,
    });

    pub const PutValue = struct {
        len: usize,
        ptr: ?[*]const u8,

        pub fn fromSlice(slice: []const u8) PutValue {
            return .{ .len = slice.len, .ptr = slice.ptr };
        }
    };

    pub const PutResult = union(enum) {
        ok,
        existing,
        reserved: []u8,
    };

    /// Get a value stored in the database.
    ///
    /// In case the key already exists and the `no_overwrite` flag is set,
    /// retruns a pointer to the previous value.
    pub fn putRaw(
        cursor: Cursor,
        key: []const u8,
        value: PutValue,
        flags: PutFlags,
    ) !PutResult {
        var key_val = c.MDB_val{
            .mv_size = key.len,
            .mv_data = @ptrCast(@constCast(key.ptr)),
        };

        var data_val = c.MDB_val{
            .mv_size = value.len,
            .mv_data = @ptrCast(@constCast(value.ptr)),
        };

        const result = c.mdb_cursor_put(cursor.handle, &key_val, &data_val, @bitCast(flags));

        if (result == 0 and flags.reserve) {
            return .{ .reserved = @as([*]u8, @ptrCast(data_val.mv_data))[0..data_val.mv_size] };
        }

        if (result == c.MDB_KEYEXIST and flags.no_overwrite) {
            return .existing;
        }

        try check(result);

        return .ok;
    }

    /// Insert a key-value pair into the database, replacing any previous value.
    pub fn put(cursor: Cursor, key: []const u8, data: []const u8) !void {
        _ = try cursor.putRaw(key, PutValue.fromSlice(data), .{});
    }

    /// Attempts to insert a key-value pair into the database. If a value
    /// already exists for that key, returns the exsiting data without
    /// modifying it.
    pub fn getOrPut(cursor: Cursor, key: []const u8, data: []const u8) !?[]const u8 {
        const result = try cursor.putRaw(key, PutValue.fromSlice(data), .{ .no_overwrite = true });
        return if (result == .existing) cursor.get(.current) else null;
    }

    /// Reserve space for some data, returning a pointer to the value which
    /// should be filled by the caller before the transaction is committed.
    pub fn reserve(cursor: Cursor, key: []const u8, size: usize) ![]u8 {
        const result = try cursor.putRaw(key, .{ .len = size, .ptr = null }, .{ .reserve = true });
        return result.reserved;
    }

    pub const GetOrReserveResult = union(enum) {
        existing: []const u8,
        reserved: []u8,
    };

    /// Reserve space for some data, returning a pointer to the value which
    /// should be filled by the caller before the transaction is committed.
    pub fn getOrReserve(cursor: Cursor, key: []const u8, size: usize) !GetOrReserveResult {
        const result = try cursor.putRaw(key, .{ .size = size, .ptr = null }, .{
            .no_overwrite = true,
            .reserve = true,
        });
        return switch (result) {
            .existing => .{ .existing = cursor.get(.current).? },
            .reserved => |new| .{ .reserved = new },
            .ok => unreachable,
        };
    }

    /// Remove the current pair from the database.
    pub fn remove(cursor: Cursor) !void {
        try check(c.mdb_cursor_del(cursor.handle, 0));
    }
};

/// Given a (possibly sparse) enum with non-overlapping single-bit tags,
/// creates a packed struct with those tags as its fields.
fn Bitflags(comptime Enum: type) type {
    const Type = std.builtin.Type;

    const info = @typeInfo(Enum).Enum;
    const backing = info.tag_type;

    var fields: [@bitSizeOf(backing)]Type.StructField = undefined;

    for (0..@bitSizeOf(backing)) |bit| {
        fields[bit] = .{
            .name = std.fmt.comptimePrint("_{}", .{bit}),
            .type = bool,
            .default_value = &false,
            .is_comptime = false,
            .alignment = 0,
        };
    }

    for (info.fields) |field| {
        const bits = @as(backing, field.value);
        if (@popCount(bits) != 1)
            @compileError("expected just a single bit set in " ++ field.name);
        fields[@ctz(bits)].name = field.name;
    }

    return @Type(.{ .Struct = .{
        .layout = .@"packed",
        .backing_integer = backing,
        .is_tuple = false,
        .fields = &fields,
        .decls = &.{},
    } });
}

pub const Error = error{
    Unknown,

    // == LMDB == //

    KeyAlreadyExists,
    KeyNotFound,
    PageNotFound,
    Corrupted,
    FatalError,
    VersionMismatch,
    Invalid,
    MapFull,
    MaxDatabases,
    MaxReaders,
    TlsFull,
    TxnFull,
    CursorFull,
    PageFull,
    MapResized,
    Incompatible,
    BadReaderSlot,
    BadTransaction,
    BadValueSize,
    BadDbi,

    // == libc == //

    FileNotFound,
    InvalidParameter,
    OutOfDiskSpace,
    OutOfMemory,
    IoFailure,
    /// Attempt to write in a read-only environment/transaction.
    ReadOnly,
};

fn check(code: c_int) Error!void {
    if (code < 0) {
        return switch (code) {
            @intFromEnum(std.posix.E.NOENT) => error.FileNotFound,
            @intFromEnum(std.posix.E.INVAL) => error.InvalidParameter,
            @intFromEnum(std.posix.E.NOSPC) => error.OutOfDiskSpace,
            @intFromEnum(std.posix.E.NOMEM) => error.OutOfMemory,
            @intFromEnum(std.posix.E.IO) => error.IoFailure,
            @intFromEnum(std.posix.E.ACCES) => error.ReadOnly,
            c.MDB_KEYEXIST => error.KeyAlreadyExists,
            c.MDB_NOTFOUND => error.KeyNotFound,
            c.MDB_PAGE_NOTFOUND => error.PageNotFound,
            c.MDB_CORRUPTED => error.Corrupted,
            c.MDB_PANIC => error.FatalError,
            c.MDB_VERSION_MISMATCH => error.VersionMismatch,
            c.MDB_INVALID => error.Invalid,
            c.MDB_MAP_FULL => error.MapFull,
            c.MDB_DBS_FULL => error.MaxDatabases,
            c.MDB_READERS_FULL => error.MaxReaders,
            c.MDB_TLS_FULL => error.TlsFull,
            c.MDB_TXN_FULL => error.TxnFull,
            c.MDB_CURSOR_FULL => error.CursorFull,
            c.MDB_PAGE_FULL => error.PageFull,
            c.MDB_MAP_RESIZED => error.MapResized,
            c.MDB_INCOMPATIBLE => error.Incompatible,
            c.MDB_BAD_RSLOT => error.BadReaderSlot,
            c.MDB_BAD_TXN => error.BadTransaction,
            c.MDB_BAD_VALSIZE => error.BadValueSize,
            c.MDB_BAD_DBI => error.BadDbi,
            else => error.Unknown,
        };
    }
}
