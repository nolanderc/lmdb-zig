const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const use_llvm = b.option(bool, "use-llvm", "Use LLVM as the codegen backend [default=true]") orelse true;

    const lib = b.addStaticLibrary(.{
        .name = "lmdb",
        .root_source_file = .{ .path = "lmdb.zig" },
        .target = target,
        .optimize = optimize,
        .use_lld = use_llvm,
        .use_llvm = use_llvm,
    });

    lib.linkLibC();
    lib.addIncludePath(.{ .path = "libraries/liblmdb/" });
    lib.addCSourceFiles(.{ .files = &.{
        "libraries/liblmdb/mdb.c",
        "libraries/liblmdb/midl.c",
    } });

    b.installArtifact(lib);

    const example = b.addExecutable(.{
        .name = "example",
        .root_source_file = .{ .path = "examples/main.zig" },
        .optimize = optimize,
        .target = target,
        .use_lld = use_llvm,
        .use_llvm = use_llvm,
    });
    example.root_module.addImport("lmdb", &lib.root_module);

    const run_example = b.addRunArtifact(example);
    if (b.args) |args| run_example.addArgs(args);
    b.step("run", "run the example executable")
        .dependOn(&run_example.step);
}
