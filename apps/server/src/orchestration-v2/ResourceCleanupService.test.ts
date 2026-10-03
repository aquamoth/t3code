// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { CheckpointRef } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";

const VcsProcessTestLayer = VcsProcess.layer.pipe(Layer.provide(NodeServices.layer));
const CheckpointStoreTestLayer = CheckpointStore.layer.pipe(
  Layer.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcessTestLayer))),
);
const TestLayer = ResourceCleanupService.live.pipe(
  Layer.provide(Layer.mock(TerminalManager.TerminalManager)({})),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), { prefix: "t3-cleanup-test-" })),
  Layer.provideMerge(VcsProcessTestLayer),
  Layer.provideMerge(NodeServices.layer),
);

const git = Effect.fn(function* (cwd: string, args: ReadonlyArray<string>) {
  const process = yield* VcsProcess.VcsProcess;
  const result = yield* process.run({
    operation: "ResourceCleanupService.test.git",
    command: "git",
    cwd,
    args,
    timeoutMs: 10_000,
  });
  return result.stdout.trim();
});

const initRepo = Effect.fn(function* (cwd: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* git(cwd, ["init"]);
  yield* git(cwd, ["config", "user.email", "test@test.com"]);
  yield* git(cwd, ["config", "user.name", "Test"]);
  yield* fileSystem.writeFileString(NodePath.join(cwd, "README.md"), "# test\n");
  yield* git(cwd, ["add", "."]);
  yield* git(cwd, ["commit", "-m", "initial commit"]);
});

const ref = (name: string) => CheckpointRef.make(`refs/t3/orchestration-v2/checkpoints/${name}`);
const listCheckpointRefs = (cwd: string) =>
  git(cwd, ["for-each-ref", "--format=%(refname)", "refs/t3/"]);

it.layer(TestLayer)("ResourceCleanupService.cleanupCheckpointRefs", (it) => {
  it.effect("deletes only the listed refs, including packed ones, and is idempotent", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-refs-" });
      yield* initRepo(cwd);
      for (const name of ["deleted/ordinal/0", "deleted/ordinal/1", "kept/ordinal/0"]) {
        yield* git(cwd, ["update-ref", ref(name), "HEAD"]);
      }
      yield* git(cwd, ["pack-refs", "--all"]);
      const cleanup = yield* ResourceCleanupService.ResourceCleanupService;
      const targets = [
        {
          cwd,
          checkpointRefs: [
            ref("deleted/ordinal/0"),
            ref("deleted/ordinal/1"),
            ref("never/existed"),
          ],
        },
      ];
      yield* cleanup.cleanupCheckpointRefs(targets);
      yield* cleanup.cleanupCheckpointRefs(targets);
      assert.strictEqual(yield* listCheckpointRefs(cwd), ref("kept/ordinal/0"));
      assert.strictEqual(yield* git(cwd, ["status", "--porcelain"]), "");
    }),
  );

  it.effect("skips targets whose directory is gone or is not a repository", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repo = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-repo-" });
      yield* initRepo(repo);
      yield* git(repo, ["update-ref", ref("deleted/ordinal/0"), "HEAD"]);
      const plain = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-plain-" });
      const cleanup = yield* ResourceCleanupService.ResourceCleanupService;
      yield* cleanup.cleanupCheckpointRefs([
        { cwd: NodePath.join(plain, "removed-worktree"), checkpointRefs: [ref("x/ordinal/0")] },
        { cwd: plain, checkpointRefs: [ref("x/ordinal/0")] },
        { cwd: repo, checkpointRefs: [ref("deleted/ordinal/0")] },
      ]);
      assert.strictEqual(yield* listCheckpointRefs(repo), "");
    }),
  );

  it.effect("deletes a removed worktree's refs through the project root", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-root-" });
      yield* initRepo(root);
      const worktree = NodePath.join(root, ".worktrees", "feature");
      yield* git(root, ["worktree", "add", "-b", "feature", worktree]);
      yield* git(worktree, ["update-ref", ref("deleted/ordinal/0"), "HEAD"]);
      yield* git(root, ["update-ref", ref("kept/ordinal/0"), "HEAD"]);
      assert.strictEqual(
        yield* listCheckpointRefs(root),
        [ref("deleted/ordinal/0"), ref("kept/ordinal/0")].join("\n"),
      );
      yield* git(root, ["worktree", "remove", "--force", worktree]);
      const cleanup = yield* ResourceCleanupService.ResourceCleanupService;
      yield* cleanup.cleanupCheckpointRefs([
        { cwd: worktree, checkpointRefs: [ref("deleted/ordinal/0")] },
        { cwd: root, checkpointRefs: [ref("deleted/ordinal/0")] },
      ]);
      assert.strictEqual(yield* listCheckpointRefs(root), ref("kept/ordinal/0"));
    }),
  );

  it.effect("fails on a held lock after trying every target, and a retry finishes the job", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const locked = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-locked-" });
      yield* initRepo(locked);
      yield* git(locked, ["update-ref", ref("deleted/ordinal/0"), "HEAD"]);
      const lockPath = NodePath.join(locked, ".git", `${ref("deleted/ordinal/0")}.lock`);
      yield* fileSystem.writeFileString(lockPath, "");
      const free = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-free-" });
      yield* initRepo(free);
      yield* git(free, ["update-ref", ref("deleted/ordinal/0"), "HEAD"]);
      const cleanup = yield* ResourceCleanupService.ResourceCleanupService;
      const targets = [
        { cwd: locked, checkpointRefs: [ref("deleted/ordinal/0")] },
        { cwd: free, checkpointRefs: [ref("deleted/ordinal/0")] },
      ];

      const result = yield* Effect.result(cleanup.cleanupCheckpointRefs(targets));
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.operation, "checkpoint");
        assert.strictEqual(result.failure.cwd, locked);
      }
      assert.strictEqual(yield* listCheckpointRefs(locked), ref("deleted/ordinal/0"));
      assert.strictEqual(yield* listCheckpointRefs(free), "");

      yield* fileSystem.remove(lockPath);
      yield* cleanup.cleanupCheckpointRefs(targets);
      assert.strictEqual(yield* listCheckpointRefs(locked), "");
    }),
  );

  it.effect("never deletes refs outside the checkpoint namespace", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cleanup-guard-" });
      yield* initRepo(cwd);
      yield* git(cwd, ["branch", "victim"]);
      yield* git(cwd, ["update-ref", ref("deleted/ordinal/0"), "HEAD"]);
      const cleanup = yield* ResourceCleanupService.ResourceCleanupService;
      yield* cleanup.cleanupCheckpointRefs([
        {
          cwd,
          checkpointRefs: [CheckpointRef.make("refs/heads/victim"), ref("deleted/ordinal/0")],
        },
        { cwd, checkpointRefs: [CheckpointRef.make("refs/heads/victim")] },
      ]);
      assert.strictEqual(yield* listCheckpointRefs(cwd), "");
      assert.strictEqual(
        yield* git(cwd, ["rev-parse", "--verify", "refs/heads/victim"]),
        yield* git(cwd, ["rev-parse", "HEAD"]),
      );
    }),
  );
});
