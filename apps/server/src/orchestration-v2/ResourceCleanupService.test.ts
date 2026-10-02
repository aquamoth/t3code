// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { CheckpointRef } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

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
});
