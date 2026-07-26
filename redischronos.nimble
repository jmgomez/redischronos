import std/[os, strutils]

# Package
version       = "1.1.2"
author        = "jmgomez"
description   = "Chronos-native KV and Pub/Sub with interchangeable memory and Redis backends"
license       = "MIT"
srcDir        = "src"

# Dependencies
requires "nim >= 2.2.0"
requires "chronos >= 4.0.0"
requires "chronos < 5.0.0"

proc resolveNim(): string =
  let configured = getEnv("NIM_BIN").strip()
  if configured.len > 0:
    return configured

  let pathNim = findExe("nim")
  if pathNim.len > 0:
    return pathNim

  let nimRoot = getHomeDir() / ".nimble" / "nimbinaries"
  for version in ["2.2.10", "2.2.8", "2.2.6"]:
    let candidate = nimRoot / ("nim-" & version) / "bin" / "nim"
    if fileExists(candidate):
      return candidate

  quit "Nim 2.2.x not found; set NIM_BIN to the absolute Nim binary path"

proc testCommand(
    mm = "";
    testFile = "tests/tall.nim";
    redisIntegration = false
): string =
  result = quoteShell(resolveNim()) & " c --path:src -d:test"
  if redisIntegration:
    if getEnv("REDIS_TEST_URL").strip().len == 0:
      quit "REDIS_TEST_URL is mandatory for Redis integration tests"
    result.add " -d:redisIntegration"
  if mm.len > 0:
    result.add " --mm:" & mm
  result.add " -r " & quoteShell(testFile)

task test, "Run all tests":
  exec testCommand()

task testRefc, "Run all tests with refc":
  exec testCommand("refc")

task testOrc, "Run all tests with ORC":
  exec testCommand("orc")

task testRedisRefc, "Run memory and mandatory real-Redis tests with refc":
  exec testCommand("refc", redisIntegration = true)

task testRedisOrc, "Run memory and mandatory real-Redis tests with ORC":
  exec testCommand("orc", redisIntegration = true)

task testFile, "Run one test file; pass its path as the first argument":
  if commandLineParams().len == 0:
    quit "Usage: nimble testFile tests/tname.nim"
  exec testCommand(testFile = commandLineParams()[^1])
