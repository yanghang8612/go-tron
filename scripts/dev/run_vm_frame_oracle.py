#!/usr/bin/env python3
"""Compile the owning Java VM sources and emit real-VM frame-boundary vectors.

Uses an existing java-tron build and local Maven dependencies. The Java source
tree is read-only; all compilation and output stay in a new scratch directory.
"""
import argparse
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("java_tron", type=Path)
    parser.add_argument("scratch", type=Path)
    parser.add_argument("--java-home", type=Path, help="JDK compatible with the existing java-tron build")
    parser.add_argument("--maven-cache", type=Path, default=Path.home() / ".m2/repository")
    args = parser.parse_args()
    repo = args.java_tron.resolve()
    scratch = args.scratch.resolve()
    if scratch == repo or repo in scratch.parents:
        raise SystemExit("Scratch must be outside the java-tron source checkout")
    scratch.mkdir(parents=True, exist_ok=False)
    classes = scratch / "classes"
    classes.mkdir()
    go_repo = Path(__file__).resolve().parents[2]
    java_home = args.java_home or os.environ.get("JAVA_HOME")
    if not java_home:
        java_home = subprocess.check_output(["/usr/libexec/java_home", "-v", "17"], text=True).strip()
    java_home = Path(java_home)
    dependencies = [
        "org/mockito/mockito-core/4.11.0/mockito-core-4.11.0.jar",
        "net/bytebuddy/byte-buddy/1.12.19/byte-buddy-1.12.19.jar",
        "org/objenesis/objenesis/3.3/objenesis-3.3.jar",
    ]
    jars = [args.maven_cache / p for p in dependencies]
    lombok = args.maven_cache / "org/projectlombok/lombok/1.18.34/lombok-1.18.34.jar"
    fat_jar = repo / "build/libs/FullNode.jar"
    for path in jars + [lombok, fat_jar]:
        if not path.is_file():
            raise SystemExit("Missing existing build dependency: " + str(path))
    modules = ["actuator", "common", "chainbase", "crypto", "protocol", "platform", "consensus", "framework"]
    classpath = [classes] + [repo / m / "build/classes/java/main" for m in modules] + [fat_jar, lombok] + jars
    cp = os.pathsep.join(str(p) for p in classpath)
    vm_sources = repo / "actuator/src/main/java/org/tron/core/vm"
    sources = sorted(vm_sources.rglob("*.java"))
    sources += [repo / "common/src/main/java/org/tron/core/vm/config/VMConfig.java",
                repo / "chainbase/src/main/java/org/tron/common/runtime/ProgramResult.java"]
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--"] + [str(p) for p in sources], cwd=repo, check=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    sources.append(go_repo / "vm/testdata/frame_boundaries/JavaFrameOracle.java")
    command = [str(java_home / "bin/javac"), "-source", "8", "-target", "8", "-cp", cp,
               "-processorpath", str(lombok), "-d", str(classes)] + [str(p) for p in sources]
    with (scratch / "compile.log").open("w") as log:
        compiled = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    if compiled.returncode:
        raise SystemExit("Java compilation failed; see " + str(scratch / "compile.log"))
    logging = scratch / "logback.xml"
    logging.write_text('<configuration><root level="ERROR"/></configuration>\n')
    output = scratch / "java_golden.json"
    with (scratch / "run.log").open("w") as log:
        executed = subprocess.run([str(java_home / "bin/java"), "-Xmx1g", "-Dlogback.configurationFile=" + str(logging),
                                   "-cp", cp, "JavaFrameOracle", str(output), commit],
                                  stdout=log, stderr=subprocess.STDOUT)
    if executed.returncode:
        raise SystemExit("Java oracle failed; see " + str(scratch / "run.log"))
    print(output)


if __name__ == "__main__":
    main()
