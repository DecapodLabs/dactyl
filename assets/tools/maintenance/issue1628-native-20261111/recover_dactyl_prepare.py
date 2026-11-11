"""Recover exact pending authored source on the same task; prepare a repaired dependency graph for independent review."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import tarfile
import tempfile
import tomllib
import urllib.request

REPO = "DecapodLabs/dactyl"
BASE = "1fa52a232e1d34a27be28d158298805ffc175ffc"
SOURCE = "bef1a565819c67f5403dd24ed6a35163a3a788eb"
TASK = "feat_01m76c2002nnt3za"
BRANCH = "agent/chatgpt-issue1628-dactyl/" + TASK + "-cloud-connector"
ACTOR = "chatgpt-issue1628-dactyl-next"
RECEIVER = "chatgpt-issue1628-dactyl"
OPS = "ops/issue1628-native-20261111"
TRANSFER = "01M76FGKDG70VGTN9GKQ4E4NFM"
INCOMING = "sha256:bdd1146489e8c4ad8e4fff95cae1c5a801f2661f6444347628b71e3648b32081"
CONTROLLER = "31bd8e3983dd328a1796da8af827647be691982c91d7ec7a7b11c7fdc2b4d723"
CERT = "c54fbd6b328ebc89c57a0b158cbdddc434e37f48b8283b0e9af532506600f6dc"
SPEC = ".decapod/managed/specs/"
GENERATED = {"AGENTS.md", "CLAUDE.md", "CODEX.md", "GEMINI.md", ".decapod/governance.json", ".decapod/managed/Dockerfile.decapod"}
LEGACY_MIGRATION_DELETIONS = {".decapod/governance/" + name + ".json" for name in ["claims", "plan", "trajectory", "validation"]}
BLOCKS = re.compile(r"<!-- decapod:([A-Za-z0-9:_-]+):start -->.*?<!-- decapod:\1:end -->", re.S)
REQUEST = "This must be solved. Do so now: https://github.com/DecapodLabs/decapod/issues/1628"
CODES = r"\b(?:AUTHORING_|SESSION_|WORKSPACE_|SCOPE_|GOVERNANCE_|HISTORY_|TASK_TRANSFER_|READINESS_|PUBLICATION_|CONTROLLER_|REPOSITORY_|PR_|DECISION_GATE|POLICY_DENIED)[A-Z0-9_]*\b"


class Blocked(RuntimeError):
    pass


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--material", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--native-transfer", type=Path, required=True)
    parser.add_argument("--repair-material", type=Path, required=True)
    parser.add_argument("--repair-manifest", type=Path, required=True)
    parser.add_argument("--runtime-inventory", type=Path, required=True)
    parser.add_argument("--runtime-review", type=Path, required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    root, material, out = Path.cwd().resolve(), args.material.resolve(), args.output.resolve()
    manifest = json.loads(args.manifest.read_text())
    repair = json.loads(args.repair_manifest.read_text())
    repair_material = args.repair_material.resolve()
    if (manifest.get("repository") != REPO or manifest.get("base") != BASE
            or manifest.get("source") != SOURCE or manifest.get("task") != TASK
            or manifest.get("branch") != BRANCH or out.is_relative_to(root)):
        parser.error("requires exact task, source/base and separate result directory")
    files = {x["path"]: x for x in manifest["files"]}
    if (not files or len(files) != len(manifest["files"])
            or any(p.startswith("/") or ".." in Path(p).parts or p in GENERATED
                   or p.startswith(".decapod/") for p in files)):
        parser.error("reviewed material cannot contain runtime or generated governance")
    out.mkdir(parents=True, mode=0o700, exist_ok=False)
    private = Path(tempfile.mkdtemp(prefix="issue1628-dactyl-private-", dir="/root"))
    diagnostics = private / "diagnostics"
    diagnostics.mkdir(mode=0o700)
    env = dict(os.environ, DECAPOD_AGENT_ID=ACTOR, DECAPOD_CLAIM_AUTORUN="false",
               DECAPOD_SESSION_PASSWORD=secrets.token_hex(32), DECAPOD_VALIDATE_TIMEOUT_SECS="600")
    sensitive = [v for k, v in env.items() if re.search("token|password|secret|credential|authorization", k, re.I) and len(v) >= 8]
    result = dict(schema="decapod.issue1628-dactyl-continuation.v1", repository=REPO,
                  base=BASE, source=SOURCE, task=TASK, branch=BRANCH,
                  operations_sha=env.get("GITHUB_SHA"), status="blocked", imported=False,
                  source_recovery=manifest["source_recovery"], preparation_validation="not_run",
                  admitted=False, container_observed=False, checkpoints=False,
                  publication_ready=False, full_boundary_passed=False, custody_exported=False,
                  head_sha=None, pr_url=None, pushed=False)
    stage, work, control, controller = "preconditions", None, None, None
    claimed = certificate_verified = False
    prepared_hashes, authored_specs = {}, {}

    def scrub(value):
        if isinstance(value, str):
            for secret in sensitive:
                value = value.replace(secret, "[REDACTED]")
            return re.sub(r"(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]{20,}|(?i:bearer)\s+\S+|https?://[^/\s:@]+:[^/@\s]+@", "[REDACTED]", value)
        if isinstance(value, dict):
            return {k: scrub(v) for k, v in value.items() if not re.search("password|secret|credential|authorization|session_token|access_token", k, re.I)}
        if isinstance(value, list):
            return [scrub(x) for x in value]
        return value

    def execute(label, argv, cwd=None, data=None, timeout=240):
        nonlocal stage
        stage = label
        if argv[0] == "decapod":
            if controller is None or hashlib.sha256(controller.read_bytes()).hexdigest() != CONTROLLER:
                raise Blocked("official controller identity changed")
            argv = [str(controller), *argv[1:]]
        print(json.dumps({"stage": label}), flush=True)
        proc = subprocess.Popen(argv, cwd=cwd or root, env=env, text=True, start_new_session=True,
                                stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        timed_out = False
        try:
            stdout, stderr = proc.communicate(data, timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                stdout, stderr = proc.communicate()
        stem = re.sub(r"[^a-z0-9-]", "-", label.lower())
        (diagnostics / (stem + ".stdout.txt")).write_text(scrub(stdout))
        (diagnostics / (stem + ".stderr.txt")).write_text(scrub(stderr))
        return proc.returncode, stdout, stderr, timed_out

    def run(label, *argv, cwd=None, data=None, timeout=240):
        code, stdout, stderr, timed_out = execute(label, list(argv), cwd, data, timeout)
        if code or timed_out:
            result["blocker"] = dict(stage=label, exit_code=code, timeout=timed_out,
                                     codes=sorted(set(re.findall(CODES, stdout + stderr))),
                                     diagnostic=scrub((stderr or stdout)[-12000:]))
            raise Blocked("native or source preparation command failed")
        return stdout

    def doc(label, *argv, cwd=None, data=None):
        value = json.loads(run(label, "decapod", *argv, cwd=cwd, data=data))
        if isinstance(value, dict) and value.get("error"):
            result["blocker"] = dict(stage=label, diagnostic=scrub(value["error"]))
            raise Blocked("native operation returned an error")
        return value

    def encrypt(source, name):
        if not certificate_verified:
            raise Blocked("unverified custody recipient")
        target = out / name
        run("encrypt " + name, "openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
            "-in", str(source), "-out", str(target), "-outform", "DER", str(args.certificate.resolve()))
        return dict(artifact=name, plaintext_sha256=digest(source.read_bytes()), ciphertext_sha256=digest(target.read_bytes()))

    def fresh_target():
        if run("verify current accepted target", "git", "ls-remote", "origin", "refs/heads/main").split() != [BASE, "refs/heads/main"]:
            raise Blocked("accepted target changed; source review and full validation required")

    def reconciliation(value):
        report = value.get("result", {}).get("reconciliation")
        if not isinstance(report, dict):
            raise Blocked("native initialization omitted required reconciliation")
        return report

    def verify_candidate(label):
        for p, expected in prepared_hashes.items():
            f = work / p
            if f.is_symlink() or not f.is_file() or digest(f.read_bytes()) != expected:
                raise Blocked(label + ": prepared material changed: " + p)
        for p, expected in authored_specs.items():
            if BLOCKS.sub("", (work / p).read_text()).strip() != expected:
                raise Blocked(label + ": authored specification changed: " + p)
        changed = set(run(label + " tracked scope", "git", "diff", "--name-only", "-z", BASE, cwd=work).split("\0")) - {""}
        changed |= set(run(label + " untracked scope", "git", "ls-files", "--others", "--exclude-standard", "-z", cwd=work).split("\0")) - {""}
        if any(p not in set(files) | GENERATED | LEGACY_MIGRATION_DELETIONS | {".decapod/config.toml"} and not p.startswith(SPEC) for p in changed):
            raise Blocked(label + ": source exceeds reviewed product/proof/generated scope")
        if any((work / p).exists() or (work / p).is_symlink() for p in LEGACY_MIGRATION_DELETIONS):
            raise Blocked(label + ": obsolete controller-owned governance paths must be native migration deletions only")

    try:
        if (env.get("GITHUB_ACTIONS") != "true" or env.get("BUILDKITE") == "true"
                or env.get("GITHUB_REPOSITORY") != REPO or env.get("GITHUB_REF") != "refs/heads/" + OPS
                or not Path("/.dockerenv").is_file()):
            raise Blocked("requires exact authorized repository and actual hosted container")
        result["container_observed"] = True
        fingerprint = run("verify operator custody certificate", "openssl", "x509", "-in", str(args.certificate.resolve()),
                          "-noout", "-fingerprint", "-sha256").strip().split("=", 1)[1].replace(":", "").lower()
        if fingerprint != CERT:
            raise Blocked("operator custody certificate differs")
        certificate_verified = True
        # Reuse the reviewed metadata/digest-checked installer, never a candidate build.
        installer = material.parent / "install_controller.py"
        run("install exact official released controller", "/usr/bin/python3.12", str(installer), str(private))
        controller = private / "decapod"
        if hashlib.sha256(controller.read_bytes()).hexdigest() != CONTROLLER:
            raise Blocked("released controller bytes differ")
        env["PATH"] = str(private) + os.pathsep + env["PATH"]
        result["controller"] = dict(release="v0.138.2", binary_sha256=CONTROLLER)
        run("ingest native doctrine", "decapod", "docs", "ingest")
        evaluation = doc("evaluate exact requested work", "eval", "--stdin", "--format", "json", data=REQUEST + "\n\n" + manifest["intent"])
        if evaluation.get("status") != "allow":
            raise Blocked("native request evaluation did not allow this work")
        result["evaluation"] = evaluation
        if run("verify accepted checkout", "git", "rev-parse", "HEAD").strip() != BASE or run("verify clean checkout", "git", "status", "--porcelain").strip():
            raise Blocked("initial source is not exact clean accepted main")
        if run("verify repository origin", "git", "remote", "get-url", "origin").strip() != "https://github.com/" + REPO + ".git":
            raise Blocked("unexpected source origin")
        fresh_target()
        if run("verify task branch remains unpublished", "git", "ls-remote", "origin", "refs/heads/" + BRANCH).strip():
            raise Blocked("task branch exists remotely; recover its exact candidate instead of initial bootstrap")
        run("verify historical source", "git", "cat-file", "-e", SOURCE + "^{commit}")
        native_bytes = args.native_transfer.read_bytes()
        native = json.loads(native_bytes)
        if (digest(native_bytes) != INCOMING or native["transfer_id"] != TRANSFER
                or native["receiver"] != ACTOR or native["projection"]["task"]["id"] != TASK
                or native["source_commit"] != BASE or native["source_branch"] != manifest["incoming_source_branch"]):
            raise Blocked("sealed incoming task identity differs")
        for p, entry in files.items():
            f = material / p
            if f.is_symlink() or not f.is_file() or f.stat().st_size != entry["bytes"] or digest(f.read_bytes()) != entry["sha256"]:
                raise Blocked("reviewed proposal digest differs: " + p)
        if (repair.get("repository") != REPO or repair.get("task") != TASK
                or repair.get("source_recovery_sha256") != manifest["source_recovery"]["authored_manifest_sha256"]):
            raise Blocked("dependency repair is not bound to this exact recovered task/source")
        repair_files = {entry["path"]: entry for entry in repair["files"]}
        if not repair_files or len(repair_files) != len(repair["files"]):
            raise Blocked("dependency repair paths are ambiguous")
        for p, entry in repair_files.items():
            if p.startswith("/") or ".." in Path(p).parts or p.startswith(".decapod/") or p in GENERATED:
                raise Blocked("repair may contain reviewed authored source only")
            f = repair_material / p
            if f.is_symlink() or not f.is_file() or f.stat().st_size != entry["bytes"] or digest(f.read_bytes()) != entry["sha256"]:
                raise Blocked("reviewed dependency repair bytes differ: " + p)
        control = "issue1628-dactyl-recovery-" + secrets.token_hex(6)
        run("name private control checkout", "git", "switch", "-c", control, BASE)
        run("acquire native session", "decapod", "session", "acquire")
        baseline = reconciliation(doc("initialize accepted control plane", "rpc", "--op", "agent.init"))
        if baseline["discovered_sources"] != manifest["accepted_sources"]:
            raise Blocked("accepted source inventory differs from independent review")
        if any(r.get("owner") != "agent" or r.get("code") not in {"MISSING_REQUIRED_PROFILE", "UNKNOWN_CI_CONTRACT", "INTENT_BUILD_MISMATCH", "REQUIRED_PROOF_NOT_RUN"} for r in baseline["repair_tasks"]):
            raise Blocked("native initial adoption contains an unreviewed decision or access gate")
        result["baseline_reconciliation"] = baseline
        imported = doc("import exact sealed task custody", "todo", "transfer", "import", "--input", str(args.native_transfer.resolve()),
                       "--expected-digest", INCOMING, "--accept-source-transfer", "--format", "json")
        if imported.get("status") != "ok" or imported.get("task_id") != TASK:
            raise Blocked("native task import was not confirmed")
        result["imported"] = True
        claim = doc("claim original native task", "todo", "claim", "--id", TASK, "--lease-seconds", "10800", "--format", "json")
        if claim.get("status") != "ok":
            raise Blocked("native task claim was not confirmed")
        claimed = True
        admitted = doc("readmit original governed workspace", "workspace", "ensure", "--branch", BRANCH)
        if admitted.get("can_work") is not True or admitted.get("branch") != BRANCH:
            raise Blocked("native workspace admission failed")
        work = Path(admitted["worktree_path"]).resolve()
        result["admitted"] = True
        if run("verify workspace source base", "git", "rev-parse", "HEAD", cwd=work).strip() != BASE:
            raise Blocked("readmitted workspace has unexpected source")
        run("orient original task", "decapod", "infer", "orientation", "--task-id", TASK, cwd=work)
        run("read native core doctrine", "decapod", "constitution", "get", "core/DECAPOD", cwd=work)
        doc("resolve task context", "rpc", "--op", "context.resolve", cwd=work)
        try:
            doc("record historical source recovery", "govern", "artifacts", "record-history-recovery", "--source-ref", SOURCE,
                "--reason", "Recover independently reviewed connector source from immutable prior branch, preserving its historical checkpoint gaps without inheriting ownership or proof. Original branch remains untouched.", cwd=work)
            result["source_history"] = "observed_gap_recorded"
        except Blocked:
            if result.get("blocker", {}).get("codes") != ["HISTORY_RECOVERY_NOT_APPLICABLE"]:
                raise
            old_base = run("resolve original merge base", "git", "merge-base", BASE, SOURCE, cwd=work).strip()
            proof = doc("verify historical checkpoint lineage", "govern", "artifacts", "verify-checkpoints", "--base-branch", old_base,
                        "--head-ref", SOURCE, "--target-base", "main", "--format", "json", cwd=work)
            if not (proof.get("history_proven") and proof.get("inspection_complete") and proof.get("findings") == []):
                raise Blocked("historical source is neither recoverable nor already proven")
            result.pop("blocker", None)
            result["source_history"] = "already_proven"
        doc("record reviewed connector plan", "govern", "plan", "update", "--title", manifest["title"], "--intent", manifest["intent"],
            "--todo-id", TASK, "--proof-hook", "decapod workspace publish", "--file-touch-budget", str(len(files) + 24), cwd=work)
        doc("approve already authorized plan", "govern", "plan", "approve", cwd=work)
        doc("record falsifiable connector claim", "govern", "artifacts", "claim", "--id", "claim:dactyl-cloud-connector",
            "--statement", manifest["intent"], "--falsifier", manifest["falsifier"], "--status", "open", "--proof-ref", SPEC + "VALIDATION.md", cwd=work)
        for p in sorted(files):
            destination = work / p
            if destination.is_symlink():
                raise Blocked("reviewed proposal cannot overwrite a symlink")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((material / p).read_bytes())
            destination.chmod(0o644)
        for p, prose in manifest["spec_additions"].items():
            if not p.startswith(SPEC) or not p.endswith(".md") or "decapod:" in prose:
                raise Blocked("spec proposal may contain authored prose only")
            f = work / p
            previous = f.read_text()
            title = re.search(r"(?m)^# [^\n]*\n", previous)
            if not title:
                raise Blocked("spec lacks reviewed heading insertion point")
            rewritten = previous[:title.end()] + "\n" + prose + "\n" + previous[title.end():]
            if [m.group(0) for m in BLOCKS.finditer(previous)] != [m.group(0) for m in BLOCKS.finditer(rewritten)]:
                raise Blocked("authored spec proposal changes native blocks")
            f.write_text(rewritten)
        template = (material.parent / "proof-config.toml").read_bytes()
        if digest(template) != manifest["proof_config_sha256"]:
            raise Blocked("reviewed profile template changed")
        runtime_bytes = args.runtime_inventory.read_bytes()
        runtime = json.loads(runtime_bytes)
        review = json.loads(args.runtime_review.read_text())
        if review != {"runtime_inventory_sha256": digest(runtime_bytes), "reviewed": True,
                      "scope": "installed canonical Rust1.96.1 and Rust1.75.0 x86 runtime for required local proof"}:
            raise Blocked("installed runtime closure lacks independent bound review")
        observed_runtime = run("verify reviewed installed runtime closure", "/usr/bin/python3.12", "scripts/proof/runtime_inventory.py", cwd=work)
        if json.loads(observed_runtime) != runtime:
            raise Blocked("installed runtime closure changed after independent review")
        tools = runtime["toolchain_files"]
        if (not tools or tools.get("python") != "/usr/bin/python3.12"
                or any(not str(p).startswith(("/usr/", "/lib/", "/bin/", "/etc/python3.12/"))
                       for p in tools.values())):
            raise Blocked("runtime closure contains an unexpected tool source")
        rendered_tools = "\n".join(json.dumps(k) + " = " + json.dumps(v) for k, v in sorted(tools.items()))
        config_template = template.decode()
        if config_template.count("# REVIEWED_RUNTIME_FILES") != 1:
            raise Blocked("profile runtime insertion point is ambiguous")
        config_template = config_template.replace("# REVIEWED_RUNTIME_FILES", rendered_tools)
        old_config = tomllib.loads((work / ".decapod/config.toml").read_text())
        new_config = tomllib.loads(config_template)
        old_other = {k: v for k, v in old_config.items() if k != "proof"}
        new_other = {k: v for k, v in new_config.items() if k != "proof"}
        old_other["repo"]["product_name"] = "dactyl-db"
        if old_other != new_other:
            raise Blocked("initial proof adoption changes unrelated policy")
        (work / ".decapod/config.toml").write_text(config_template)
        # These bytes already passed the same pinned formatter in the failed
        # admitted runner. Preserve them exactly for the recovery checkpoint.
        run("format reviewed source", "/usr/local/rustup/toolchains/1.96.1-x86_64-unknown-linux-gnu/bin/cargo", "fmt", "--all", cwd=work)
        for p, entry in files.items():
            if digest((work / p).read_bytes()) != entry["sha256"]:
                raise Blocked("recovered authored source changed before preservation checkpoint: " + p)
        candidate = reconciliation(doc("measure reviewed proof declaration", "rpc", "--op", "agent.init", cwd=work))
        if candidate["discovered_sources"] != manifest["candidate_sources"]:
            raise Blocked("complete candidate source inventory differs from reviewed workflow/helper/build contract")
        command_digest = candidate.get("command_contract_digest")
        source_digest = candidate.get("inventory_digest")
        if not all(re.fullmatch(r"sha256:[0-9a-f]{64}", v or "") for v in [command_digest, source_digest]):
            raise Blocked("native review digests unavailable")
        config_text = config_template
        for name, measured in [("reviewed_commands_digest", command_digest), ("reviewed_sources_digest", source_digest)]:
            config_text, count = re.subn(r'(?m)^' + name + r' = "sha256:0{64}"$', name + ' = "' + measured + '"', config_text)
            if count != 1:
                raise Blocked("profile review placeholder is ambiguous")
        (work / ".decapod/config.toml").write_text(config_text)
        final = reconciliation(doc("verify initial profile reconciliation", "rpc", "--op", "agent.init", cwd=work))
        if final.get("status") != "proof_pending" or any(r["code"] != "REQUIRED_PROOF_NOT_RUN" for r in final["repair_tasks"]):
            result["final_reconciliation"] = final
            raise Blocked("profile adoption still has a required remediation")
        result["profile_review"] = dict(inventory_digest=source_digest, command_contract_digest=command_digest,
                                        source_inventory_verified=True, execution_proof=False)
        doc("refresh native source projections", "rpc", "--op", "specs.refresh", cwd=work)
        for name in ["ARCHITECTURE", "INTERFACES", "SECURITY"]:
            review = manifest["spec_reviews"][name]
            doc("record current spec review " + name.lower(), "govern", "plan", "review-spec", "--path", SPEC + name + ".md",
                "--disposition", review["disposition"], "--reason", review["reason"], cwd=work)
        run("verify prepared source whitespace", "git", "diff", "--check", cwd=work)
        trajectory = doc("inspect native trajectory", "govern", "artifacts", "status", cwd=work)["sections"]["trajectory"]["run_id"]
        doc("record actual preparation evidence", "govern", "trajectory", "record", "--run-id", trajectory, "--task-id", TASK,
            "--phase", "validation", "--scope", "dactyl-cloud-connector", "--boundary", "Independently reviewed recovered source and initial repository proof adoption; hosted service and registry publication remain separate",
            "--command", "cargo fmt --all", "--command", "decapod rpc --op specs.refresh",
            "--check", "source-review=passed", "--check", "full-workbranch-proof-result=unavailable",
            "--evidence", "Same original task restored from native sealed successor after run43120599440. Authored pending source recovered with independently reviewed terminal-blank cleanup from verified encrypted archive " + manifest["source_recovery"]["archive_plaintext_sha256"] + "; native projections regenerated through CLI. Prior MSRV dependency fetch failed on rand_pcg0.10.2 edition2024 before build/test/checkpoint. This checkpoint preserves that source before repair; full proof has not run and unavailable describes the absent result.", cwd=work)
        prepared_hashes = {p: digest((work / p).read_bytes()) for p in files}
        prepared_hashes[".decapod/config.toml"] = digest((work / ".decapod/config.toml").read_bytes())
        authored_specs = {str(p.relative_to(work)): BLOCKS.sub("", p.read_text()).strip() for p in (work / SPEC).glob("*.md")}
        verify_candidate("prepared candidate")
        run("stage reviewed candidate", "git", "add", "-A", cwd=work)
        run("verify complete staged source whitespace", "git", "diff", "--cached", "--check", cwd=work)
        doc("checkpoint exact source and initial proof adoption", "govern", "artifacts", "checkpoint", "--id", "dactyl-cloud-connector-1628",
            "--summary", "Recover and harden the logical cloud connector/native RPC contract, preserve auth uncertainty and private caller custody, and adopt the complete reviewed local Rust proof under current released controller. Full readiness remains pending.",
            "--proof-ref", SPEC + "VALIDATION.md", cwd=work)
        run("stage native checkpoint", "git", "add", ".decapod/governance.json", cwd=work)
        run("commit exact checkpointed candidate", "git", "commit", "--signoff", "-m", manifest["commit_message"], cwd=work)
        head = run("bind checkpointed candidate", "git", "rev-parse", "HEAD", cwd=work).strip()
        result["head_sha"] = head
        proof = doc("verify exact checkpoint history", "govern", "artifacts", "verify-checkpoints", "--base-branch", BASE,
                    "--head-ref", head, "--target-base", "main", "--format", "json", cwd=work)
        if not (proof.get("history_proven") and proof.get("inspection_complete") and proof.get("findings") == []
                and proof.get("base_commit") == BASE and proof.get("head_commit") == head):
            raise Blocked("candidate checkpoint lineage is not proven")
        result.update(checkpoints=True, checkpoint_verification=proof)
        verify_candidate("committed candidate")
        if run("verify clean checkpointed worktree", "git", "status", "--porcelain", cwd=work).strip():
            raise Blocked("checkpointed candidate is dirty")
        fresh_target()
        # Preserve the exact recovered authored foundation before any dependency
        # repair. This commit grants no proof and is never a publication result.
        result["recovered_checkpoint_sha"] = head
        for p, entry in repair_files.items():
            destination = work / p
            if destination.is_symlink():
                raise Blocked("repair cannot overwrite a symlink")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((repair_material / p).read_bytes())
            destination.chmod(0o644)
            files[p] = entry
        run("format reviewed dependency repair", "/usr/local/rustup/toolchains/1.96.1-x86_64-unknown-linux-gnu/bin/cargo", "fmt", "--all", cwd=work)
        before_resolve = {p: digest((work / p).read_bytes()) for p in files if p != "Cargo.lock"}
        stable = "/usr/local/rustup/toolchains/1.96.1-x86_64-unknown-linux-gnu/bin/cargo"
        # Only dependency resolution/acquisition is allowed at this preparation
        # boundary. The exact resulting lock is reviewed before native proof.
        if repair["resolution_commands"] != [["update", "--workspace"]]:
            raise Blocked("dependency resolution differs from the exact reviewed preparation")
        for index, command in enumerate(repair["resolution_commands"]):
            run("resolve reviewed dependency graph " + str(index), stable, *command, cwd=work, timeout=1200)
        metadata_text = run("inspect repaired dependency graph", stable, "metadata", "--locked", "--all-features", "--format-version", "1", cwd=work, timeout=600)
        (diagnostics / "dependency-metadata.json").write_text(metadata_text)
        metadata = json.loads(metadata_text)
        names = {package["name"] for package in metadata["packages"]}
        forbidden = set(repair["forbidden_dependencies"])
        if names & forbidden:
            raise Blocked("unwanted dependency graph remains: " + ", ".join(sorted(names & forbidden)))
        lock = tomllib.loads((work / "Cargo.lock").read_text())
        if lock.get("version") != 3 or {package["name"] for package in lock["package"]} & forbidden:
            raise Blocked("repaired lock has incompatible format or forbidden dependency")
        for packages in [metadata["packages"], lock["package"]]:
            for package in packages:
                prefixes = repair["forbidden_version_prefixes"].get(package["name"], [])
                if any(package["version"].startswith(prefix) for prefix in prefixes):
                    raise Blocked("incompatible optional graph package remains: " + package["name"] + " " + package["version"])
            for name, expected in repair["expected_dependency_versions"].items():
                if [p["version"] for p in packages if p["name"] == name] != [expected]:
                    raise Blocked("dependency resolution moved the reviewed TLS package: " + name)
        packages_by_id = {p["id"]: p for p in metadata["packages"]}
        features = {packages_by_id[n["id"]]["name"]: set(n["features"]) for n in metadata["resolve"]["nodes"]}
        if (features["reqwest"] & {"http3", "quinn", "__rustls-ring"}
                or "rustls-tls-webpki-roots-no-provider" not in features["reqwest"]
                or features["rustls"] != {"ring", "std", "tls12"}):
            raise Blocked("resolved HTTP/TLS features differ from the reviewed private-provider contract")
        for p, expected in before_resolve.items():
            if digest((work / p).read_bytes()) != expected:
                raise Blocked("dependency resolution changed non-lock source: " + p)
        prepared_hashes = {p: digest((work / p).read_bytes()) for p in files}
        prepared_hashes[".decapod/config.toml"] = digest((work / ".decapod/config.toml").read_bytes())
        verify_candidate("review-pending repaired candidate")
        result.update(status="dependency_graph_prepared_for_independent_review",
                      repaired_lock_sha256=digest((work / "Cargo.lock").read_bytes()),
                      repaired_source_hashes=prepared_hashes,
                      dependency_graph_packages=sorted({(p["name"], p["version"]) for p in metadata["packages"]}),
                      forbidden_dependencies_absent=sorted(forbidden),
                      dependency_metadata_sha256=digest(metadata_text.encode()),
                      observed_http_tls_features={name: sorted(features[name]) for name in ["reqwest", "rustls"]},
                      full_boundary_passed=False, publication_ready=False,
                      next_action="Independently review exact repaired lock/source, then run unchanged Cargo1.75/clean-consumer dependency preparation on this same task and checkpoint repair before full native proof. No build/test or PR performed.")
    except (Blocked, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        result.setdefault("blocker", dict(stage=stage, kind=type(exc).__name__))
        result["blocker"]["reason"] = str(exc)
    finally:
        first_blocker = result.get("blocker")
        if claimed:
            try:
                if (run("verify custody control branch", "git", "symbolic-ref", "--quiet", "HEAD").strip() != "refs/heads/" + control
                        or run("verify custody source base", "git", "rev-parse", "HEAD").strip() != BASE):
                    raise Blocked("private export source changed")
                run("yield exact native task", "decapod", "todo", "yield", "--id", TASK, "--reason", "Explicit transfer to " + RECEIVER + " before ephemeral runner teardown")
                transfer = private / "native-transfer.json"
                code, stdout, stderr, timeout = execute("export sealed native custody", ["decapod", "todo", "transfer", "export", "--id", TASK, "--to", RECEIVER, "--output", str(transfer), "--format", "json"])
                if code or timeout:
                    sealed = re.search(r"source sealed as ([0-9A-HJKMNP-TV-Z]{26}); artifact publication unconfirmed:", stdout + stderr)
                    if timeout or not sealed:
                        raise Blocked("native export failed without supported exact recovery")
                    transfer = private / "native-transfer-recovered.json"
                    exported = doc("recover exact sealed custody", "todo", "transfer", "recover", "--id", TASK, "--transfer-id", sealed[1], "--output", str(transfer), "--format", "json")
                else:
                    exported = json.loads(stdout)
                b = transfer.read_bytes()
                n = json.loads(b)
                if not (exported.get("status") == "ok" and exported.get("source_sealed") is True
                        and exported.get("artifact_sha256") == digest(b) and n["receiver"] == RECEIVER
                        and n["projection"]["task"]["id"] == TASK and n["source_commit"] == BASE
                        and n["source_branch"] == control and n["transfer_id"] == exported["transfer_id"]
                        and not any(secret.encode() in b for secret in sensitive)):
                    raise Blocked("exported native custody binding differs")
                result["transfer"] = dict(task_id=TASK, transfer_id=n["transfer_id"], receiver=RECEIVER,
                    source_sealed=True, native_source_commit=BASE, native_source_branch=control,
                    candidate_branch=BRANCH, candidate_sha=result.get("head_sha"), **encrypt(transfer, "native-custody.der"))
                result["custody_exported"] = True
            except (Blocked, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
                result["transfer_error"] = dict(stage=stage, kind=type(exc).__name__, reason=str(exc))
        if certificate_verified:
            try:
                if work is not None:
                    final_head = run("observe final source", "git", "rev-parse", "HEAD", cwd=work).strip()
                    result["final_head_sha"] = final_head
                    result["source_dirty"] = bool(run("observe final worktree", "git", "status", "--porcelain", cwd=work).strip())
                    if final_head != BASE:
                        bundle = private / "candidate.bundle"
                        run("preserve exact candidate history", "git", "bundle", "create", str(bundle), "HEAD", "^" + BASE, cwd=work)
                        run("verify candidate preservation", "git", "bundle", "verify", str(bundle), cwd=work)
                        result["candidate_bundle"] = encrypt(bundle, "candidate.der")
                    for label, argv, name in [
                        ("preserve pending tracked material", ["git", "diff", "--binary", "HEAD"], "pending-tracked.patch"),
                        ("preserve pending index", ["git", "diff", "--cached", "--binary", "HEAD"], "pending-index.patch"),
                    ]:
                        code, stdout, stderr, timeout = execute(label, argv, cwd=work)
                        if code or timeout:
                            raise Blocked("exact pending source preservation failed")
                        # Raw source stays inside this private encrypted archive;
                        # display diagnostics remain scrubbed independently.
                        (diagnostics / name).write_text(stdout)
                    pending = set(run("inspect pending source paths", "git", "ls-files", "--others", "--exclude-standard", "-z", cwd=work).split("\0")) - {""}
                    for p in sorted(pending):
                        if p in set(files) | GENERATED or p.startswith(SPEC):
                            f = work / p
                            if f.is_file() and not f.is_symlink() and f.stat().st_size <= 8 * 1024 * 1024:
                                destination = diagnostics / "untracked-source" / p
                                destination.parent.mkdir(parents=True, exist_ok=True)
                                destination.write_bytes(f.read_bytes())
                    (diagnostics / "prepared-hashes.json").write_text(json.dumps(prepared_hashes, indent=2))
                archive = private / "diagnostics.tar"
                with tarfile.open(archive, "w") as tar:
                    tar.add(diagnostics, arcname="diagnostics")
                result["diagnostics"] = encrypt(archive, "native-diagnostics.der")
            except (Blocked, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
                result["preservation_error"] = dict(stage=stage, kind=type(exc).__name__, reason=str(exc))
        if first_blocker is not None:
            result["blocker"] = first_blocker
        if result["publication_ready"] and (not result["custody_exported"] or result.get("blocker")):
            result["status"] = "published_with_observation_or_custody_blocker"
        (out / "result.json").write_text(json.dumps(scrub(result), indent=2) + "\n")
        print(json.dumps(scrub(result), indent=2), flush=True)
    return 0 if result["status"] == "dependency_graph_prepared_for_independent_review" and result["custody_exported"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
