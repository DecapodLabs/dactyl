"""Canonicalize the same required proof declarations on the preserved Dactyl task."""
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
CANDIDATE = "e09d80a4e31b0664a7699844522a11fde153a7e2"
PRESERVED_COMMITS = ["ca8454b5fc818845abb80f1043f155b19224442f", "e5fb1dfdaae3bbf06084be751b4a774480ac1b0b", "26e671d809a83c3dcc7eaf09d0b87322c50ff922", CANDIDATE]
BUNDLE_SHA = "sha256:ae6c785d79288bac76f5a30a685db84f7d3186fbd71b117711df639787a37145"
TASK = "feat_01m76c2002nnt3za"
BRANCH = "agent/chatgpt-issue1628-dactyl/" + TASK + "-cloud-connector"
ACTOR = "chatgpt-issue1628-dactyl"
RECEIVER = "chatgpt-issue1628-dactyl-next"
OPS = "ops/issue1628-native-20261111"
TRANSFER = "01M76KRMTCAN1AY9TWA7GCGRTB"
INCOMING = "sha256:200d58e2115c4ff7f00416d60aa1f1684186ab55aa6dd53cdb887a8a847efa50"
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
    parser.add_argument("--candidate-bundle", type=Path, required=True)
    parser.add_argument("--runtime-inventory", type=Path, required=True)
    parser.add_argument("--runtime-review", type=Path, required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    root, material, out = Path.cwd().resolve(), args.material.resolve(), args.output.resolve()
    manifest = json.loads(args.manifest.read_text())
    if (manifest.get("repository") != REPO or manifest.get("base") != BASE
            or manifest.get("source") != SOURCE or manifest.get("task") != TASK
            or manifest.get("branch") != BRANCH or manifest.get("resume_head") != CANDIDATE or out.is_relative_to(root)):
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
                  admitted=False, container_observed=False, checkpoints=False,
                  publication_ready=False, full_boundary_passed=False, custody_exported=False,
                  head_sha=None, pr_url=None, pushed=False, source_recovery=manifest["source_recovery"], preparation_validation="not_run")
    stage, work, control, controller = "preconditions", None, None, None
    claimed = certificate_verified = False
    prepared_hashes, authored_specs = {}, {}
    native_readiness_report = None
    evidence_names_before = None

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
        if any(p not in set(files) | GENERATED | LEGACY_MIGRATION_DELETIONS | {".decapod/config.toml", ".gitignore"} and not p.startswith(SPEC) for p in changed):
            raise Blocked(label + ": source exceeds reviewed product/proof/generated scope")
        if any((work / p).exists() or (work / p).is_symlink() for p in LEGACY_MIGRATION_DELETIONS):
            raise Blocked(label + ": obsolete governance paths must remain native migration deletions")

    def native_evidence_parent():
        parent = Path(env.get("TMPDIR", ""))
        if (parent != Path("/root/issue1628-tmp") or parent.is_symlink()
                or not parent.is_dir() or parent.stat().st_uid != os.getuid()):
            raise Blocked("native evidence parent differs from the owned execution directory")
        return parent

    def native_evidence_names():
        parent = native_evidence_parent()
        names = set()
        for entry in parent.iterdir():
            if not re.fullmatch(r"decapod-readiness-[A-Za-z0-9_-]{6,64}", entry.name):
                continue
            if (entry.is_symlink() or not entry.is_dir() or entry.stat().st_uid != os.getuid()
                    or entry.resolve().parent != parent.resolve()):
                raise Blocked("native evidence directory has unsafe ancestry or ownership")
            names.add(entry.name)
        return names

    def preserve_native_evidence():
        returned = native_readiness_report.get("evidence_path") if native_readiness_report is not None else None
        if returned is None and evidence_names_before is None:
            result["native_proof_outputs"] = dict(files={}, total_bytes=0, reason="Native publisher was not invoked")
            return
        parent = native_evidence_parent()
        locator = "exact_native_returned_path"
        if returned is None:
            created = native_evidence_names() - evidence_names_before
            if len(created) != 1:
                raise Blocked("native evidence retention has no unique newly created direct directory")
            returned = str(parent / next(iter(created)))
            locator = "unique_new_direct_directory_bound_to_exact_candidate_for_recovery_only"
        if not isinstance(returned, str):
            raise Blocked("native returned evidence path is not a string")
        directory = Path(returned)
        if (not directory.is_absolute() or directory.parent != parent
                or not re.fullmatch(r"decapod-readiness-[A-Za-z0-9_-]{6,64}", directory.name)
                or directory.is_symlink() or not directory.is_dir() or directory.stat().st_uid != os.getuid()
                or directory.resolve().parent != parent.resolve()):
            raise Blocked("native evidence directory has unsafe ancestry or ownership")
        completion = directory / "completion"
        if completion.exists() and (completion.is_symlink() or not completion.is_dir()
                or completion.stat().st_uid != os.getuid() or completion.resolve().parent != directory.resolve()):
            raise Blocked("native completion evidence has unsafe ancestry or ownership")
        if locator != "exact_native_returned_path":
            observations = directory / "observations.json"
            verdict = completion / "verdict.json" if completion.exists() else directory / "verdict.json"
            for source in (observations, verdict):
                if source.is_symlink() or not source.is_file() or source.stat().st_size > 8 * 1024 * 1024:
                    raise Blocked("new native evidence lacks bounded regular observations and verdict")
            observed = json.loads(observations.read_text())
            if (not isinstance(observed, dict) or not isinstance(observed.get("before"), dict)
                    or not result.get("head_sha") or observed["before"].get("head_revision") != result["head_sha"]):
                raise Blocked("new native evidence does not bind the exact checkpointed candidate")
        sources = [source for source in directory.iterdir() if re.fullmatch(
            r"(?:required-catalog|expected-plan|observations|snapshot-handoff|verdict|historical-proof|laya-readiness)\.json|native-[0-9]+\.(?:stdout|stderr|steps\.json|diagnostic\.json)", source.name)]
        # Successful native verdict/receipt have these exact released-controller paths.
        for name in ("verdict.json", "native-readiness-receipt.json"):
            if (completion / name).exists():
                sources.append(completion / name)
        captured, total = {}, 0
        for source in sorted(sources):
            if (source.is_symlink() or not source.is_file() or source.stat().st_uid != os.getuid()
                    or source.resolve().parent not in {directory.resolve(), completion.resolve()}):
                raise Blocked("native proof output is not a regular owned evidence file")
            total += source.stat().st_size
            if total > 64 * 1024 * 1024 or len(captured) >= 512:
                raise Blocked("native proof output preservation exceeded its bounded archive limit")
            value = source.read_bytes()
            target = diagnostics / "native-readiness" / directory.name / source.relative_to(directory)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(value)
            captured[str(target.relative_to(diagnostics))] = dict(bytes=len(value), sha256=digest(value))
        result["native_proof_outputs"] = dict(evidence_path=returned, locator=locator, files=captured, total_bytes=total,
            custody="Only named native proof files are retained inside encrypted private diagnostics; this recovery locator grants no readiness authority")

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
        if run("verify exact existing task source ref", "git", "ls-remote", "origin", "refs/heads/" + BRANCH).split() != [CANDIDATE,"refs/heads/" + BRANCH]:
            raise Blocked("task source ref differs from the exact preserved candidate")
        run("verify historical source", "git", "cat-file", "-e", SOURCE + "^{commit}")
        native_bytes = args.native_transfer.read_bytes()
        native = json.loads(native_bytes)
        if (digest(native_bytes) != INCOMING or native["transfer_id"] != TRANSFER
                or native["receiver"] != ACTOR or native["projection"]["task"]["id"] != TASK
                or native["source_commit"] != BASE or native["source_branch"] != manifest["incoming_source_branch"]):
            raise Blocked("sealed incoming task identity differs")
        if manifest["overlays"]:
            raise Blocked("canonical ordering repair cannot contain product source overlays")
        if digest(args.candidate_bundle.read_bytes()) != BUNDLE_SHA:
            raise Blocked("preserved checkpoint bundle differs from independent verified artifact")
        run("verify preserved candidate bundle", "git", "bundle", "verify", str(args.candidate_bundle.resolve()))
        if run("bind preserved candidate bundle head", "git", "bundle", "list-heads", str(args.candidate_bundle.resolve())).splitlines() != [CANDIDATE + " HEAD"]:
            raise Blocked("bundle does not name the exact preserved checkpoint")
        run("load preserved candidate objects", "git", "fetch", "--no-tags", "--no-write-fetch-head", str(args.candidate_bundle.resolve()), "HEAD:refs/heads/issue1628-preserved-candidate")
        if run("verify preserved checkpoint ancestry", "git", "rev-list", "--reverse", BASE + ".." + CANDIDATE).split() != PRESERVED_COMMITS:
            raise Blocked("preserved checkpoint history differs")
        prior_bytes = (material.parent / "dactyl-order-result.json").read_bytes()
        prior = json.loads(prior_bytes)
        if (digest(prior_bytes) != manifest["previous_report_sha256"]
                or prior.get("head_sha") != CANDIDATE or prior.get("final_head_sha") != CANDIDATE
                or prior.get("source_dirty") is not False or prior.get("checkpoints") is not True
                or prior.get("publication_ready") is not False or prior.get("custody_exported") is not True
                or prior.get("pushed") is not True or prior.get("observed_remote_head_sha") != CANDIDATE
                or prior.get("publisher", {}).get("exit_code") != 1 or prior["publisher"].get("timeout") is not False
                or prior.get("blocker", {}).get("stage") != "native full workspace publisher"
                or "Guided init proof commands must have non-empty sorted unique names and commands" not in prior.get("blocker", {}).get("diagnostic", "")
                or prior["transfer"]["transfer_id"] != TRANSFER or prior["transfer"]["plaintext_sha256"] != INCOMING
                or prior["candidate_bundle"]["plaintext_sha256"] != BUNDLE_SHA):
            raise Blocked("preserved native command ordering failure is not exactly bound")
        control = "issue1628-dactyl-order-" + secrets.token_hex(6)
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
        # Preserve the fresh native admission seed, then restore the original
        # committed journal by an exact Git fast-forward inside its admitted
        # branch. Re-admission must leave that existing journal unchanged.
        changed = set(run("inspect native admission seed", "git", "diff", "HEAD", "--name-only", "-z", cwd=work).split("\0")) - {""}
        untracked = set(run("inspect untracked admission seed", "git", "ls-files", "--others", "--exclude-standard", "-z", cwd=work).split("\0")) - {""}
        if any(p not in GENERATED | LEGACY_MIGRATION_DELETIONS and not p.startswith(SPEC) for p in changed | untracked):
            raise Blocked("fresh native admission contains unexpected authored material")
        if changed or untracked:
            run("preserve native admission seed", "git", "stash", "push", "--include-untracked", "--message", "Preserve native admission seed before exact original task continuation", "--", *sorted(changed | untracked), cwd=work)
            seed = run("bind preserved native admission seed", "git", "rev-parse", "refs/stash", cwd=work).strip()
            if run("verify native seed ancestry", "git", "rev-parse", seed + "^1", cwd=work).strip() != BASE:
                raise Blocked("native admission seed has unexpected parent")
            archive = private / "admission-seed.bundle"
            run("preserve native seed bundle", "git", "bundle", "create", str(archive), "refs/stash", "^" + BASE, cwd=work)
            run("verify native seed bundle", "git", "bundle", "verify", str(archive), cwd=work)
            result["admission_seed"] = dict(oid=seed, paths=sorted(changed | untracked), **encrypt(archive, "admission-seed.der"))
        if run("verify clean restoration boundary", "git", "status", "--porcelain", cwd=work).strip():
            raise Blocked("native seed preservation left unexplained material")
        run("restore original checkpointed branch", "git", "reset", "--keep", CANDIDATE, cwd=work)
        if run("verify restored original candidate", "git", "rev-parse", "HEAD", cwd=work).strip() != CANDIDATE:
            raise Blocked("checkpoint source restoration differs")
        journal = run("bind preserved native journal", "git", "rev-parse", CANDIDATE + ":.decapod/governance.json", cwd=work).strip()
        readmitted = doc("readmit preserved native journal", "workspace", "ensure", "--branch", BRANCH, cwd=work)
        if readmitted.get("can_work") is not True or readmitted.get("branch") != BRANCH:
            raise Blocked("original journal readmission did not authorize same-task continuation")
        if run("verify original journal preserved", "git", "hash-object", ".decapod/governance.json", cwd=work).strip() != journal:
            raise Blocked("native readmission changed the preserved journal")
        if run("verify clean preserved candidate", "git", "status", "--porcelain", cwd=work).strip():
            raise Blocked("preserved candidate changed during native readmission")
        old_proof = doc("verify preserved checkpoint history", "govern", "artifacts", "verify-checkpoints", "--base-branch", BASE, "--head-ref", CANDIDATE, "--target-base", "main", "--format", "json", cwd=work)
        if not (old_proof.get("history_proven") and old_proof.get("inspection_complete") and old_proof.get("findings") == []
                and old_proof.get("base_commit") == BASE and old_proof.get("head_commit") == CANDIDATE):
            raise Blocked("preserved original checkpoint history is not proven")
        if sorted(old_proof.get("verified_commits", [])) != sorted(PRESERVED_COMMITS):
            raise Blocked("native history does not preserve all four original checkpoints")
        result["preserved_checkpoint_verification"] = old_proof
        result["checkpoint_verification"] = old_proof
        result["checkpoints"] = True
        result["head_sha"] = CANDIDATE
        result["resumed_original_checkpoint"] = True
        run("orient original task", "decapod", "infer", "orientation", "--task-id", TASK, cwd=work)
        run("read native core doctrine", "decapod", "constitution", "get", "core/DECAPOD", cwd=work)
        doc("resolve task context", "rpc", "--op", "context.resolve", cwd=work)
        result["source_history"] = "same_task_four_preserved_checkpoints_canonical_required_command_order"
        original_journal_bytes = (work / ".decapod/governance.json").read_bytes()
        original_journal = json.loads(original_journal_bytes)
        doc("check original approved execution plan", "govern", "plan", "check-execute", "--todo-id", TASK, cwd=work)
        for p, expected in manifest["previous_files"].items():
            if digest((work / p).read_bytes()) != expected:
                raise Blocked("preserved reviewed source changed: " + p)
        for p, expected in manifest["review_material"].items():
            f = material / p
            if f.is_symlink() or not f.is_file() or digest(f.read_bytes()) != expected:
                raise Blocked("reviewed proof material differs: " + p)
        config = work / ".decapod/config.toml"
        if digest(config.read_bytes()) != manifest["prior_config_sha256"]:
            raise Blocked("preserved effective proof configuration differs")
        previous_config = tomllib.loads(config.read_text())
        proposed = material / "configured-proof.toml"
        if digest(proposed.read_bytes()) != manifest["configured_proof_sha256"]:
            raise Blocked("canonical ordering proposal differs from reviewed material")
        observed_config = tomllib.loads(proposed.read_text())
        expected_config = json.loads(json.dumps(previous_config))
        expected_config["proof"]["commands"].sort(key=lambda command:command["name"])
        expected_config["proof"]["repository_contract"]["reviewed_commands_digest"] = manifest["expected_command_contract_digest"]
        if observed_config != expected_config or observed_config != manifest["expected_native_config"]:
            raise Blocked("canonical ordering repair changes another proof or repository contract value")
        config.write_bytes(proposed.read_bytes())
        if (work / ".decapod/governance.json").read_bytes() != original_journal_bytes:
            raise Blocked("config source preparation changed the original source journal")
        for p, prose in manifest["spec_additions"].items():
            if not p.startswith(SPEC) or not p.endswith(".md") or "decapod:" in prose:
                raise Blocked("spec proposal may contain authored prose only")
            f = work / p
            previous = f.read_text()
            title = re.search(r"(?m)^# [^\n]*\n", previous)
            if not title:
                raise Blocked("spec lacks reviewed heading insertion point")
            rewritten = previous[:title.end()] + "\n" + prose.rstrip("\n") + "\n" + previous[title.end():]
            if [m.group(0) for m in BLOCKS.finditer(previous)] != [m.group(0) for m in BLOCKS.finditer(rewritten)]:
                raise Blocked("authored spec proposal changes native blocks")
            f.write_text(rewritten)
        runtime_bytes = args.runtime_inventory.read_bytes()
        runtime = json.loads(runtime_bytes)
        review = json.loads(args.runtime_review.read_text())
        if review != {"runtime_inventory_sha256": digest(runtime_bytes), "reviewed": True,
                      "scope": "installed canonical Rust1.96.1 and Rust1.75.0 x86 runtime for required local proof"}:
            raise Blocked("installed runtime lacks independent bound review")
        if json.loads(run("verify reviewed installed runtime closure", "/usr/bin/python3.12", "scripts/proof/runtime_inventory.py", cwd=work)) != runtime:
            raise Blocked("installed runtime closure changed after review")
        if digest(runtime_bytes) != manifest["runtime_inventory_sha256"]:
            raise Blocked("original complete runtime closure differs")
        measurements = json.loads((material / "runtime-sizes.json").read_text())
        measured = {row["label"]:row for row in measurements["files"]}
        complete = runtime["toolchain_files"]
        if len(complete) != 280 or set(measured) != set(complete):
            raise Blocked("runtime measurement does not cover the complete reviewed closure")
        first_labels, size_by_path = {}, {}
        prefix = "/usr/local/rustup/toolchains/"
        selected_prefixes = {"stable":prefix + "1.96.1-x86_64-unknown-linux-gnu/",
                             "msrv":prefix + "1.75.0-x86_64-unknown-linux-gnu/"}
        for label,path in sorted(complete.items()):
            row = measured[label]
            if row["path"] != path or (path.startswith(prefix) and not any(path.startswith(p) for p in selected_prefixes.values())):
                raise Blocked("runtime partition contains an unreviewed path")
            if path in size_by_path and size_by_path[path] != row["bytes"]:
                raise Blocked("duplicate runtime measurement is inconsistent")
            first_labels.setdefault(path,label)
            size_by_path[path] = row["bytes"]
        if len(first_labels) != 272:
            raise Blocked("canonical runtime union differs")
        contract = observed_config["proof"]
        if (contract["required_profile"]["id"] != "repo.pr-readiness.v1"
                or contract["validation_buildkite_preflights"] is not False
                or contract["repository_contract"]["reviewed_commands_digest"] != manifest["expected_command_contract_digest"]
                or contract["required_profile"]["reviewed_sources_digest"] != manifest["expected_source_inventory_digest"]):
            raise Blocked("reviewed mandatory proof identity differs")
        actual_groups, group_sizes = {}, {}
        for group,timeout_seconds in [("stable",2400),("msrv",1200)]:
            name = "rust-contract-" + group
            execution = contract["repository_contract"]["executions"][name]
            expected = dict(sorted((label,path) for path,label in first_labels.items()
                if not path.startswith(prefix) or path.startswith(selected_prefixes[group])))
            if execution["toolchain_files"] != expected or execution["timeout_seconds"] != timeout_seconds:
                raise Blocked("required toolchain group omits or adds runtime files")
            size = size_by_path[execution["program"]] + sum(size_by_path[path] for path in expected.values())
            if size >= 1073741824:
                raise Blocked("measured scoped runtime still exceeds the unchanged invocation budget")
            actual_groups[group] = set(expected.values())
            group_sizes[group] = size
        if actual_groups["stable"] | actual_groups["msrv"] != set(complete.values()):
            raise Blocked("required groups do not preserve the complete runtime union")
        final = reconciliation(doc("independently verify exact reviewed complete proof contract", "rpc", "--op", "agent.init", cwd=work))
        if (final["discovered_sources"] != manifest["candidate_sources"]
                or final.get("inventory_digest") != manifest["expected_source_inventory_digest"]
                or final.get("command_contract_digest") != manifest["expected_command_contract_digest"]
                or final.get("status") != "proof_pending"
                or any(item["code"] != "REQUIRED_PROOF_NOT_RUN" for item in final["repair_tasks"])):
            result["final_reconciliation"] = final
            raise Blocked("native reconciliation does not reproduce the independently approved proof pins")
        result["profile_review"] = dict(inventory_digest=final["inventory_digest"],
            command_contract_digest=final["command_contract_digest"], source_inventory_verified=True,
            execution_proof=False, complete_runtime_paths=272, measured_group_bytes=group_sizes,
            timeout_total_seconds=3600, required_checks=14,
            policy_change="Canonical name order only; both mandatory proof groups and all execution/source/runtime values preserved")
        expected_authored = {str(p.relative_to(work)):BLOCKS.sub("",p.read_text()).strip()
                            for p in (work / SPEC).glob("*.md")}
        doc("refresh native canonical command projections", "rpc", "--op", "specs.refresh", cwd=work)
        for p, expected in expected_authored.items():
            if BLOCKS.sub("",(work/p).read_text()).strip() != expected:
                raise Blocked("native projection refresh changed approved authored prose: " + p)
        for name in ["ARCHITECTURE", "INTERFACES", "SECURITY"]:
            review = manifest["spec_reviews"][name]
            doc("record current spec review " + name.lower(), "govern", "plan", "review-spec", "--path", SPEC + name + ".md",
                "--disposition", review["disposition"], "--reason", review["reason"], cwd=work)
        run("verify prepared source whitespace", "git", "diff", "--check", cwd=work)
        trajectory = doc("inspect native trajectory", "govern", "artifacts", "status", cwd=work)["sections"]["trajectory"]["run_id"]
        doc("record actual native command ordering repair", "govern", "trajectory", "record", "--run-id", trajectory, "--task-id", TASK,
            "--phase", "validation", "--scope", "dactyl-cloud-connector", "--boundary", "Same task and four preserved checkpoints; only canonical declaration order and reviewed command pin change; all product checks, runtime closure, environment and timeouts remain exact",
            "--command", "decapod rpc --op agent.init", "--command", "decapod rpc --op specs.refresh",
            "--check", "source-review=passed", "--check", "prior-full-workbranch-readiness=failed", "--check", "current-full-workbranch-proof-result=unavailable",
            "--evidence", "Run 43129927440 confirmed the exact authorized source ref and clean four-checkpoint candidate, but native project prerequisites rejected unsorted proof command names before any product execution. This checkpoint sorts only proof.commands by name and binds independently approved command digest " + manifest["expected_command_contract_digest"] + ". Source inventory remains " + manifest["expected_source_inventory_digest"] + "; both required groups, all 14 exact checks, all 272 runtime paths, every environment/argument/timeout and source-coverage order remain unchanged. Current complete proof remains pending.", cwd=work)
        prepared_hashes = {p: digest((work / p).read_bytes()) for p in files}
        for p in [".decapod/config.toml", ".gitignore", ".decapod/OVERRIDE.md", ".decapod/README.md", "AGENTS.md", "CLAUDE.md", "CODEX.md", "GEMINI.md"]:
            prepared_hashes[p] = digest((work / p).read_bytes())
        authored_specs = {str(p.relative_to(work)): BLOCKS.sub("", p.read_text()).strip() for p in (work / SPEC).glob("*.md")}
        verify_candidate("prepared canonical candidate")
        run("stage reviewed canonical candidate", "git", "add", "-A", cwd=work)
        run("verify complete staged source whitespace", "git", "diff", "--cached", "--check", cwd=work)
        doc("checkpoint canonical required command order", "govern", "artifacts", "checkpoint", "--id", "dactyl-cloud-proof-order-1628",
            "--summary", "Preserve all four original checkpoints and the complete required proof while canonicalizing only command declaration order and its reviewed pin. Product execution remains unproven until native full validation succeeds.",
            "--proof-ref", SPEC + "VALIDATION.md", cwd=work)
        run("stage native checkpoint", "git", "add", ".decapod/governance.json", cwd=work)
        run("commit exact canonical candidate", "git", "commit", "--signoff", "-m", manifest["commit_message"], cwd=work)
        head = run("bind checkpointed candidate", "git", "rev-parse", "HEAD", cwd=work).strip()
        result["head_sha"] = head
        proof = doc("verify exact checkpoint history", "govern", "artifacts", "verify-checkpoints", "--base-branch", BASE,
                    "--head-ref", head, "--target-base", "main", "--format", "json", cwd=work)
        if not (proof.get("history_proven") and proof.get("inspection_complete") and proof.get("findings") == []
                and proof.get("base_commit") == BASE and proof.get("head_commit") == head
                and sorted(proof.get("verified_commits", [])) == sorted(PRESERVED_COMMITS + [head])
                and run("verify complete unrewritten source history", "git", "rev-list", "--reverse", BASE + ".." + head, cwd=work).split() == PRESERVED_COMMITS + [head]):
            raise Blocked("canonical order checkpoint did not preserve exact native source history")
        current_journal = json.loads((work / ".decapod/governance.json").read_text())
        if (current_journal["baseline"] != original_journal["baseline"]
                or current_journal["change"] != original_journal["change"]
                or current_journal["checkpoints"][:len(original_journal["checkpoints"])] != original_journal["checkpoints"]
                or current_journal["sections"]["plan"]["intent"] != original_journal["sections"]["plan"]["intent"]
                or current_journal["sections"]["trajectory"]["run_id"] != original_journal["sections"]["trajectory"]["run_id"]):
            raise Blocked("canonical order repair changed original task journal continuity")
        result.update(checkpoints=True, checkpoint_verification=proof)
        delta = set(run("verify narrow correction delta", "git", "diff", "--name-only", "-z", CANDIDATE, head, cwd=work).split("\0")) - {""}
        if any(p not in {".decapod/config.toml", ".decapod/governance.json"} and not p.startswith(SPEC) for p in delta):
            raise Blocked("canonical order correction includes unrelated product or workflow source")
        verify_candidate("committed canonical candidate")
        if run("verify clean checkpointed worktree", "git", "status", "--porcelain", cwd=work).strip():
            raise Blocked("checkpointed candidate is dirty")
        fresh_target()
        if run("verify exact task ref before normal fast-forward", "git", "ls-remote", "origin", "refs/heads/" + BRANCH, cwd=work).split() != [CANDIDATE,"refs/heads/" + BRANCH]:
            raise Blocked("remote task branch changed before the exact source fast-forward")
        result["source_push"] = dict(attempted=True, expected_remote_head=CANDIDATE, candidate=head,
            workflow_delta=False, authority="Current AGENTS rule 7 permits standalone work-branch pushes before preflight; this update changes no workflow relative to the exact existing remote candidate")
        run("push exact checkpointed correction without force", "git", "push", "origin", "HEAD:refs/heads/" + BRANCH, cwd=work, timeout=180)
        if run("verify exact source fast-forward", "git", "ls-remote", "origin", "refs/heads/" + BRANCH, cwd=work).split() != [head,"refs/heads/" + BRANCH]:
            raise Blocked("source fast-forward was not confirmed at the exact checkpoint")
        result["pushed"] = True
        result["source_push"]["confirmed"] = True
        run("prepare unchanged locked MSRV and clean-consumer cache", "/usr/bin/python3.12", "scripts/proof/prepare.py", cwd=work, timeout=1200)
        verify_candidate("after dependency-only preparation")
        if run("verify clean prepared candidate", "git", "status", "--porcelain", cwd=work).strip():
            raise Blocked("dependency-only preparation changed the committed candidate")
        run("refresh exact remote tag history", "git", "-c", "fetch.prune=false", "-c", "fetch.pruneTags=false", "fetch", "--no-force", "--no-tags", "--no-write-fetch-head", "--no-recurse-submodules", "--no-prune", "--no-prune-tags", "--refmap=", "origin", "refs/tags/*:refs/tags/*", cwd=work)
        remote_tags = sorted(tuple(x.split()) for x in run("observe current tags", "git", "ls-remote", "--tags", "origin", cwd=work).splitlines())
        local_tags = sorted(tuple(x.split()) for x in run("observe local tags", "git", "show-ref", "--tags", "--dereference", cwd=work).splitlines())
        if remote_tags != local_tags:
            raise Blocked("local and remote release history differ")
        body = manifest["body"].replace("{{HEAD_SHA}}", head).replace("{{BASE_SHA}}", BASE)
        evidence_names_before = native_evidence_names()
        code, stdout, stderr, timeout = execute("native full workspace publisher", ["decapod", "workspace", "publish", "--title", manifest["title"], "--description", body], cwd=work, timeout=7200)
        result["publisher"] = dict(exit_code=code, timeout=timeout, ci_identity_preserved=True, codes=sorted(set(re.findall(CODES, stdout + stderr))))
        for stream in (stderr, stdout):
            marker = "PUBLICATION_NOT_READY: "
            if marker in stream:
                try:
                    candidate_report, _ = json.JSONDecoder().raw_decode(stream.split(marker, 1)[1].lstrip())
                except ValueError:
                    continue
                if (isinstance(candidate_report, dict)
                        and candidate_report.get("scope") == "repository_local_readiness"
                        and isinstance(candidate_report.get("ready"), bool)):
                    native_readiness_report = candidate_report
                    result["native_readiness_report"] = scrub(candidate_report)
                    break
        try:
            published = json.loads(stdout)
        except ValueError:
            published = None
        if not (code == 0 and not timeout and isinstance(published, dict) and published.get("status") == "ok"
                and published.get("remote_verified") is True and published.get("pr_verified") is True
                and published.get("branch") == BRANCH and published.get("commit_hash") == head
                and re.fullmatch(r"https://github.com/DecapodLabs/dactyl/pull/[0-9]+", published.get("pr_url", ""))):
            result["blocker"] = dict(stage=stage, diagnostic=scrub((stderr or stdout)[-24000:]))
            raise Blocked("full native publisher did not confirm exact readiness and PR")
        result.update(status="published_after_native_full_boundary", publication_ready=True, full_boundary_passed=True,
                      pr_url=published["pr_url"], pushed=True, publication=published)
        fresh_target()
        if run("verify exact remote candidate", "git", "ls-remote", "origin", "refs/heads/" + BRANCH, cwd=work).split() != [head, "refs/heads/" + BRANCH]:
            raise Blocked("remote candidate no longer matches verified publication")
        verify_candidate("after full native publisher")
    except (Blocked, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        result.setdefault("blocker", dict(stage=stage, kind=type(exc).__name__))
        result["blocker"]["reason"] = str(exc)
    finally:
        first_blocker = result.get("blocker")
        try:
            preserve_native_evidence()
        except (Blocked, OSError, ValueError) as exc:
            result["native_proof_output_preservation_error"] = str(exc)
        if work is not None and result.get("head_sha"):
            try:
                code, stdout, stderr, timeout = execute(
                    "observe final remote candidate", ["git", "ls-remote", "origin", "refs/heads/" + BRANCH],
                    cwd=work, timeout=120)
                if code or timeout:
                    result["remote_observation_error"] = dict(exit_code=code, timeout=timeout)
                elif not stdout.split():
                    result["observed_remote_head_sha"] = None
                    result["pushed"] = False
                else:
                    fields = stdout.split()
                    if len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{40}", fields[0]) or fields[1] != "refs/heads/" + BRANCH:
                        raise Blocked("unexpected remote candidate observation")
                    result["observed_remote_head_sha"] = fields[0]
                    result["pushed"] = fields[0] == result["head_sha"]
            except (Blocked, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
                result["remote_observation_error"] = dict(kind=type(exc).__name__, reason=str(exc))
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
    return 0 if result["status"] == "published_after_native_full_boundary" and result["custody_exported"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
