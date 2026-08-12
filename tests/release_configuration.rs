const RELEASE_CONFIG: &str = include_str!("../.github/release.toml");
const DATABASE_MANIFEST: &str = include_str!("../Cargo.toml");

fn workspace_setting(name: &str) -> &str {
    let mut in_workspace = false;
    for line in RELEASE_CONFIG.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_workspace = line == "[workspace]";
            continue;
        }
        if in_workspace {
            if let Some((key, value)) = line.split_once('=') {
                if key.trim() == name {
                    return value.trim();
                }
            }
        }
    }
    panic!("missing [workspace].{name}");
}

fn manifest_package_version(manifest: &str) -> &str {
    let mut in_package = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
            continue;
        }
        if in_package {
            if let Some((key, value)) = line.split_once('=') {
                if key.trim() == "version" {
                    return value.trim().trim_matches('"');
                }
            }
        }
    }
    panic!("missing [package].version");
}

#[test]
fn ordinary_main_pushes_do_not_publish_packages() {
    assert_eq!(workspace_setting("release_always"), "false");
}

#[test]
fn release_commits_cannot_recursively_prepare_another_release() {
    let filter = workspace_setting("release_commits");
    assert_eq!(filter, r#"'^(feat|fix)(\([^)]*\))?!?:'"#);
}

#[test]
fn package_metadata_has_the_application_driver_tags() {
    assert_eq!(manifest_package_version(DATABASE_MANIFEST), "0.6.0");
    assert!(DATABASE_MANIFEST
        .contains("keywords = [\"datastore\", \"vercel\", \"postgres\", \"sql\", \"sqlite\"]"));
    assert!(!DATABASE_MANIFEST.contains("#decapod"));
}
