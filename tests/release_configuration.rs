const RELEASE_CONFIG: &str = include_str!("../.github/release.toml");
const DATABASE_MANIFEST: &str = include_str!("../Cargo.toml");
const MACROS_MANIFEST: &str = include_str!("../dactyl-db-macros/Cargo.toml");

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

    panic!("missing [workspace].{name} in .github/release.toml");
}

fn package_setting<'a>(package: &str, name: &str) -> &'a str {
    let mut in_package = false;
    let mut selected_package = false;

    for line in RELEASE_CONFIG.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[[package]]";
            selected_package = false;
            continue;
        }

        if in_package {
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();
                if key == "name" {
                    selected_package = value.trim_matches('"') == package;
                } else if selected_package && key == name {
                    return value;
                }
            }
        }
    }

    panic!("missing [[package]] {package:?} setting {name:?} in .github/release.toml");
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

    assert_eq!(filter, r"'^(feat|fix)(\([^)]*\))?!?:'");
    assert!(!filter.contains("chore"));
    assert!(filter.contains("feat|fix"));
}

#[test]
fn workspace_packages_render_distinct_tags_at_the_same_version() {
    let template = workspace_setting("git_tag_name").trim_matches('"');
    assert_eq!(template, "{{ package }}-v{{ version }}");

    let render = |package: &str, version: &str| {
        template
            .replace("{{ package }}", package)
            .replace("{{ version }}", version)
    };

    let database_tag = render("dactyl-db", "0.2.2");
    let macros_tag = render("dactyl-db-macros", "0.2.2");
    assert_eq!(database_tag, "dactyl-db-v0.2.2");
    assert_eq!(macros_tag, "dactyl-db-macros-v0.2.2");
    assert_ne!(database_tag, macros_tag);
}

#[test]
fn workspace_package_manifests_and_dependency_are_aligned() {
    assert_eq!(manifest_package_version(DATABASE_MANIFEST), "0.2.2");
    assert_eq!(manifest_package_version(MACROS_MANIFEST), "0.2.2");
    assert!(DATABASE_MANIFEST
        .contains(r#"dactyl-db-macros = { path = "dactyl-db-macros", version = "0.2.2" }"#));
}

#[test]
fn release_worthy_changes_select_and_group_both_packages() {
    assert_eq!(package_setting("dactyl-db", "version_group"), r#""dactyl""#);
    assert_eq!(
        package_setting("dactyl-db-macros", "version_group"),
        r#""dactyl""#
    );
    assert_eq!(
        package_setting("dactyl-db", "changelog_include"),
        r#"["dactyl-db-macros"]"#
    );
    assert_eq!(
        package_setting("dactyl-db-macros", "changelog_include"),
        r#"["dactyl-db"]"#
    );
}
