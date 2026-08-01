const RELEASE_CONFIG: &str = include_str!("../.github/release.toml");

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
